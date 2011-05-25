package com.nexr.dedup;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.nexr.dedup.workflow.DedupConstants;
import com.nexr.dedup.workflow.job.DedupJob;
import com.nexr.framework.workflow.JobExecutionException;
import com.nexr.framework.workflow.JobLauncher;
import com.nexr.rolling.workflow.ZkClientFactory;
import com.nexr.rolling.workflow.job.Duplication;

/**
 * @author dani.kim@nexr.com
 */
public class DedupManager implements Runnable, IZkChildListener {
	private static final String DEDUP_QUEUE = "/dedup/queue";
	
	private Logger LOG = LoggerFactory.getLogger(getClass());
	
	private ZkClient client = ZkClientFactory.getClient();
	private ClassPathXmlApplicationContext ctx;
	private JobLauncher launcher;

	private volatile boolean running = true;
	
	private BlockingQueue<Duplication> queue;
	private ConcurrentHashMap<String, Duplication> duplications;
	
	public DedupManager() {
		queue = new LinkedBlockingQueue<Duplication>();
		duplications = new ConcurrentHashMap<String, Duplication>();
		client.subscribeChildChanges(DEDUP_QUEUE, this);
		
		ctx = new ClassPathXmlApplicationContext("classpath:workflow-app.xml");
		launcher = ctx.getBean(JobLauncher.class);
	}
	
	@Override
	public synchronized void run() {
		while (running) {
			if (queue.peek() == null) {
				LOG.debug("Queue is empty.");
				try {
					wait();
				} catch (InterruptedException e) {
				}
				continue;
			}
			execute(queue.poll());
		}
	}
	
	private void execute(Duplication duplication) {
		LOG.info("Start Dedup Job. {}", new Object[] { duplication.getPath() });
		if (validateDedupJob(duplication)) {
			DedupJob job = ctx.getBean(DedupJob.class);
			job.addParameter(DedupConstants.JOB_TYPE, duplication.getType());
			job.addParameter(DedupConstants.PATH, duplication.getPath());
			job.addParameter(DedupConstants.NEW_SOURCE_DIR, String.format("%s/%s", duplication.getNewSource(), duplication.getPath()));
			job.addParameter(DedupConstants.SOURCE_DIR, String.format("%s/%s", duplication.getSource(), duplication.getPath()));
			job.addParameter(DedupConstants.OUTPUT_PATH, String.format("%s/%s", "/dedup", duplication.getType()));
			job.addParameter(DedupConstants.RESULT_PATH, duplication.getSource());
			
			String preffixOfClass = Character.toUpperCase(duplication.getType().charAt(0)) + duplication.getType().substring(1);
			job.addParameter(DedupConstants.MR_CLASS, String.format("com.nexr.rolling.core.%sDedupMr", preffixOfClass));
			
			try {
				LOG.info("Starting Dedup Job : {}", duplication.getPath());
				launcher.run(job);
			} catch (JobExecutionException e) {
				e.printStackTrace();
			}
		}
	}
	
	private boolean validateDedupJob(Duplication duplication) {
		if (duplication.getType() == null) {
			return false;
		}
		if (duplication.getType().length() == 0) {
			return false;
		}
		return true;
	}

	public boolean isRunning() {
		return running;
	}

	public void stop() {
		running = false;
	}
	
	@Override
	public synchronized void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
		if (currentChilds != null) {
			for (String child : currentChilds) {
				if (!duplications.containsKey(child)) {
					Object json = client.readData(String.format("%s/%s", DEDUP_QUEUE, child));
					if (json != null) {
						Duplication duplication = Duplication.JsonDeserializer.deserialize(json.toString());
						queue.add(duplication);
						duplications.put(child, duplication);
					}
				}
			}
		}
		notifyAll();
	}
}
