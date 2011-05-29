package com.nexr.framework.workflow;

import java.util.ArrayList;
import java.util.List;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nexr.rolling.workflow.ZkClientFactory;

/**
 * @author dani.kim@nexr.com
 */
public abstract class WorkflowManager implements Runnable, IZkChildListener {
	private Logger LOG = LoggerFactory.getLogger(getClass());
	
	private Configuration config = Configuration.getInstance();
	private ZkClient client = ZkClientFactory.getClient();

	private JobLauncher launcher;
	private volatile boolean running = true;

	private List<String> runnings;
	private List<String> outages;
	
	public WorkflowManager() {
		runnings = new ArrayList<String>();
		outages = new ArrayList<String>();
		client.subscribeChildChanges(config.getZkWorkflowOutage(), this);
		client.subscribeChildChanges(config.getZkWorkflowRunning(), this);
		
		client.deleteRecursive(config.getZkWorkflowComplete());
		client.deleteRecursive(config.getZkWorkflowRunning());
		client.deleteRecursive(config.getZkWorkflowAbandone());
		client.deleteRecursive("/lock");
		client.deleteRecursive("/dedup/queue");
		
		client.createPersistent(config.getZkWorkflowRunning(), true);
		client.createPersistent(config.getZkWorkflowComplete(), true);
		client.createPersistent(config.getZkWorkflowAbandone(), true);
		client.createPersistent(config.getZkWorkflowOutage(), true);
	}
	
	public synchronized void stop() {
		running = false;
		notifyAll();
	}
	
	@Override
	public synchronized void run() {
		launcher = createLauncher();
		JobExecutionDao executionDao = getExecutionDao();
		while (running) {
			for (String jobId : runnings) {
				if (!client.exists(config.getZkWorkflowOutage(jobId))) {
					JobExecution execution = executionDao.findJobExecutionById(jobId);
					if (validJobStatus(execution)) {
						LOG.info("Outage. jobId: {}", jobId);
						try {
							execution.incrementRetryCount();
							if (execution.getRetryCount() > 4) {
								executionDao.failJob(execution);
								continue;
							}
							executionDao.updateJobExecution(execution);
							launcher.run(execution.getJob());
						} catch (JobExecutionException e) {
							e.printStackTrace();
						}
					}
				}
			}
			try {
				wait();
			} catch (InterruptedException e) {
			}
		}
	}
	
	private boolean validJobStatus(JobExecution execution) {
		if (execution == null) {
			return false;
		}
		JobStatus status = execution.getStatus();
		return status != null && (status == JobStatus.STARTED || status == JobStatus.STARTING || status == JobStatus.FAILED);
	}

	@Override
	public synchronized void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
		boolean isRunning = parentPath.startsWith(config.getZkWorkflowRunning());
		boolean isOutage = parentPath.startsWith(config.getZkWorkflowOutage());
		if (isRunning) {
			updateKeys(runnings, currentChilds);
		}
		if (isOutage) {
			updateKeys(outages, currentChilds);
			notifyAll();
		}
	}
	
	protected void updateKeys(List<String> collection, List<String> currentChilds) {
		synchronized (collection) {
			collection.clear();
			if (currentChilds != null) {
				for (String child : currentChilds) {
					collection.add(child);
				}
			}
		}
	}
	
	public abstract JobLauncher createLauncher();
	
	public abstract JobFactory getJobFactory();
	
	public abstract JobExecutionDao getExecutionDao();
}
