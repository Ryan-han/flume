package com.nexr.agent.cp;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cloudera.flume.agent.FlumeNode;
import com.cloudera.flume.agent.LogicalNode;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.handlers.endtoend.AckListener;
import com.google.common.base.Preconditions;

public class CheckPointManagerImpl implements CheckPointManager {
	static final Log log = LogFactory.getLog(CheckPointManagerImpl.class);
	
	private final String SEPERATOR = "\t";
	private final String LINE_SEPERATOR = "\n";
	static final String CKPOINT_SUFFIX = ".checkpoint";

	private String baseDir;
	private String logicalNodeName;
	private Map<String, Long> fileOffsets;
	private AckListener listener;
	
	private CheckpointThread t;
	
	public CheckPointManagerImpl(String logicalNodeName, String baseDir) {
		this.logicalNodeName = logicalNodeName;
		this.baseDir = baseDir;
		this.fileOffsets = new HashMap<String, Long>();

		try {
			readFromFile();
		} catch (IOException e) {
			log.error("Failed to read checkpoint file", e);
		}
		
		t = new CheckpointThread(this.baseDir, FlumeConfiguration.get().getConfigHeartbeatPeriod() * 2);
	}
	
	@Override
	public Map<String, Long> getCheckpoint() {
		return Collections.unmodifiableMap(this.fileOffsets);
	}
	
	@Override
	public void setPendingQ(AckListener agentAckQueuer) {
		this.listener = agentAckQueuer;
	}
	
	@Override
	public void addPendingQ(String tagId, Map<String, Long> fileOffsets) throws IOException {
		//TODO 해당 tagId가 들어오면 fileOffsets을 합쳐야 한다. 
		// 이 때 같은 파일이 들어오면 더 높은 offset으로 update를 하면 되고
		// 새로운 파일이 들어오면 맵에 추가 하면 된다. 
		// 이 때 언제 저 맵을 clear해 줄 것인가? 체크포인트 파일로 쓸때? 아니면 
		// 삭제를 하면 안되나? ==> Master에서 정보를 확인하고 삭제 해야 할거 같은데
		
		// TODO IOException 처리
		log.info("add pending : " + tagId + " , " + fileOffsets);
		pending.add(new CheckpointData(tagId, fileOffsets));
		listener.end(tagId);
	}

	@Override
	public void toAcked(String tag) throws IOException {
		//TODO tag에 해당되는 파일 오프셋을 갱신?
		//TODO 파일에 내리는 것은 다른 쓰레드가 하는게 좋겠지?
		log.info("toAcked : " + tag);
		for(CheckpointData data : pending) {
			if(data.tag.equals(tag)) {
				data.setAcked(true);
			}
		}
	}

	@Override
	public void retry(String tag) throws IOException {
		// TODO 실패하면 리커버를 해야 한다. 
		log.info("retry : " + tag);
		for(CheckpointData data : pending) {
			if(data.tag.equals(tag)) {
				data.setAcked(false);
				break;
			}
		}
		mergeAndWrite();
		try {
			recover();
		} catch (InterruptedException e) {
			e.printStackTrace(); //TODO 예외 처리 
		} catch (RuntimeException e) {
			e.printStackTrace();
		} catch (FlumeSpecException e) {
			e.printStackTrace();
		}
	}
	
	private ConcurrentLinkedQueue<CheckpointData> pending = new ConcurrentLinkedQueue<CheckpointData>();
	
	private synchronized void mergeAndWrite() throws IOException {
		List<CheckpointData> ackedList = new ArrayList<CheckpointData>();
		
		List<CheckpointData> removed = new ArrayList<CheckpointData>();

		for(CheckpointData data : pending) {
			if(data.isAcked == true) {
				ackedList.add(data);
				removed.add(data);
			} else {
				break;
			}
		}
		pending.removeAll(removed);
		
		//TODO rety(tag)로 충분하나?. 순서대로 안왔을 때 보장은 안 해줘도 되나?
		for(CheckpointData data : ackedList) {
			for(String fileName : data.fileOffsets.keySet()) {
				fileOffsets.put(fileName, data.fileOffsets.get(fileName));
			}
		}
		writeToFile();
	}
	
	
	private void writeToFile() throws IOException {
		if(fileOffsets == null || fileOffsets.size() == 0) 
			return;
		
		File parent = new File(this.baseDir);
		if(!parent.exists()) {
			log.info(parent + " not exist and create " + parent.mkdir());
		}

		StringBuilder sb = new StringBuilder();
		for(String fileName : this.fileOffsets.keySet()) {
			sb.append(fileName + ":" + fileOffsets.get(fileName) + LINE_SEPERATOR);
		}
		BufferedWriter bw = null;
		try {
			bw = new BufferedWriter(new FileWriter(new File(this.baseDir, this.logicalNodeName + CKPOINT_SUFFIX)));
			bw.write(sb.toString());
			bw.flush();
		} finally {
			if(bw != null) {
				bw.close();
			}
		}
	}
	
	private void readFromFile() throws IOException {
		log.info("Read checkpoint file ");
		if(this.fileOffsets == null) {
			this.fileOffsets = new HashMap<String, Long>();
		} else {
			fileOffsets.clear();
		}
		
		String filePath = this.baseDir + File.separatorChar + this.logicalNodeName + CKPOINT_SUFFIX;
		File path = new File(filePath);
		if(!path.exists()) {
			return;
		}
		
		//fileName : offset
		BufferedReader br = new BufferedReader(new FileReader(path));
		String line = null;
		while( (line = br.readLine()) != null) {
			String[] result = line.split(":");
			this.fileOffsets.put(result[0], Long.parseLong(result[1]));
		}
	}
	
	public void start() {
		if(t.isAlive()) {
			log.warn(t + " is already started...");
			return;
		}
		t.start();
	}
	
	public void stop() {
		CountDownLatch stopped = t.stopped;
		t.done = true;
		try {
			stopped.await();
		} catch (InterruptedException e) {
			log.error("Problem waiting for CheckpointManager to stop", e);
			t.interrupt();
		}
	}
	
	/**
	 * 여기에 위치하는게 맞나? 
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws RuntimeException
	 * @throws FlumeSpecException
	 */
	public void recover() throws IOException, InterruptedException, RuntimeException,
		FlumeSpecException {
		LogicalNode logicalNode = FlumeNode.getInstance().getLogicalNodeManager().get(logicalNodeName);
		if (logicalNode == null) {
			log.error("Failed recover [" + logicalNodeName + "] is not registed in CheckpointManager");
			return;
		}
		log.info("Closing logicalNode[" + logicalNodeName + "]");
		logicalNode.close();
		log.info("Closed logicalNode[" + logicalNodeName + "]");
		
		log.info("Restarting LogicalNode [" + logicalNodeName + "]");
		logicalNode.restartNode();
		log.info("Finished LogicalNode [" + logicalNodeName + "]");
	}
	
	class CheckpointThread extends Thread {
		long period;
		String baseDir;
		volatile boolean done = false;
		CountDownLatch stopped = new CountDownLatch(1);
		
		CheckpointThread(String baseDir, long period) {
			Preconditions.checkArgument(period > 0);
			this.period = period;
			this.baseDir = baseDir;
		}

		@Override
		public void run() {
			while(!done) {
				try {
					mergeAndWrite();
					Thread.sleep(period);
				} catch (InterruptedException e) {
					log.error("CheckPointManager interrupted, this is not expected!", e);
				} catch (IOException e) {
					log.error("Failed to write checkpoint file!", e);
				}
			}
			stopped.countDown();
		}
	}
	
	class CheckpointData {
		String tag;
		Map<String, Long> fileOffsets;
		boolean isAcked = false;
		
		CheckpointData(String tag, Map<String, Long> fileOffsets) {
			this.tag = tag;
			this.fileOffsets = fileOffsets;
		}

		public boolean isAcked() {
			return isAcked;
		}

		public void setAcked(boolean isAcked) {
			this.isAcked = isAcked;
		}
	}
	
	
}
