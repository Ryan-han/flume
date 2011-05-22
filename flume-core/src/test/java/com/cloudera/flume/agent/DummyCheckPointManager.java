package com.cloudera.flume.agent;

import java.io.IOException;
import java.util.Map;

import com.cloudera.flume.handlers.endtoend.AckListener;
import com.nexr.agent.cp.CheckPointManager;

public class DummyCheckPointManager implements CheckPointManager {
	
	private Map<String, Long> checkPointMap;
	
	public void setCheckPointMap(Map<String, Long> checkPointMap) {
		this.checkPointMap = checkPointMap;
	}


	@Override
	public void toAcked(String tag) throws IOException {
	}


	@Override
	public void retry(String tag) throws IOException {
	}


	@Override
	public void setPendingQ(AckListener agentAckQueuer) {
	}


	@Override
	public void addPendingQ(String tagId, Map<String, Long> fileOffsets) throws IOException {
	}


	@Override
	public Map<String, Long> getCheckpoint() {
		return checkPointMap;
	}


	@Override
	public void start() {
	}


	@Override
	public void stop() {
	}
}
