package com.cloudera.flume.agent;

import java.util.List;
import java.util.Map;

import com.nexr.agent.cp.CheckPointManager;

public class DummyCheckPointManager implements CheckPointManager {
	
	private Map<String, Long> checkPointMap;
	
	@Override
	public String getTagId(String agentName, String filename) {
		return null;
	}

	@Override
	public void stopClient() {
	}

	@Override
	public Map<String, Long> getOffset(String logicalNodeName) {
		return checkPointMap;
	}

	@Override
	public void setCollectorHost(String host) {
	}

	@Override
	public void startServer() {
	}

	public void setCheckPointMap(Map<String, Long> checkPointMap) {
		this.checkPointMap = checkPointMap;
	}

	@Override
	public void addPendingQ(String tagId, String logicalNodeName,
			Map<String, Long> tagContent) {
	}

	@Override
	public void addCollectorCompleteList(List<String> tagIds) {
	}

	@Override
	public boolean getTagList(String tagId) {
		return false;
	}

	@Override
	public void startClient() {
	}

	@Override
	public void startTagChecker(String agentName, String collectorHost,
			int collectorPort) {
	}

	@Override
	public void stopTagChecker(String agentName) {
	}

	@Override
	public void stopServer() {
	}

	@Override
	public void startServer(int port) {
	}
}
