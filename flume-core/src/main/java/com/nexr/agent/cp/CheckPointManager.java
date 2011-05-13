package com.nexr.agent.cp;

import java.util.List;
import java.util.Map;

public interface CheckPointManager {

	public String getTagId(String agentName, String filename);

	public void startClient();
	
	public void stopClient();

	/**
	 * 
	 * @param tagId
	 * @param tagContent
	 *            : key:fileName, value:lastOffset
	 */
	public void addPendingQ(String tagId, String logicalNodeName, Map<String, Long> tagContent);

	public Map<String, Long> getOffset(String logicalNodeName);

	public void setCollectorHost(String host);

	public void startTagChecker(String agentName, String collectorHost, int collectorPort);
	
	public void stopTagChecker(String agentName);
	
	//for Collector
	public void startServer(int port);
	
	public void startServer();
	
	public void stopServer();
	
	public void addCollectorCompleteList(List<String> tagIds);

	public boolean getTagList(String tagId);
}
