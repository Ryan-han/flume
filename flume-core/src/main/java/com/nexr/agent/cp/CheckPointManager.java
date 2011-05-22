package com.nexr.agent.cp;

import java.io.IOException;
import java.util.Map;

import com.cloudera.flume.agent.durability.WALCompletionNotifier;
import com.cloudera.flume.handlers.endtoend.AckListener;

public interface CheckPointManager extends WALCompletionNotifier{
	
	public void setPendingQ(AckListener agentAckQueuer);
	
	public void addPendingQ(String tagId, Map<String, Long> fileOffsets) throws IOException;
	
	public Map<String, Long> getCheckpoint();
	
	public void start();
	
	public void stop();

}
