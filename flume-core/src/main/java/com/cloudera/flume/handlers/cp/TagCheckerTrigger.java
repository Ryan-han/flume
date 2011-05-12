package com.cloudera.flume.handlers.cp;

import com.cloudera.flume.agent.FlumeNode;
import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.LogicalNodeContext;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkListener;
import com.google.common.base.Preconditions;

public class TagCheckerTrigger implements EventSinkListener {

	public static final String USE_CHECKPOINT = "useCheckpoint";
	
	private String logicalNodeName;
	private String collectorHost;
	private int checkpointPort;

	public TagCheckerTrigger(String logicalNodeName, String collectorHost, int checkpointPort) {
		this.logicalNodeName = logicalNodeName;
		this.collectorHost = collectorHost;
		this.checkpointPort = checkpointPort;
	}

	@Override
	public void open() {
		// TODO setCollectorHost는 사용하지 않는다 삭제해야 하지 않을까?
		FlumeNode.getInstance().getCheckPointManager().setCollectorHost(collectorHost);
		FlumeNode.getInstance().getCheckPointManager().startTagChecker(logicalNodeName, collectorHost, checkpointPort);
	}

	@Override
	public void close() {
		FlumeNode.getInstance().getCheckPointManager().stopTagChecker(logicalNodeName);
	}
	
	public static EventSink registTagCheckerTrigger(EventSink snk, Context context, String collectorHost) {
		Preconditions.checkNotNull(context.getValue(LogicalNodeContext.C_LOGICAL), "Logical Node name is null");
		String logicalNodeName = context.getValue(LogicalNodeContext.C_LOGICAL);
		int cpPort = FlumeConfiguration.get().getCheckPointPort();
		snk.addListener(new TagCheckerTrigger(logicalNodeName, collectorHost, cpPort));
		return snk;
	}
}