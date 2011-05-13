package com.nexr.agent.cp;

import java.io.IOException;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.agent.FlumeNode;
import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkDecoBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;
import com.cloudera.flume.handlers.endtoend.AckListener;
import com.google.common.base.Preconditions;

public class CheckpointRegister<S extends EventSink> extends EventSinkDecorator<S> {

	static final Logger LOG = LoggerFactory.getLogger(CheckpointRegister.class);
	
	final AckListener listener;
	
	public CheckpointRegister(S s, AckListener l) {
		super(s);
		Preconditions.checkNotNull(l);
		this.listener = l;
	}
	
	public CheckpointRegister(S s) {
		super(s);
		this.listener = new AckListener() {

			@Override
			public void start(String group) throws IOException {
				LOG.info("started" + group);
			}

			@Override
			public void end(String group) throws IOException {
				LOG.info("ended" + group);
			}

			@Override
			public void err(String group) throws IOException {
				LOG.info("erred" + group);
			}

			@Override
			public void expired(String key) throws IOException {
				LOG.info("expired" + key);
			}
		};
	}

	@Override
	public void append(Event e) throws IOException, InterruptedException {
		byte[] btyp = e.get(CheckpointDeco.ATTR_CK_TYPE);
		
		if(btyp == null) {
			super.append(e);
			return;
		}
		
		byte[] btag = e.get(CheckpointDeco.ATTR_CK_TAG);
		String tag = new String(btag);
		
		if(Arrays.equals(btyp, CheckpointDeco.CK_START)) {
			listener.start(tag);
			return;
		} else if(Arrays.equals(btyp, CheckpointDeco.CK_END)) {
			listener.end(tag);
			return;
		}
		super.append(e);
	}
	
	public static SinkDecoBuilder builder() {
		return new SinkDecoBuilder() {
			@Override
			public EventSinkDecorator<EventSink> build(Context context,
					String... argv) {
				Preconditions.checkArgument(argv.length == 0, "usage: checkpointRegister");
				return new CheckpointRegister<EventSink>(null, new AckListener() {
					CheckPointManager manager = FlumeNode.getInstance().getCheckPointManager();
					@Override
					public void start(String group) throws IOException {
					}
					
					@Override
					public void expired(String key) throws IOException {
					}
					
					@Override
					public void err(String group) throws IOException {
					}
					
					@Override
					public void end(String group) throws IOException {
//						manager.addCollectorPendingList(group);
						//TODO builder로 생성하지 않는다. 
					}
				});  
			}
		};
	}
	
}
