package com.nexr.agent.cp;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.CRC32;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cloudera.flume.agent.FlumeNode;
import com.cloudera.flume.agent.LogicalNode;
import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.LogicalNodeContext;
import com.cloudera.flume.conf.SinkFactory.SinkDecoBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;
import com.cloudera.flume.handlers.endtoend.AckChecksumInjector;
import com.cloudera.flume.handlers.rolling.RollTrigger;
import com.cloudera.flume.handlers.rolling.TimeTrigger;
import com.cloudera.flume.handlers.text.TailSource;
import com.google.common.base.Preconditions;

/**
 * 주기적으로 Checkpoint Start Event와 End Event를 Event 사이에 보내 준다.
 * 
 * @author bitaholic
 * 
 * @param <S>
 */
public class CheckpointInjector<S extends EventSink> extends EventSinkDecorator<S>{

	static Log LOG = LogFactory.getLog(CheckpointInjector.class);

	private RollTrigger trigger;
	private Map<String, Long> offsetMap;
	private ChecksumHelper chkHelper;
	
	byte[] tag;
	
	CheckPointManager manager;
	
	/**
	 * this is only for test
	 * 
	 * @param s
	 */
	public CheckpointInjector(S s) {
		super(s);
	}
	
	/**
	 * 
	 * @param s
	 * @param logicalNodeName
	 *          : unique한 Tag를 만들 때 쓴다.
	 * @param maxAge
	 *          : Tag Rolling 하는 주기 (msec)
	 */
	public CheckpointInjector(S s, long maxAge, CheckPointManager manager) {
		super(s);
		this.trigger = new TimeTrigger(maxAge);
		this.chkHelper = new ChecksumHelper();
		this.manager = manager;
		this.offsetMap = new HashMap<String, Long>();
	}

	@Override
	public void open() throws IOException, InterruptedException {
		super.open();
		super.append(openEvent());
		trigger.reset();
		manager.start();
		//TODO listener.start(new String(tag));
	}

	@Override
	public void close() throws IOException, InterruptedException {
		super.append(closeEvent());
		super.close();
		//TODO listener.end(new String(tag));
		if(offsetMap != null && offsetMap.size() > 0) {
			manager.addPendingQ(new String(tag), new HashMap<String, Long>(offsetMap));
		}
		manager.stop();
	}

	@Override
	public void append(Event e) throws IOException, InterruptedException {
		if (trigger.isTriggered()) {
			rotate();
		}

		byte[] offset = e.get(TailSource.A_TAILSRCOFFSET); 
		// 체크포인트 방식은 파일에서만 할 수 있다.
		if (offset != null) {
			long o = ByteBuffer.wrap(offset).asLongBuffer().get();
			// TODO 파일이름이 메타 정보로 없을 때 처리
			String fileName = new String(ByteBuffer.wrap(e.get(TailSource.A_TAILSRCFILE)).array());
			offsetMap.put(fileName, o); // 동일 파일이면 같은 Tag내에서는 offset이 업데이트 된다.
		} else {
			long curChk = chkHelper.addChecksum(e.getBody());
			e.set(AckChecksumInjector.ATTR_ACK_TYPE, AckChecksumInjector.CHECKSUM_MSG);
			e.set(AckChecksumInjector.ATTR_ACK_TAG, tag);
			e.set(AckChecksumInjector.ATTR_ACK_HASH, ByteBuffer.allocate(8).putLong(curChk).array());
			super.append(e);
		}
	}

	/**
	 * 정해진 시간 마다 EndEvent를 보내고 현재 파일 오프셋 정보를 Manager 에게 보낸다. 그리고 새로운 태그로
	 * StartEvent를 보낸다.
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void rotate() throws IOException, InterruptedException {
		LOG.debug("start to rotate");
		super.append(closeEvent());
		
		manager.addPendingQ(new String(tag), new HashMap<String, Long>(offsetMap));
		trigger.reset();
		offsetMap.clear();
		
		super.append(openEvent());
		LOG.debug("Completed rotate");
	}
	
	public Event openEvent() {
		tag = trigger.getTagger().newTag().getBytes();
    Event e = new EventImpl(new byte[0]);
    chkHelper.reset(e.getTimestamp());
    
    e.set(AckChecksumInjector.ATTR_ACK_TYPE, AckChecksumInjector.CHECKSUM_START);
    e.set(AckChecksumInjector.ATTR_ACK_HASH, chkHelper.checksumArray());
    e.set(AckChecksumInjector.ATTR_ACK_TAG, tag);
    return e;
  }
	
	public Event closeEvent() {
    Event e = new EventImpl(new byte[0]);
    e.set(AckChecksumInjector.ATTR_ACK_TYPE, AckChecksumInjector.CHECKSUM_STOP);
    e.set(AckChecksumInjector.ATTR_ACK_HASH, chkHelper.checksumArray());
    e.set(AckChecksumInjector.ATTR_ACK_TAG, tag);
    return e;
  }
	
	class ChecksumHelper {
		CRC32 chk = new CRC32();
		long checksum;
		
		byte[] checksumArray() {
			return ByteBuffer.allocate(8).putLong(checksum).array();
		}
		
		long addChecksum(byte[] content) {
			chk.reset();
			chk.update(content);
			long curchk = chk.getValue();
			checksum ^= curchk;
			return curchk;
		}
		
		void reset(long newChecksum) {
			checksum = newChecksum;
		}
	}

	public static SinkDecoBuilder builder() {
		return new SinkDecoBuilder() {
			@Override
			public EventSinkDecorator<EventSink> build(Context context, String... argv) {
				Preconditions.checkArgument(argv.length <= 1, "usage: checkpointInjector");
				
				FlumeNode node = FlumeNode.getInstance();
				String ckNode = context.getValue(LogicalNodeContext.C_LOGICAL);
				
				 Preconditions.checkArgument(ckNode != null,
         		"Context does not have a logical node name");
				 
				 CheckPointManager ckManager = node.getAddCheckpointManager(ckNode);
				 ckManager.setPendingQ(node.getAckChecker().getAgentAckQueuer());
				return new CheckpointInjector<EventSink>(null, FlumeConfiguration.get().getCheckpointRollPeriod(), 
						ckManager);
			}
		};
	}
}
