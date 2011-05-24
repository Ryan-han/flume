package com.cloudera.flume.agent;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.collector.MemorySink;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.handlers.endtoend.AckChecksumInjector;
import com.cloudera.flume.handlers.endtoend.AckListener;
import com.cloudera.flume.handlers.text.TailSource;
import com.cloudera.util.Clock;
import com.cloudera.util.FileUtil;
import com.nexr.agent.cp.CheckPointManager;
import com.nexr.agent.cp.CheckPointManagerImpl;
import com.nexr.agent.cp.CheckpointInjector;

public class TestCheckpoint {
	static final Logger LOG = LoggerFactory.getLogger(TestCheckpoint.class);
	
	File tmpdir = null;
	
	@Before
	public void setup() {
		try {
			tmpdir = FileUtil.mktempdir();
		} catch (IOException e) {
			Assert.fail("mk temp dir failed");
		}
	}
	
	@After
	public void tearDown() {
		try {
			FileUtil.rmr(tmpdir);
		} catch (IOException e) {
			LOG.error("Failed to remove dir "+ tmpdir, e);
		}
	}
	
	/**
	 * CheckpointInjector의 주기를 5000으로 주고, rotate가 2번 일어 나게 sleep 을 알맞게 건다. 
	 * @throws FlumeSpecException 
	 * @throws InterruptedException 
	 * @throws IOException 
	 * 
	 */
	@Test
	public void testCheckpointBehavior() throws FlumeSpecException, IOException, InterruptedException {
		MemorySink snk = new MemorySink("foo");
		CheckPointManager manager = new CheckPointManagerImpl("testNode", tmpdir.getPath());
		manager.start();
		AckListener ackListener = Mockito.mock(AckListener.class);
		manager.setPendingQ(ackListener);
		CheckpointInjector<EventSink> es = new CheckpointInjector<EventSink>(snk, 5000, manager);
		
		es.open();
		
		for(int i=0; i<3; i++) {
			Event e = new EventImpl("testmsg".getBytes());
			es.append(e);
		}
		
		Event metaEvent = new EventImpl();
		metaEvent.set(TailSource.A_TAILSRCOFFSET, ByteBuffer.allocate(8).putLong(100).array());
		metaEvent.set(TailSource.A_TAILSRCFILE, "FileA".getBytes());
		es.append(metaEvent);
		
		Clock.sleep(5000);
		
		for(int i=0; i<3; i++) {
			Event e = new EventImpl("testmsg".getBytes());
			es.append(e);
		}
		
		metaEvent = new EventImpl();
		metaEvent.set(TailSource.A_TAILSRCOFFSET, ByteBuffer.allocate(8).putLong(200).array());
		metaEvent.set(TailSource.A_TAILSRCFILE, "FileA".getBytes());
		es.append(metaEvent);
		metaEvent = new EventImpl();
		metaEvent.set(TailSource.A_TAILSRCOFFSET, ByteBuffer.allocate(8).putLong(100).array());
		metaEvent.set(TailSource.A_TAILSRCFILE, "FileB".getBytes());
		es.append(metaEvent);
		es.close();
		Clock.sleep(5000);

		byte[] tag = snk.eventList.get(0).get(AckChecksumInjector.ATTR_ACK_TAG);
		manager.toAcked(new String(tag));
		tag = snk.eventList.get(5).get(AckChecksumInjector.ATTR_ACK_TAG);
		manager.toAcked(new String(tag));
		
		Clock.sleep(15000); // 체크포인트 파일 쓰는 쓰레드 주기를 기다려 주어야 한다.(heartbeat period * 2) == 10000
		manager.stop();
		
		Mockito.verify(ackListener, Mockito.times(2)).end(Mockito.anyString());
		
		// 체크포인트 파일 내용 확인
		Assert.assertEquals(1, tmpdir.listFiles().length);

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		
		for(File file : tmpdir.listFiles()) {
			Log.info("checkpoint file : " + file.getName());
			BufferedReader br = new BufferedReader(new FileReader(file));
			String str = null;
			while( (str = br.readLine()) != null ) {
				Log.info(" -- " + str);
				out.write(str.getBytes());
			}
		}
		Assert.assertEquals("FileA:200FileB:100", out.toString());
	}
}
