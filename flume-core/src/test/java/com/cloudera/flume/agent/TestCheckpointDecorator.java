package com.cloudera.flume.agent;

import java.io.IOException;

import junit.framework.Assert;

import org.junit.Test;

import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.LogicalNodeContext;
import com.cloudera.flume.conf.ReportTestingContext;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.reporter.ReportManager;
import com.cloudera.flume.reporter.aggregator.CounterSink;
import com.cloudera.util.Clock;

public class TestCheckpointDecorator {

	@Test
	public void testBuilder() throws FlumeSpecException {
		FlumeNode node = new FlumeNode(new MockMasterRPC(), false, false);

		String cfg = " { checkpointInjector => null }";
		FlumeBuilder.buildSink(LogicalNodeContext.testingContext(), cfg);
	}

	@Test
	public void testOpenClose() throws FlumeSpecException, IOException, InterruptedException {
		String rpt = "foo";
		String snk = " { checkpointInjector => [console, counter(\"" + rpt + "\" ) ] }";
		for (int i = 0; i < 100; i++) {
			EventSink es = FlumeBuilder.buildSink(LogicalNodeContext.testingContext(), snk);
			es.open();
			es.close();
		}
	}

	/**
	 * CheckpointInject 는 250ms 마다  롤링 이벤트를 내보낸다.
	 * 아래와 같이 메세지를 보내는 간격 마다 50ms 씩 쉬면 10개를 보내는 동안
	 * 1번의 rotate가 일어난다. 
	 * 따라서 처음 open()할 때 open event가 보내지고 
	 * rotate 할 때 close, open event가 보내지고
	 * close() 할 때 close event가 보내져서 로그 이벤트 보다 
	 * +4개가 추가로 보내진다.
	 * @throws FlumeSpecException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Test
	public void testBehavior() throws FlumeSpecException, IOException, InterruptedException {
		
		int count = 10;
		String rpt = "foo";
		String snk = " { checkpointInjector => [console, counter(\"" + rpt + "\" ) ] }";
		 
		EventSink es = FlumeBuilder.buildSink(new ReportTestingContext(
				LogicalNodeContext.testingContext()), snk);
		
		es.open();
		for(int i = 0; i < count; i++) {
			Event e = new EventImpl(("test message" + i).getBytes());
			System.out.println("initial append: " + e);
			es.append(e);
			Clock.sleep(50);
		}
		Clock.sleep(5000);
		es.close();
		
		CounterSink ctr = (CounterSink) ReportManager.get().getReportable(rpt);
		Assert.assertEquals(count + 4, ctr.getCount());
	}

}
