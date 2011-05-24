package com.cloudera.flume.filter;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.zip.CRC32;

import junit.framework.Assert;

import org.junit.Test;

import com.cloudera.flume.collector.MemorySink;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.handlers.filter.XmlFilter;
import com.cloudera.flume.handlers.text.TailSource;

public class TestXmlFilter {
	@Test
	public void testParsing() throws IOException, InterruptedException {
		MemorySink s = new MemorySink("memorySink");
		XmlFilter xmlFilter = new XmlFilter(s, "logicalNodeName");

		String bodyStr = "<TransactionLog>"
				+ "<SystemHeader>"
				+ "  <CorpID>KT</CorpID>"
				+ "  <SystemID>SDP</SystemID>"
				+ "  <LogType>TRANSACTION</LogType>"
				+ "  <UserID>10301276</UserID>"
				+ "  <ScreenId>LoginPage</ScreenId>"
				+ " </SystemHeader>"
				+ " <DataHeader>"
				+ "  <TransactionID>3c72c8a9-f4d0-3f83-9117-1a3c2f7a1ebd</TransactionID>"
				+ "  <SeqNumber>1</SeqNumber>"
				+ "  <Timestamp>2011-04-20 21:02:55.530</Timestamp>"
				+ "  <Moduletype>CSM</Moduletype>"
				+ "  <ServerName>MW75GRHVE4KMZ9</ServerName>"
				+ "  <ServerIP>147.6.152.162</ServerIP>"
				+ "  <ObjectName>Test.LogTest</ObjectName>"
				+ "  <MethodName>IN_REQ_TEST</MethodName>"
				+ "  <CounterPartName></CounterPartName>"
				+ "  <CounterPartIP></CounterPartIP>"
				+ "  <ServiceName></ServiceName>"
				+ " </DataHeader>"
				+ " <Body>"
				+ "  <Logtype>IN_REQ</Logtype>"
				+ "  <ResultCode></ResultCode>"
				+ "  <ResultDescription></ResultDescription>"
				+ "  <PayLoad>ADDRESS_IN_REQ=SEOUL|!|NAME_IN_REQ=SUNG-KEUN</PayLoad>"
				+ " </Body>" + "</TransactionLog>";
		byte[] body = bodyStr.getBytes();

		Event e = new EventImpl(body);
		e.set(TailSource.A_TAILSRCFILE, "testfile".getBytes());

		xmlFilter.open();
		xmlFilter.append(e);
		xmlFilter.close();

		Assert.assertEquals(1, s.eventList.size());
		Map<String, byte[]> attrMap = s.eventList.get(0).getAttrs();
		
		Assert.assertTrue(Arrays.equals("KT".getBytes(), attrMap.get("SystemHeader.CorpID")));
		Assert.assertTrue(Arrays.equals("SDP".getBytes(), attrMap.get("SystemHeader.SystemID")));
		Assert.assertTrue(Arrays.equals("TRANSACTION".getBytes(), attrMap.get("SystemHeader.LogType")));
		Assert.assertTrue(Arrays.equals("10301276".getBytes(), attrMap.get("SystemHeader.UserID")));
		Assert.assertTrue(Arrays.equals("LoginPage".getBytes(), attrMap.get("SystemHeader.ScreenId")));
		Assert.assertTrue(Arrays.equals("3c72c8a9-f4d0-3f83-9117-1a3c2f7a1ebd".getBytes(), attrMap.get("DataHeader.TransactionID")));
		Assert.assertTrue(Arrays.equals("1".getBytes(), attrMap.get("DataHeader.SeqNumber")));
		Assert.assertTrue(Arrays.equals("2011-04-20 21:02:55.530".getBytes(), attrMap.get("DataHeader.Timestamp")));
		Assert.assertTrue(Arrays.equals("CSM".getBytes(), attrMap.get("DataHeader.Moduletype")));
		Assert.assertTrue(Arrays.equals("MW75GRHVE4KMZ9".getBytes(), attrMap.get("DataHeader.ServerName")));
		Assert.assertTrue(Arrays.equals("147.6.152.162".getBytes(), attrMap.get("DataHeader.ServerIP")));
		Assert.assertTrue(Arrays.equals("Test.LogTest".getBytes(), attrMap.get("DataHeader.ObjectName")));
		Assert.assertTrue(Arrays.equals("IN_REQ_TEST".getBytes(), attrMap.get("DataHeader.MethodName")));
		Assert.assertNull(attrMap.get("DataHeader.CounterPartName"));
		Assert.assertNull(attrMap.get("DataHeader.CounterPartIP"));
		Assert.assertNull(attrMap.get("DataHeader.ServiceName"));
		Assert.assertTrue(Arrays.equals("IN_REQ".getBytes(), attrMap.get("Body.Logtype")));
		Assert.assertNull(attrMap.get("Body.ResultCode"));
		Assert.assertNull(attrMap.get("Body.ResultDescription"));
		Assert.assertTrue(Arrays.equals("ADDRESS_IN_REQ=SEOUL|!|NAME_IN_REQ=SUNG-KEUN".getBytes(), attrMap.get("Body.PayLoad")));
		
		CRC32 crc = new CRC32();
		crc.reset();
		crc.update(e.getBody());
		String logId = "logicalNodeNametestfile"+crc.getValue();
		System.out.println(new String(logId.getBytes()));
		System.out.println(new String(attrMap.get(XmlFilter.LOG_ID)));
		Assert.assertTrue(Arrays.equals(logId.getBytes(), attrMap.get(XmlFilter.LOG_ID)));
	}
}
