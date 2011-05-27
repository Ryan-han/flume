package com.nexr.data.sdp.rolling.hdfs;

import java.text.ParseException;

import junit.framework.Assert;

import org.junit.Test;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.handlers.filter.XmlFilter;

public class TestLogRecordKey {
	@Test
	public void testTimeParse() throws ParseException {
		Event e = new EventImpl();
		
		String date = "2010-05-027 13:20:30.000";
		
		e.set(LogRecordKey.DATA_TYPE, "testType".getBytes());
		e.set(LogRecordKey.TIME, date.getBytes());
		e.set(XmlFilter.LOG_ID, "testLogID".getBytes());
		
		LogRecordKey key = new LogRecordKey(e);
		
		String parsedTime = key.getTime();
		
		long expectedTime = LogRecordKey.formatter.parse(date).getTime();
		
		Assert.assertEquals(expectedTime, Long.parseLong(parsedTime));
	}
}
