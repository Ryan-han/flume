package com.cloudera.flume.handlers.hdfs;

import java.util.ArrayList;
import java.util.List;

import com.cloudera.flume.core.Event;

public class LogKey extends DetailEventKey {
	
	public static final String DATA_TYPE = "SystemHeader.LogType";
	public static final String TIME = "DataHeader.Timestamp";
	public static final String LOG_ID= "DataHeader.TransactionID"; //TODO 새로 생성해 주어야 한다. UUID로 

	public LogKey(Event e) {
		super(e);
		List<String> keys = new ArrayList<String>();
		keys.add(DATA_TYPE);
		keys.add(TIME);
		keys.add(LOG_ID);
		init(keys);
	}
	
	public LogKey() {
		super();
	}
}
