package com.nexr.rolling.workflow;

/**
 * @author dani.kim@nexr.com
 */
public class RollingConstants {
	public static final String JOB_TYPE = "job.type";
	public static final String JOB_CLASS = "job.class";
	public static final String DATETIME = "job.datetime";
	public static final String IS_COLLECTOR_SOURCE = "is.collector.source";
	public static final String MR_CLASS = "job.mapred.class";
	
	public static final String RAW_PATH = "raw.path";
	public static final String INPUT_PATH = "input.path";
	public static final String OUTPUT_PATH = "output.path";
	public static final String RESULT_PATH = "result.path";
	
	public static final String TODAY_PATH = "today.path";
	public static final String INPUT_DEPTH = "input.depth";
	public static final String NOTIFY_ZKPATH_AFTER_ROLLING = "rolling.notify.zk.path";
}
