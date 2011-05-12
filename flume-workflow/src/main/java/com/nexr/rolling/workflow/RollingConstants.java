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
	
	@Deprecated
	public static final String HOURLY_MR_RAW_PATH = "";
	@Deprecated
	public static final String HOURLY_MR_INPUT_PATH = "";
	@Deprecated
	public static final String HOURLY_MR_OUTPUT_PATH = "";
	@Deprecated
	public static final String HOURLY_MR_RESULT_PATH = "";
	@Deprecated
	public static final String DAILY_MR_INPUT_PATH = "";
	@Deprecated
	public static final String DAILY_MR_OUTPUT_PATH = "";
	@Deprecated
	public static final String DAILY_MR_RESULT_PATH = "";
}
