package com.nexr.rolling.core;

import org.apache.log4j.Logger;

public class RollingConfig {

	private static final Logger log = Logger.getLogger(RollingConfig.class);

	public static final String RAW_PATH = "raw.path";
	
	public static final String POST_MR_RESULT_PATH = "post.mr.result.path";
	public static final String POST_MR_INPUT_PATH = "post.mr.input.path";
	public static final String POST_MR_OUTPUT_PATH = "post.mr.output.path";

	public static final String HOURLY_MR_RESULT_PATH = "hourly.mr.result.path";
	public static final String HOURLY_MR_INPUT_PATH = "hourly.mr.input.path";
	public static final String HOURLY_MR_OUTPUT_PATH = "hourly.mr.output.path";

	public static final String DAILY_MR_RESULT_PATH = "daily.mr.result.path";
	public static final String DAILY_MR_INPUT_PATH = "daily.mr.input.path";
	public static final String DAILY_MR_OUTPUT_PATH = "daily.mr.output.path";

	public static final String RETRY_COUNT = "retry.count";
	public static final String RETRY_DELAY = "retry.delay.time";
	public static final String NOTIFY_EMAIL = "notify.email";

	public static final String POST_SCHEDULE = "post.schedule";
	public static final String HOURLY_SCHEDULE = "hourly.schedule";
	public static final String DAILY_SCHEDULE = "daily.schedule";
	
	public static final String DEDUP_MR_INPUT_PATH = "dedup.mr.input.path";
	public static final String DEDUP_MR_OUTPUT_PATH = "dedup.mr.output.path";
	public static final String DEDUP_POST_PATH = "dedup.post.path";
	public static final String DEDUP_HOURLY_PATH = "dedup.hourly.path";
	public static final String DEDUP_DAILY_PATH = "dedup.daily.path";

	private String rawPath;
	private String postMrResultPath;
	private String postMrInputPath;
	private String postMrOutputPath;
	
	private String hourlyMrResultPath;
	private String hourlyMrInputPath;
	private String hourlyMrOutputPath;
	
	private String dailyMrResultPath;
	private String dailyMrInputPath;
	private String dailyMrOutputPath;
	
	private int retryCount;
	private long retryDelaytime;
	private String notifyEmail;
	
	private String postSchedule;
	private String hourlySchedule;
	private String dailySchedule;
	
	private String dedupMrInputPath;
	private String dedupMrOutputPath;

	private String dedupPostPath;
	private String dedupHourlyPath;
	private String dedupDailyPath;
	
	private String todayPath;

//	public String getRawPath() {
//		return rawPath;
//	}
//
//	public void setRawPath(String rawPath) {
//		this.rawPath = rawPath;
//	}
//
//	public String getHourlyMrResultPath() {
//		return hourlyMrResultPath;
//	}
//
//	public void setHourlyMrResultPath(String hourlyMrResultPath) {
//		this.hourlyMrResultPath = hourlyMrResultPath;
//	}
//
//	public String getHourlyMrInputPath() {
//		return hourlyMrInputPath;
//	}
//
//	public void setHourlyMrInputPath(String hourlyMrInputPath) {
//		this.hourlyMrInputPath = hourlyMrInputPath;
//	}
//
//	public String getHourlyMrOutputPath() {
//		return hourlyMrOutputPath;
//	}
//
//	public void setHourlyMrOutputPath(String hourlyMrOutputPath) {
//		this.hourlyMrOutputPath = hourlyMrOutputPath;
//	}
//
//	public String getDailyMrResultPath() {
//		return dailyMrResultPath;
//	}
//
//	public void setDailyMrResultPath(String dailyMrResultPath) {
//		this.dailyMrResultPath = dailyMrResultPath;
//	}
//
//	public String getDailyMrInputPath() {
//		return dailyMrInputPath;
//	}
//
//	public void setDailyMrInputPath(String dailyMrInputPath) {
//		this.dailyMrInputPath = dailyMrInputPath;
//	}
//
//	public String getDailyMrOutputPath() {
//		return dailyMrOutputPath;
//	}
//
//	public void setDailyMrOutputPath(String dailyMrOutputPath) {
//		this.dailyMrOutputPath = dailyMrOutputPath;
//	}
//
//	public int getRetryCount() {
//		return retryCount;
//	}
//
//	public void setRetryCount(int retryCount) {
//		this.retryCount = retryCount;
//	}
//
//	public long getRetryDelaytime() {
//		return retryDelaytime;
//	}
//
//	public void setRetryDelaytime(long retryDelaytime) {
//		this.retryDelaytime = retryDelaytime;
//	}
//
//	public String getNotifyEmail() {
//		return notifyEmail;
//	}
//
//	public void setNotifyEmail(String notifyEmail) {
//		this.notifyEmail = notifyEmail;
//	}
//
//	public String getHourlySchedule() {
//		return hourlySchedule;
//	}
//
//	public void setHourlySchedule(String hourlySchedule) {
//		this.hourlySchedule = hourlySchedule;
//	}
//
//	public String getDailySchedule() {
//		return dailySchedule;
//	}
//
//	public void setDailySchedule(String dailySchedule) {
//		this.dailySchedule = dailySchedule;
//	}
//
//	public String getPostMrResultPath() {
//		return postMrResultPath;
//	}
//
//	public void setPostMrResultPath(String postMrResultPath) {
//		this.postMrResultPath = postMrResultPath;
//	}
//
//	public String getPostMrInputPath() {
//		return postMrInputPath;
//	}
//
//	public void setPostMrInputPath(String postMrInputPath) {
//		this.postMrInputPath = postMrInputPath;
//	}
//
//	public String getPostMrOutputPath() {
//		return postMrOutputPath;
//	}
//
//	public void setPostMrOutputPath(String postMrOutputPath) {
//		this.postMrOutputPath = postMrOutputPath;
//	}
//
//	public String getPostSchedule() {
//		return postSchedule;
//	}
//
//	public void setPostSchedule(String postSchedule) {
//		this.postSchedule = postSchedule;
//	}
//
//	public String getDedupMrInputPath() {
//		return dedupMrInputPath;
//	}
//
//	public void setDedupMrInputPath(String dedupMrInputPath) {
//		this.dedupMrInputPath = dedupMrInputPath;
//	}
//
//	public String getDedupMrOutputPath() {
//		return dedupMrOutputPath;
//	}
//
//	public void setDedupMrOutputPath(String dedupMrOutputPath) {
//		this.dedupMrOutputPath = dedupMrOutputPath;
//	}
//
//	public String getDedupPostPath() {
//		return dedupPostPath;
//	}
//
//	public void setDedupPostPath(String dedupPostPath) {
//		this.dedupPostPath = dedupPostPath;
//	}
//
//	public String getDedupHourlyPath() {
//		return dedupHourlyPath;
//	}
//
//	public void setDedupHourlyPath(String dedupHourlyPath) {
//		this.dedupHourlyPath = dedupHourlyPath;
//	}
//
//	public String getDedupDailyPath() {
//		return dedupDailyPath;
//	}
//
//	public void setDedupDailyPath(String dedupDailyPath) {
//		this.dedupDailyPath = dedupDailyPath;
//	}
//	
//	public String getTodayPath() {
//		return todayPath;
//	}
//	
//	public void setTodayPath(String todayPath) {
//		this.todayPath = todayPath;
//	}

	@Override
	public String toString() {
		return "RollingConfig [rawPath=" + rawPath + ", postMrResultPath="
				+ postMrResultPath + ", postMrInputPath=" + postMrInputPath
				+ ", postMrOutputPath=" + postMrOutputPath
				+ ", hourlyMrResultPath=" + hourlyMrResultPath
				+ ", hourlyMrInputPath=" + hourlyMrInputPath
				+ ", hourlyMrOutputPath=" + hourlyMrOutputPath
				+ ", dailyMrResultPath=" + dailyMrResultPath
				+ ", dailyMrInputPath=" + dailyMrInputPath
				+ ", dailyMrOutputPath=" + dailyMrOutputPath + ", retryCount="
				+ retryCount + ", retryDelaytime=" + retryDelaytime
				+ ", notifyEmail=" + notifyEmail + ", postSchedule="
				+ postSchedule + ", hourlySchedule=" + hourlySchedule
				+ ", dailySchedule=" + dailySchedule + ", dedupMrInputPath="
				+ dedupMrInputPath + ", dedupMrOutputPath=" + dedupMrOutputPath
				+ ", dedupPostPath=" + dedupPostPath + ", dedupHourlyPath="
				+ dedupHourlyPath + ", dedupDailyPath=" + dedupDailyPath + "]";
	}

}
