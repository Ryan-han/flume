package com.nexr.rolling.schd;

import java.util.Date;

import org.apache.log4j.Logger;
import org.quartz.CronTrigger;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;

import com.nexr.sdp.Configuration;

public class RollingScheduler {

	private static final Logger log = Logger.getLogger(RollingScheduler.class);

	static SchedulerFactory schedFact = new org.quartz.impl.StdSchedulerFactory();
	private static Scheduler sched = null;
	Scheduler scheduler = null;
	JobDetail hourlyJob = null;
	JobDetail dailyJob = null;
	JobDetail postJob = null;
	
	public static Scheduler getInstance() {
		if (sched == null) {
			try {
				sched = schedFact.getScheduler();
			} catch (SchedulerException e) {
				e.printStackTrace();
			}
		}
		return sched;
	}
	
	public void startScheuler() throws Exception {
		try {
			RollingScheduler.getInstance().start();
			log.info("Start Scheduler");
		} catch (SchedulerException e) {
			throw e;
		}
	}

	public void restartScheduler() throws Exception {
		RollingScheduler.getInstance().shutdown(true);
		sched = null;
		startScheuler();
	}
	
	public void addPostJobToScheduler(Configuration config)
			throws Exception {
		String postScheduleName = "Post Rolling";
		postJob = new JobDetail();
		postJob.getJobDataMap().put("config", config);
		
		postJob.setName(postScheduleName);
		postJob.setGroup(postScheduleName + " group");
		postJob.setJobClass(com.nexr.rolling.schd.PostTask.class);
		
		CronTrigger postCronTrigger = new CronTrigger();
		postCronTrigger.setName(postScheduleName);
		postCronTrigger.setGroup(postScheduleName  + " group");
		postCronTrigger.setJobName(postScheduleName);
		postCronTrigger.setJobGroup(postScheduleName + " group");
		postCronTrigger.setStartTime(new Date());
		
		if (log.isInfoEnabled()) {
			log.info("CRON_TYPE Expression : " + config.getScheduleExpression("post"));
		}
		postCronTrigger.setCronExpression(config.getScheduleExpression("post").trim());
		getInstance().scheduleJob(postJob, postCronTrigger);
	}
	
	public void addHourlyJobToScheduler(Configuration config)
			throws Exception {
		String hourlyScheduleName = "Hourly Rolling";
		hourlyJob = new JobDetail();
		hourlyJob.getJobDataMap().put("config", config);

		hourlyJob.setName(hourlyScheduleName);
		hourlyJob.setGroup(hourlyScheduleName + " group");
		hourlyJob.setJobClass(com.nexr.rolling.schd.HourlyTask.class);

		CronTrigger hourlyCronTrigger = new CronTrigger();
		hourlyCronTrigger.setName(hourlyScheduleName);
		hourlyCronTrigger.setGroup(hourlyScheduleName  + " group");
		hourlyCronTrigger.setJobName(hourlyScheduleName);
		hourlyCronTrigger.setJobGroup(hourlyScheduleName + " group");
		hourlyCronTrigger.setStartTime(new Date());

		if (log.isInfoEnabled()) {
			log.info("CRON_TYPE Expression : " + config.getScheduleExpression("hourly"));
		}
		hourlyCronTrigger.setCronExpression(config.getScheduleExpression("hourly").trim());
		getInstance().scheduleJob(hourlyJob, hourlyCronTrigger);
	}

	public void addDailyJobToScheduler(Configuration config)
			throws Exception {
		String dailyScheduleName = "Daily Rolling";
		dailyJob = new JobDetail();
		dailyJob.getJobDataMap().put("config", config);

		dailyJob.setName(dailyScheduleName);
		dailyJob.setGroup(dailyScheduleName + " group");
		dailyJob.setJobClass(com.nexr.rolling.schd.DailyTask.class);

		CronTrigger dailyCronTrigger = new CronTrigger();
		dailyCronTrigger.setName(dailyScheduleName);
		dailyCronTrigger.setGroup(dailyScheduleName + " group");
		dailyCronTrigger.setJobName(dailyScheduleName);
		dailyCronTrigger.setJobGroup(dailyScheduleName + " group");
		dailyCronTrigger.setStartTime(new Date());
		if (log.isInfoEnabled()) {
			log.info("CRON_TYPE Expression : " + config.getScheduleExpression("daily"));
		}
		dailyCronTrigger.setCronExpression(config.getScheduleExpression("daily").trim());
		getInstance().scheduleJob(dailyJob, dailyCronTrigger);
	}

}
