package com.nexr.rolling.schd;

import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;
import org.quartz.CronTrigger;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;


import com.nexr.rolling.core.RollingConfig;

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
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return sched;
	}
	
	public void startScheuler() throws Exception {
		try {
			RollingScheduler.getInstance().start();
			log.info("Start Scheduler");
		} catch (SchedulerException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
	}

	public void restartScheduler() throws Exception {
		RollingScheduler.getInstance().shutdown(true);
		sched = null;
		startScheuler();
	}
	
	public void addPostJobToScheduler(RollingConfig rollingConfig)
			throws Exception {
		String postScheduleName = "Post Rolling";
		postJob = new JobDetail();
		postJob.getJobDataMap().put("config", rollingConfig);
		
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
			log.info("CRON_TYPE Expression : "
					+ rollingConfig.getHourlySchedule());
		}
		
		postCronTrigger.setCronExpression(rollingConfig.getPostSchedule()
				.trim());
		getInstance().scheduleJob(postJob, postCronTrigger);
	}
	
	public void addHourlyJobToScheduler(RollingConfig rollingConfig)
			throws Exception {
		String hourlyScheduleName = "Hourly Rolling";
		hourlyJob = new JobDetail();
		hourlyJob.getJobDataMap().put("config", rollingConfig);

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
			log.info("CRON_TYPE Expression : "
					+ rollingConfig.getHourlySchedule());
		}

		hourlyCronTrigger.setCronExpression(rollingConfig.getHourlySchedule()
				.trim());
		getInstance().scheduleJob(hourlyJob, hourlyCronTrigger);
	}

	public void addDailyJobToScheduler(RollingConfig rollingConfig)
			throws Exception {
		String dailyScheduleName = "Daily Rolling";
		dailyJob = new JobDetail();
		dailyJob.getJobDataMap().put("config", rollingConfig);

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
			log.info("CRON_TYPE Expression : "
					+ rollingConfig.getDailySchedule());
		}

		dailyCronTrigger.setCronExpression(rollingConfig.getDailySchedule()
				.trim());
		getInstance().scheduleJob(dailyJob, dailyCronTrigger);
	}

}
