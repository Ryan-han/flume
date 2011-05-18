package com.nexr.rolling.schd;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.scheduling.quartz.QuartzJobBean;

import com.nexr.data.sdp.rolling.mr.HourlyRollingMr;
import com.nexr.framework.workflow.JobLauncher;
import com.nexr.rolling.core.RollingConfig;
import com.nexr.rolling.workflow.RollingConstants;
import com.nexr.rolling.workflow.job.RollingJob;

public class HourlyTask extends QuartzJobBean {
	
	private static final Logger log = Logger.getLogger(HourlyTask.class);
	
	@Override
	protected void executeInternal(JobExecutionContext context)
			throws JobExecutionException {
		RollingConfig config = (RollingConfig) context.getJobDetail().getJobDataMap().get("config");
		log.info("HourlyRolling Job Start");
		
		ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext("classpath:workflow-app.xml");
		JobLauncher launcher = ctx.getBean(JobLauncher.class);
		RollingJob job = ctx.getBean(RollingJob.class);
		
		job.addParameter(RollingConstants.JOB_TYPE, "hourly");
		job.addParameter(RollingConstants.JOB_CLASS, job.getClass().getName());
		job.addParameter(RollingConstants.MR_CLASS, HourlyRollingMr.class.getName());
		job.addParameter(RollingConstants.DATETIME, new SimpleDateFormat("yyyy-MM-dd HH").format(new Date()));
		job.addParameter(RollingConstants.RAW_PATH, config.getRawPath());
		job.addParameter(RollingConstants.INPUT_PATH, config.getHourlyMrInputPath());
		job.addParameter(RollingConstants.OUTPUT_PATH, config.getHourlyMrOutputPath());
		job.addParameter(RollingConstants.RESULT_PATH, config.getHourlyMrResultPath());
		
		try {
			launcher.run(job);
		} catch (com.nexr.framework.workflow.JobExecutionException e) {
			e.printStackTrace();
		}
	}
}
