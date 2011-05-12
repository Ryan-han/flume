package com.nexr.rolling.schd;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.scheduling.quartz.QuartzJobBean;

import com.nexr.data.sdp.rolling.mr.DailyRollingMr;
import com.nexr.framework.workflow.JobLauncher;
import com.nexr.rolling.core.RollingConfig;
import com.nexr.rolling.workflow.RollingConstants;
import com.nexr.rolling.workflow.job.RollingJob;

public class DailyTask extends QuartzJobBean {
	private static final Logger log = Logger.getLogger(DailyTask.class);
	
	@Override
	protected void executeInternal(JobExecutionContext context)
			throws JobExecutionException {
		RollingConfig config = (RollingConfig) context.getJobDetail().getJobDataMap().get("config");
		log.info("DailyRolling Job Start");

		ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext("classpath:workflow-app.xml");
		JobLauncher launcher = ctx.getBean(JobLauncher.class);
		RollingJob job = ctx.getBean(RollingJob.class);
		
		job.addParameter(RollingConstants.JOB_TYPE, "daily");
		job.addParameter(RollingConstants.JOB_CLASS, job.getClass().getName());
		job.addParameter(RollingConstants.MR_CLASS, DailyRollingMr.class.getName());
		job.addParameter(RollingConstants.DATETIME, new SimpleDateFormat("yyyy-MM-dd").format(new Date()));
		job.addParameter(RollingConstants.RAW_PATH, config.getHourlyMrResultPath());
		job.addParameter(RollingConstants.INPUT_PATH, config.getDailyMrInputPath());
		job.addParameter(RollingConstants.OUTPUT_PATH, config.getDailyMrOutputPath());
		job.addParameter(RollingConstants.RESULT_PATH, config.getDailyMrResultPath());
		
		try {
			launcher.run(job);
		} catch (com.nexr.framework.workflow.JobExecutionException e) {
			e.printStackTrace();
		}
	}
}
