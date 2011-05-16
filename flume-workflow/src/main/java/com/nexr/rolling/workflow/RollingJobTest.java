package com.nexr.rolling.workflow;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.nexr.data.sdp.rolling.mr.HourlyRollingMr;
import com.nexr.framework.workflow.JobLauncher;
import com.nexr.rolling.workflow.job.RollingJob;

/**
 * @author dani.kim@nexr.com
 */
public class RollingJobTest {
	public static void main(String[] args) {
		ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext("classpath:workflow-app.xml");
		JobLauncher launcher = ctx.getBean(JobLauncher.class);
		RollingJob job = ctx.getBean(RollingJob.class);
		try {
			launcher.run(createPostRollingJob(job));
			launcher.run(createHourlyRollingJob(job));
			launcher.run(createDailyRollingJob(job));
		} catch (com.nexr.framework.workflow.JobExecutionException e) {
			e.printStackTrace();
		}
	}
	
	private static RollingJob createPostRollingJob(RollingJob job) {
		job.addParameter(RollingConstants.JOB_TYPE, "post");
		job.addParameter(RollingConstants.IS_COLLECTOR_SOURCE, "true");
		job.addParameter(RollingConstants.JOB_CLASS, job.getClass().getName());
		job.addParameter(RollingConstants.MR_CLASS, HourlyRollingMr.class.getName());
		job.addParameter(RollingConstants.DATETIME, new SimpleDateFormat("yyyy-MM-dd HH:mm").format(new Date()));
		job.addParameter(RollingConstants.RAW_PATH, "/nexr/rolling/post/raw");
		job.addParameter(RollingConstants.INPUT_PATH, "/nexr/rolling/post/input");
		job.addParameter(RollingConstants.OUTPUT_PATH, "/nexr/rolling/post/output");
		job.addParameter(RollingConstants.RESULT_PATH, "/nexr/rolling/post/result");
		return job;
	}

	private static RollingJob createHourlyRollingJob(RollingJob job) {
		job.addParameter(RollingConstants.JOB_TYPE, "hourly");
		job.addParameter(RollingConstants.JOB_CLASS, job.getClass().getName());
		job.addParameter(RollingConstants.MR_CLASS, HourlyRollingMr.class.getName());
		job.addParameter(RollingConstants.DATETIME, new SimpleDateFormat("yyyy-MM-dd HH").format(new Date()));
		job.addParameter(RollingConstants.RAW_PATH, "/nexr/rolling/post/result");
		job.addParameter(RollingConstants.INPUT_PATH, "/nexr/rolling/hourly/input");
		job.addParameter(RollingConstants.OUTPUT_PATH, "/nexr/rolling/hourly/output");
		job.addParameter(RollingConstants.RESULT_PATH, "/nexr/rolling/hourly/result");
		return job;
	}

	private static RollingJob createDailyRollingJob(RollingJob job) {
		job.addParameter(RollingConstants.JOB_TYPE, "daily");
		job.addParameter(RollingConstants.JOB_CLASS, job.getClass().getName());
		job.addParameter(RollingConstants.MR_CLASS, HourlyRollingMr.class.getName());
		job.addParameter(RollingConstants.DATETIME, new SimpleDateFormat("yyyy-MM-dd").format(new Date()));
		job.addParameter(RollingConstants.RAW_PATH, "/nexr/rolling/hourly/result");
		job.addParameter(RollingConstants.INPUT_PATH, "/nexr/rolling/daily/input");
		job.addParameter(RollingConstants.OUTPUT_PATH, "/nexr/rolling/daily/output");
		job.addParameter(RollingConstants.RESULT_PATH, "/nexr/rolling/daily/result");
		return job;
	}
}
