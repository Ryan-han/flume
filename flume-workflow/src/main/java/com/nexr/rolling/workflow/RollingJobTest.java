package com.nexr.rolling.workflow;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.nexr.data.sdp.rolling.mr.DailyRollingMr;
import com.nexr.data.sdp.rolling.mr.HourlyRollingMr;
import com.nexr.data.sdp.rolling.mr.PostRollingMr;
import com.nexr.dedup.DuplicateManager;
import com.nexr.framework.workflow.JobLauncher;
import com.nexr.rolling.workflow.job.RollingJob;

/**
 * @author dani.kim@nexr.com
 */
public class RollingJobTest {
	public static void main(String[] args) {
		new Thread(new DuplicateManager()).start();
		
		ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext("classpath:workflow-app.xml");
		JobLauncher launcher = ctx.getBean(JobLauncher.class);
		ZkClientFactory.getClient().deleteRecursive("/rolling");
		ZkClientFactory.getClient().deleteRecursive("/dedup");
		try {
			launcher.run(createPostRollingJob(ctx.getBean(RollingJob.class)));
			launcher.run(createHourlyRollingJob(ctx.getBean(RollingJob.class)));
			launcher.run(createDailyRollingJob(ctx.getBean(RollingJob.class)));
		} catch (com.nexr.framework.workflow.JobExecutionException e) {
			e.printStackTrace();
		}
	}
	
	private static RollingJob createPostRollingJob(RollingJob job) {
		job.addParameter(RollingConstants.JOB_TYPE, "post");
		job.addParameter(RollingConstants.IS_COLLECTOR_SOURCE, "true");
		job.addParameter(RollingConstants.TODAY_PATH, "/nexr/rolling/today");
		job.addParameter(RollingConstants.JOB_CLASS, job.getClass().getName());
		job.addParameter(RollingConstants.MR_CLASS, PostRollingMr.class.getName());
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
		job.addParameter(RollingConstants.MR_CLASS, DailyRollingMr.class.getName());
		job.addParameter(RollingConstants.DATETIME, new SimpleDateFormat("yyyy-MM-dd").format(new Date()));
		job.addParameter(RollingConstants.RAW_PATH, "/nexr/rolling/hourly/result");
		job.addParameter(RollingConstants.INPUT_PATH, "/nexr/rolling/daily/input");
		job.addParameter(RollingConstants.OUTPUT_PATH, "/nexr/rolling/daily/output");
		job.addParameter(RollingConstants.RESULT_PATH, "/nexr/rolling/daily/result");
		return job;
	}
}
