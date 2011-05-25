package com.nexr.rolling.schd;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.scheduling.quartz.QuartzJobBean;

import com.nexr.framework.workflow.JobLauncher;
import com.nexr.framework.workflow.SpringWorkflowManager;
import com.nexr.framework.workflow.WorkflowManager;
import com.nexr.rolling.workflow.RollingConstants;
import com.nexr.rolling.workflow.job.RollingJob;
import com.nexr.rolling.workflow.mapred.HourlyRollingMr;
import com.nexr.sdp.Configuration;

public class HourlyTask extends QuartzJobBean {
	private static final Logger log = Logger.getLogger(HourlyTask.class);
	private WorkflowManager workflowManager = new SpringWorkflowManager();
	
	@Override
	protected void executeInternal(JobExecutionContext context)
			throws JobExecutionException {
		log.info("HourlyRolling Job Start");
		Configuration config = Configuration.getInstance();
		JobLauncher launcher = workflowManager.createLauncher();
		RollingJob job = workflowManager.getJobFactory().createJob(RollingJob.class);
		
		job.addParameter(RollingConstants.JOB_TYPE, "hourly");
		job.addParameter(RollingConstants.JOB_CLASS, job.getClass().getName());
		job.addParameter(RollingConstants.MR_CLASS, HourlyRollingMr.class.getName());
		job.addParameter(RollingConstants.DATETIME, new SimpleDateFormat("yyyy-MM-dd HH").format(new Date()));
		job.addParameter(RollingConstants.RAW_PATH, config.getResultDir(config.getRollingDir(), "post"));
		job.addParameter(RollingConstants.INPUT_PATH, config.getInputDir(config.getRollingDir(), "hourly"));
		job.addParameter(RollingConstants.OUTPUT_PATH, config.getOutputDir(config.getRollingDir(), "hourly"));
		job.addParameter(RollingConstants.RESULT_PATH, config.getResultDir(config.getRollingDir(), "hourly"));
		
		try {
			launcher.run(job);
		} catch (com.nexr.framework.workflow.JobExecutionException e) {
			e.printStackTrace();
		}
	}
}
