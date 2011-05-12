package com.nexr.rolling.workflow;

import javax.annotation.Resource;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.nexr.framework.workflow.JobLauncher;
import com.nexr.rolling.workflow.job.RollingJob;

/**
 * @author dani.kim@nexr.com
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:workflow-app.xml")
public class TestRollingJob {
	@Resource
	private JobLauncher launcher;
	@Resource
	private RollingJob job;
	
	@Before
	public void init() throws Exception {
		ZkClientFactory.getClient().deleteRecursive("/rolling/jobs/running");
	}
	
	@Test
	public void testRun() throws Exception {
		job.addParameter(RollingConstants.JOB_TYPE, "post");
		job.addParameter(RollingConstants.JOB_CLASS, job.getClass().getName());
		job.addParameter(RollingConstants.DATETIME, "2011-01-01 01:01");
		job.addParameter(RollingConstants.RAW_PATH, "/nexr/rolling/hourly/raw");
		job.addParameter(RollingConstants.INPUT_PATH, "/nexr/rolling/hourly/input");
		job.addParameter(RollingConstants.OUTPUT_PATH, "/nexr/rolling/hourly/output");
		job.addParameter(RollingConstants.RESULT_PATH, "/nexr/rolling/hourly/result");
		launcher.run(job);
	}
}
