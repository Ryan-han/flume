package com.nexr.rolling.workflow;

import javax.annotation.Resource;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.nexr.framework.workflow.Job;
import com.nexr.framework.workflow.JobLauncher;
import com.nexr.framework.workflow.JobTestBase;
import com.nexr.framework.workflow.SimpleJob;
import com.nexr.framework.workflow.Step;
import com.nexr.framework.workflow.StepContext;
import com.nexr.framework.workflow.Steps;
import com.nexr.framework.workflow.Tasklet;
import com.nexr.framework.workflow.listener.StepExecutionListener;

/**
 * @author dani.kim@nexr.com
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:workflow-app.xml")
public class TestRollingWorkflow extends JobTestBase {
	@Resource
	private JobLauncher launcher;
	
	private Job job;

	private ZKJobExecutionDao executionDao;

	@Before
	public void init() {
		Steps steps = new Steps();
		steps.add(new Step("init", FirstTasklet.class));
		executionDao = new ZKJobExecutionDao();
		job = createSimpleJob("test", steps, executionDao);
		SimpleJob sjob = (SimpleJob) this.job;
		if (job instanceof SimpleJob) {
			sjob.setJobExecutionListener(new JobExecutionListenerImpl());
			StepExecutionListener steplistener = new StepExecutionListenerImpl(executionDao);
			sjob.setStepExecutionListener(steplistener);
		}
	}
	
	@Test
	public void testJob() throws Exception {
		job.addParameter("param1", "1");
		job.addParameter("param2", "2");
		job.addParameter("time", System.currentTimeMillis() + "");
		launcher.run(job);
		executionDao.removeJob(job);
	}
	
	@Test
	public void testListener() throws Exception {
		launcher.run(job);
		try {
			launcher.run(job);
			Assert.fail();
		} catch (Exception e) {
		}
	}
	
	@Test
	public void testRecoverty() throws Exception {
	}
	
	public static class FirstTasklet implements Tasklet {
		@Override
		public String run(StepContext context) {
			System.out.println("first");
			return null;
		}
	}
}
