package com.nexr.rolling.workflow;

import java.util.List;

import javax.annotation.Resource;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.nexr.framework.workflow.AbstractJob;
import com.nexr.framework.workflow.JobExecution;
import com.nexr.framework.workflow.JobLauncher;
import com.nexr.framework.workflow.JobTestBase;
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
public class TestWorkflowUsingZK extends JobTestBase {
	@Resource
	private JobLauncher launcher;
	private AbstractJob job;

	private ZKJobExecutionDao executionDao;

	@Before
	public void init() {
		Steps steps = new Steps();
		steps.add(new Step("init", FirstTasklet.class));
		executionDao = new ZKJobExecutionDao();
		job = createSimpleJob("test", steps, executionDao);
		job.setJobExecutionListener(new JobExecutionListenerImpl());
		StepExecutionListener steplistener = new StepExecutionListenerImpl(executionDao);
		job.setStepExecutionListener(steplistener);
	}
	
	@Test
	public void testJob() throws Exception {
		job.addParameter("param1", "1");
		job.addParameter("param2", "2");
		job.addParameter("time", System.currentTimeMillis() + "");
		launcher.run(job);
		executionDao.removeJob("/complete", job);
	}
	
	@Test
	public void testRecovery1() throws Exception {
		List<JobExecution> executions = executionDao.findFailExecutions();
		for (JobExecution execution : executions) {
			launcher.run(execution.getJob());
		}
	}
	
	public static class FirstTasklet implements Tasklet {
		@Override
		public String run(StepContext context) {
			System.out.println("first");
			return null;
		}
	}
}
