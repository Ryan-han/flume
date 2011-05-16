package com.nexr.framework.workflow;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.nexr.framework.workflow.listener.JobExecutionListener;
import com.nexr.framework.workflow.listener.JobLauncherListener;
import com.nexr.framework.workflow.listener.StepExecutionListener;

/**
 * @author dani.kim@nexr.com
 */
public abstract class JobTestBase {
	protected JobLauncher createLauncher(JobExecutionDao executionDao, JobLauncherListener launcherListener) {
		JobLauncherImpl launcher = new JobLauncherImpl();
		launcher.executionDao = executionDao;
		launcher.listener = launcherListener;
		return launcher;
	}
	
	protected AbstractJob createJob(String name, Steps steps) {
		return new FlowJob(name, steps);
	}
	
	protected AbstractJob createSimpleJob(String name, Steps steps, JobExecutionDao executionDao) {
		return createSimpleJob(name, steps, executionDao, null);
	}
	
	protected AbstractJob createSimpleJob(String name, Steps steps, JobExecutionDao executionDao, JobExecutionListener listener) {
		return createSimpleJob(name, steps, executionDao, listener, null);
	}
	
	protected AbstractJob createSimpleJob(String name, Steps steps, JobExecutionDao executionDao, JobExecutionListener listener, StepExecutionListener steplistener) {
		FlowJob job = new FlowJob();
		job.setName(name);
		job.setSteps(steps);
		job.setExecutionDao(executionDao);
		job.setJobExecutionListener(listener);
		job.setStepExecutionListener(steplistener);
		return job;
	}
	
	protected JobExecutionDao createMockJobExecutionDao(JobExecution execution) {
		JobExecutionDao dao = mock(JobExecutionDao.class);
		when(dao.getJobExecution(any(Job.class))).thenReturn(execution);
		JobExecution savedExecution = new JobExecution();
		savedExecution.setWorkflow(new Workflow(new Steps(), new Steps()));
		when(dao.saveJobExecution(any(Job.class))).thenReturn(savedExecution);
		return dao;
	}
}
