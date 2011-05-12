package com.nexr.rolling.workflow;

import com.nexr.framework.workflow.JobExecution;
import com.nexr.framework.workflow.JobExecutionDao;
import com.nexr.framework.workflow.Step;
import com.nexr.framework.workflow.StepContext;
import com.nexr.framework.workflow.listener.StepExecutionListener;

/**
 * @author dani.kim@nexr.com
 */
public class StepExecutionListenerImpl implements StepExecutionListener {
	private JobExecutionDao executionDao;
	
	public StepExecutionListenerImpl() {
	}
	
	public StepExecutionListenerImpl(JobExecutionDao executionDao) {
		this.executionDao = executionDao;
	}
	
	@Override
	public void beforeStep(Step step, StepContext context) {
		JobExecution execution = context.getJobExecution();
		executionDao.updateStepExecution(execution, step);
	}

	@Override
	public void afterStep(Step step, StepContext context) {
	}

	@Override
	public void caught(Step step, StepContext context, Throwable cause) {
	}
	
	public void setExecutionDao(JobExecutionDao executionDao) {
		this.executionDao = executionDao;
	}
}
