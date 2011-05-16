package com.nexr.rolling.workflow;

import com.nexr.framework.workflow.Job;
import com.nexr.framework.workflow.JobExecutionDao;
import com.nexr.framework.workflow.listener.JobExecutionListener;

/**
 * @author dani.kim@nexr.com
 */
public class JobExecutionListenerImpl implements JobExecutionListener {
	@SuppressWarnings("unused")
	private JobExecutionDao executionDao;
	
	@Override
	public void beforeJob(Job job) {
	}

	@Override
	public void afterJob(Job job) {
	}
	
	public void setExecutionDao(JobExecutionDao executionDao) {
		this.executionDao = executionDao;
	}
}
