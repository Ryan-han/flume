package com.nexr.framework.workflow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.nexr.framework.workflow.listener.JobLauncherListener;

/**
 * 간단하게 구현한 {@link JobLauncher}
 * 
 * @author dani.kim@nexr.com
 */
public class JobLauncherImpl implements JobLauncher {
	private Logger LOG = LoggerFactory.getLogger(getClass());
	
	JobExecutionDao executionDao;
	JobLauncherListener listener;
	
	@Override
	public JobExecution run(Job job) throws JobExecutionException {
		Assert.notNull(job);
		Assert.notNull(job.getSteps());
		
		LOG.info("Started job {}", job);
		JobExecution execution = executionDao.getJobExecution(job);
		if (execution == null) {
			execution = executionDao.saveJobExecution(job);
		} else {
			if (execution.getStatus() == JobStatus.COMPLETED) {
				throw new IllegalJobStatusException("Job was already completed");
			}
			LOG.info("Recovery job {}", job);
			execution.setRecoveryMode(true);
		}
		try {
			job.execute(execution);
		} catch (Exception e) {
			LOG.info("Exception encountered in job", e);
			execution.setStatus(JobStatus.FAILED);
			executionDao.updateJobExecution(execution);
			if (listener != null) {
				listener.failure(execution);
			}
			
		}
		LOG.info("Ended job {}, {}", job, execution.getWorkflow());
		return execution;
	}
	
	public void setExecutionDao(JobExecutionDao executionDao) {
		this.executionDao = executionDao;
	}
}
