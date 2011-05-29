package com.nexr.framework.workflow;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
	
	// 변경해야함. 나중에 async, sync 모두 가능하도록 수정해야함. 지금은 3개의 thread 로 동기로 수행하도록 함.
	private ExecutorService executor = Executors.newFixedThreadPool(3);
	
	JobExecutionDao executionDao = new InMemoryJobExecutionDao();
	JobLauncherListener listener;
	
	@Override
	public JobExecution run(final Job job) throws JobExecutionException {
		Assert.notNull(job);
		Assert.notNull(job.getSteps());
		
		JobExecution executed = executionDao.getJobExecution(job);
		if (executed == null) {
			executed = executionDao.saveJobExecution(job);
		} else {
			if (executed.getStatus() == JobStatus.COMPLETED) {
				throw new IllegalJobStatusException("Job was already completed");
			}
			LOG.info("Recovery job {}", job);
			executed.setRecoveryMode(true);
		}
		final JobExecution execution = executed;
		
		executor.execute(new Runnable() {
			@Override
			public void run() {
				LOG.info("Started job {}", job);
				execution.setStatus(JobStatus.STARTING);
				executionDao.updateJobExecution(execution);
				try {
					if (job instanceof AbstractJob) {
						((AbstractJob) job).setExecutionDao(executionDao);
					}
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
			}
		});
		return execution;
	}
	
	public void setExecutionDao(JobExecutionDao executionDao) {
		this.executionDao = executionDao;
	}
}
