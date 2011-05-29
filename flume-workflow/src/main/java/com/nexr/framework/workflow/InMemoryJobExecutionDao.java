package com.nexr.framework.workflow;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test 용으로 사용되고 Memory 로 관리됩니다.
 * @author dani.kim@nexr.com
 */
public class InMemoryJobExecutionDao implements JobExecutionDao {
	private Map<Job, JobExecution> executionMap;
	
	public InMemoryJobExecutionDao() {
		executionMap = new HashMap<Job, JobExecution>();
	}
	
	@Override
	public JobExecution getJobExecution(Job job) {
		return executionMap.get(job);
	}

	@Override
	public JobExecution saveJobExecution(Job job) {
		JobExecution execution = new JobExecution(job);
		execution.setWorkflow(new Workflow(job));
		executionMap.put(job, execution);
		return execution;
	}

	@Override
	public JobExecution updateJobExecution(JobExecution execution) {
		executionMap.put(execution.getJob(), execution);
		return execution;
	}

	@Override
	public JobExecution completeJob(JobExecution execution) {
		execution.setStatus(JobStatus.COMPLETED);
		return updateJobExecution(execution);
	}
	
	@Override
	public JobExecution failJob(JobExecution execution) {
		return null;
	}

	@Override
	public StepExecution updateStepExecution(JobExecution execution, Step step) {
		updateJobExecution(execution);
		StepExecution stepExecution = new StepExecution();
		return stepExecution;
	}
	
	@Override
	public JobExecution findJobExecutionById(String jobId) {
		return null;
	}

	@Override
	public List<JobExecution> findFailExecutions() {
		return Collections.unmodifiableList(new ArrayList<JobExecution>());
	}

	@Override
	public JobExecution findLastFailExecution() {
		return null;
	}

	@Override
	public List<JobExecution> clearFailExecutions() {
		return Collections.unmodifiableList(new ArrayList<JobExecution>());
	}
}