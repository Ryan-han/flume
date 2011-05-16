package com.nexr.framework.workflow;

import java.util.List;

public interface JobExecutionDao {
	JobExecution getJobExecution(Job job);
	
	JobExecution saveJobExecution(Job job);

	JobExecution updateJobExecution(JobExecution execution);
	
	JobExecution completeJob(JobExecution execution);

	StepExecution updateStepExecution(JobExecution execution, Step step);

	List<JobExecution> findFailExecutions();
	
	JobExecution findLastFailExecution();
	
	List<JobExecution> clearFailExecutions();
}
