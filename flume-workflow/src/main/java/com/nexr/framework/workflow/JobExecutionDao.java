package com.nexr.framework.workflow;

public interface JobExecutionDao {
	JobExecution getJobExecution(Job job);
	
	JobExecution saveJobExecution(Job job);

	JobExecution updateJobExecution(JobExecution execution);

	StepExecution updateStepExecution(JobExecution execution, Step step);
}
