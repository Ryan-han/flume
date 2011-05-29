package com.nexr.framework.workflow;

/**
 * @author dani.kim@nexr.com
 */
public class JobExecution {
	private Job job;
	private String key;
	private JobStatus status;
	private StepContext context;
	private Workflow workflow;
	private boolean recoveryMode;
	private volatile int retryCount = 0;
	
	public JobExecution() {
	}
	
	public JobExecution(Job job) {
		this.job = job;
	}
	
	public boolean isRecoveryMode() {
		return recoveryMode;
	}
	
	public void setRecoveryMode(boolean recoveryMode) {
		this.recoveryMode = recoveryMode;
	}

	public synchronized JobStatus getStatus() {
		return status;
	}
	
	public Workflow getWorkflow() {
		return workflow;
	}
	
	public void setWorkflow(Workflow workflow) {
		this.workflow = workflow;
	}

	public synchronized void setStatus(JobStatus status) {
		this.status = status;
		notifyAll();
	}

	public StepContext getContext() {
		return context;
	}
	
	public void setContext(StepContext context) {
		this.context = context;
	}
	
	public Job getJob() {
		return job;
	}
	
	public void setJob(Job job) {
		this.job = job;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}
	
	public void incrementRetryCount() {
		retryCount++;
	}
	
	public int getRetryCount() {
		return retryCount;
	}
	
	public void setRetryCount(int retryCount) {
		this.retryCount = retryCount;
	}
}


