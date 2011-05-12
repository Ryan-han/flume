package com.nexr.framework.workflow;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nexr.framework.workflow.listener.JobExecutionListener;
import com.nexr.framework.workflow.listener.StepExecutionListener;

/**
 * @author dani.kim@nexr.com
 */
public abstract class AbstractJob implements Job {
	private Logger LOG = LoggerFactory.getLogger(getClass());

	private String name;
	private Steps steps;
	private Map<String, String> parameters;
	private boolean recoverable;
	private JobExecutionDao executionDao;
	protected JobExecutionListener joblistener;
	protected StepExecutionListener steplistener;

	public AbstractJob() {
		this(null, new Steps());
	}

	public AbstractJob(String name, Steps steps) {
		this(name, steps, false);
	}

	public AbstractJob(String name, Steps steps, boolean recoverable) {
		this.steps = new Steps(steps);
		this.name = name;
		this.recoverable = recoverable;
		this.parameters = new HashMap<String, String>();
	}

	@Override
	public String toString() {
		return new StringBuilder().append("job[name: ").append(name)
				.append("]").toString();
	}

	@Override
	public void execute(JobExecution execution) throws JobExecutionException {
		try {
			if (joblistener != null) {
				joblistener.beforeJob(this);
			}
		} catch (Exception e) {
			LOG.warn("exception encountered in beforeJob callback", e);
		}
		doExecute(execution);
		try {
			if (joblistener != null) {
				joblistener.afterJob(this);
			}
		} catch (Exception e) {
			LOG.warn("exception encountered in afterJob callback", e);
		}
		execution.setStatus(JobStatus.COMPLETED);
		executionDao.updateJobExecution(execution);
	}
	
	protected abstract void doExecute(JobExecution execution) throws JobExecutionException;

	public void addParameter(String name, String value) {
		parameters.put(name, value);
	}

	public Map<String, String> getParameters() {
		return Collections.unmodifiableMap(parameters);
	}

	public void setParameters(Map<String, String> parameters) {
		this.parameters = parameters;
	}

	@Override
	public boolean isRecoverable() {
		return recoverable;
	}

	public void setRecoverable(boolean recoverable) {
		this.recoverable = recoverable;
	}

	@Override
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Steps getSteps() {
		return steps;
	}

	public void setSteps(List<Step> steps) {
		this.steps = new Steps(steps);
	}

	public void setSteps(Steps steps) {
		if (steps != null) {
			this.steps = new Steps(steps);
		}
	}

	public void setExecutionDao(JobExecutionDao executionDao) {
		this.executionDao = executionDao;
	}

	public void setJobExecutionListener(JobExecutionListener listener) {
		joblistener = listener;
	}
	
	public void setStepExecutionListener(StepExecutionListener listener) {
		steplistener = listener;
	}
}
