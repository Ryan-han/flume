package com.nexr.framework.workflow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author dani.kim@nexr.com
 */
public class SimpleJob extends AbstractJob {
	private Logger LOG = LoggerFactory.getLogger(getClass());
	
	public SimpleJob() {
		super();
	}
	
	public SimpleJob(String name, Steps steps) {
		super(name, steps);
	}
	
	public SimpleJob(String name, Steps steps, boolean recoverable) {
		super(name, steps, recoverable);
	}
	
	@Override
	protected void doExecute(JobExecution execution) throws JobExecutionException {
		StepContext context = new StepContext();
		context.setJobExecution(execution);
		context.setConfig(new StepContext.Config(getParameters()));

		Workflow workflow = execution.getWorkflow();
		Steps steps = getSteps();
		if (execution.isRecoveryMode()) {
			steps = Steps.different(workflow.getSteps(), workflow.getFootprints());
		}
		LOG.info("Job orders : {}", steps);
		for (Step step : steps) {
			workflow.addStep(step);
			try {
				if (steplistener != null) {
					steplistener.beforeStep(step, context);
				}
			} catch (Exception e) {
				LOG.warn("Exception encountered in before callback of step", e);
			}
			Tasklet tasklet;
			try {
				tasklet = step.getTasklet().newInstance();
				tasklet.run(context);
			} catch (Exception e) {
				throw new JobExecutionException(e);
			}
			try {
				if (steplistener != null) {
					steplistener.afterStep(step, context);
				}
			} catch (Exception e) {
				LOG.warn("Exception encountered in after callback of step", e);
				try {
					steplistener.caught(step, context, e);
				} catch (Exception e2) {
					LOG.warn("Exception encountered in caught callback of step", e);
				}
			}
		}
	}
}
