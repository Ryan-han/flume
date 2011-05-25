package com.nexr.framework.workflow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlowJob extends AbstractJob {
	private Logger LOG = LoggerFactory.getLogger(getClass());
	
	public FlowJob() {
		super();
	}
	
	public FlowJob(String name, Steps steps) {
		super(name, steps);
	}
	
	public FlowJob(String name, Steps steps, boolean recoverable) {
		super(name, steps, recoverable);
	}
	
	@Override
	protected void doExecute(JobExecution execution) throws JobExecutionException {
		StepContext context = createContext(execution);
	
		Workflow workflow = execution.getWorkflow();
		Steps steps = getSteps();
		
		Step step = null;
		if (execution.isRecoveryMode()) {
			step = workflow.getFootprints().pop();
		} else {
			step = steps.first();
		}
		while (step != null) {
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
				String next = tasklet.run(context);
				if (next == null) {
					break;
				}
				step = steps.get(next);
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

