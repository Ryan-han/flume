package com.nexr.rolling.workflow.job;

import com.nexr.framework.workflow.FlowJob;
import com.nexr.framework.workflow.Step;
import com.nexr.framework.workflow.Steps;

/**
 * @author dani.kim@nexr.com
 */
public class RollingJob extends FlowJob {
	private static Steps steps;
	static {
		steps = new Steps();
		steps.add(new Step("init", InitTasklet.class));
		steps.add(new Step("prepare", PrepareTasklet.class));
		steps.add(new Step("run", RunRollingMRTasklet.class));
		steps.add(new Step("finishing", FinishingTasklet.class));
		steps.add(new Step("duplicate", DuplicateTasklet.class));
		steps.add(new Step("cleanUp", CleanUpTasklet.class));
	}
	
	public RollingJob() {
		super("rolling", steps, true);
	}
}
