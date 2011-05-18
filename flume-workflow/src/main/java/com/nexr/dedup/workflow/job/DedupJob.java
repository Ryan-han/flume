package com.nexr.dedup.workflow.job;

import com.nexr.framework.workflow.FlowJob;
import com.nexr.framework.workflow.Step;
import com.nexr.framework.workflow.Steps;
import com.nexr.rolling.workflow.job.Duplication;

/**
 * @author dani.kim@nexr.com
 */
public class DedupJob extends FlowJob {
	private static final Steps steps;
	
	static {
		steps = new Steps();
		steps.add(new Step("init", InitTasklet.class));
		steps.add(new Step("prepare", PrepareTasklet.class));
		steps.add(new Step("run", RunDedupMRTasklet.class));
		steps.add(new Step("finishing", FinishingTasklet.class));
		steps.add(new Step("cleanUp", CleanUpTasklet.class));
	}
	
	public DedupJob() {
		super("dedup", steps, true);
	}
	
	public DedupJob(Duplication duplication) {
		super("dedup", steps, true);
		addParameter("path", duplication.getPath());
		addParameter("source", duplication.getNewSource());
		addParameter("destination", duplication.getSource());
	}
}
