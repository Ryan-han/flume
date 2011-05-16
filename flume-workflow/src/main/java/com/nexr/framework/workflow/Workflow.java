package com.nexr.framework.workflow;

/**
 * @author dani.kim@nexr.com
 */
public class Workflow {
	private Steps steps;
	private Steps footprints;
	
	public Workflow(Steps steps, Steps footprints) {
		this.steps = steps;
		this.footprints = footprints;
	}
	
	public Workflow(Job job) {
		steps = new Steps(job.getSteps());
		footprints = new Steps();
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();

		sb.append("workflow[");
		synchronized (footprints) {
			boolean first = true;
			for (Step step : footprints) {
				sb.append(first ? "" : " > ");
				sb.append(step.getName());
				first = false;
			}
		}
		sb.append("]");
		
		return sb.toString();
	}
	
	public void addStep(Step step) {
		footprints.add(step);
	}
	
	public synchronized Steps getSteps() {
		return new Steps(steps);
	}
	
	public synchronized Steps getFootprints() {
		return new Steps(footprints);
	}
}
