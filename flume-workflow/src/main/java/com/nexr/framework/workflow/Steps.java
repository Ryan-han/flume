package com.nexr.framework.workflow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author dani.kim@nexr.com
 */
public class Steps implements Iterable<Step> {
	private List<Step> steps;
	private Map<String, Step> stepMap;
	
	/**
	 * steps1 과 steps2 의 차집합
	 * @param steps1
	 * @param steps2
	 * @return
	 */
	public static Steps different(Steps steps1, Steps steps2) {
		Steps diff = new Steps(steps1);
		for (Step step : steps2) {
			diff.remove(step);
		}
		return diff;
	}
	
	public Steps() {
		this(new ArrayList<Step>());
	}
	
	public Steps(List<Step> steps) {
		this.steps = new ArrayList<Step>();
		stepMap = new HashMap<String, Step>();
		if (steps != null) {
			for (Step step : steps) {
				this.steps.add(step);
				if (step.getName() == null) {
					step.setName(step.getTasklet().getName());
				}
				stepMap.put(step.getName(), step);
			}
		}
	}

	public Steps(Step[] steps) {
		this(Arrays.asList(steps));
	}

	public Steps(Steps steps) {
		this(steps.steps);
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		for (Step step : steps) {
			sb.append(first ? "" : " > ").append(step.getName());
			first = false;
		}
		return sb.toString();
	}
	
	public Steps add(Step step) {
		this.steps.add(step);
		return this;
	}
	
	public Steps remove(Step step) {
		this.steps.remove(step);
		return this;
	}
	
	public Step pop() {
		return steps.remove(steps.size() - 1);
	}
	
	@Override
	public Iterator<Step> iterator() {
		return steps.iterator();
	}
	
	public Step get(String name) {
		return stepMap.get(name);
	}

	public Step first() {
		return steps.get(0);
	}
}
