package com.nexr.framework.workflow;

import java.util.Map;

/**
 * @author dani.kim@nexr.com
 */
public interface Job {
	String getName();
	
	boolean isRecoverable();
	
	Steps getSteps();
	
	Map<String, String> getParameters();
	
	void execute(JobExecution execution) throws JobExecutionException;

	void addParameter(String string, String string2);
}
