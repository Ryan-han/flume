package com.nexr.framework.workflow;

import java.util.Map;

/**
 * @author dani.kim@nexr.com
 */
public interface Job {
	String getName();
	
	void setName(String name);
	
	boolean isRecoverable();
	
	void setRecoverable(boolean recoverable);
	
	Steps getSteps();
	
	void setSteps(Steps steps);
	
	Map<String, String> getParameters();
	
	void execute(JobExecution execution) throws JobExecutionException;

	void addParameter(String name, String value);
}
