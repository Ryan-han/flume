package com.nexr.framework.workflow;

/**
 * @author dani.kim@nexr.com
 */
public interface WorkflowManager {
	JobLauncher createLauncher();
	
	JobFactory getJobFactory();
}
