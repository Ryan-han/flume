package com.nexr.framework.workflow;

/**
 * @author dani.kim@nexr.com
 */
public interface JobFactory {
	<T extends Job> T createJob(Class<T> jobClass);
}
