package com.nexr.framework.workflow;

import java.util.Collection;
import java.util.Map;

/**
 * @author dani.kim@nexr.com
 */
public class StepContext {
	private JobExecution jobExecution;
	private Config config;
	
	public JobExecution getJobExecution() {
		return jobExecution;
	}
	
	public void setJobExecution(JobExecution jobExecution) {
		this.jobExecution = jobExecution;
	}
	
	public Config getConfig() {
		return config;
	}
	
	public void setConfig(Config config) {
		this.config = config;
	}
	
	public static class Config {
		private Map<String, String> parameters;

		public Config(Map<String, String> parameters) {
			this.parameters = parameters;
		}
		
		public String get(String name, String defaultValue) {
			return parameters.get(name) == null ? defaultValue : parameters.get(name);
		}
		
		public Collection<String> keys() {
			return parameters.keySet();
		}
	}
}
