package com.nexr.framework.workflow;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.nexr.framework.workflow.listener.StepContextEventListener;

/**
 * @author dani.kim@nexr.com
 */
public class StepContext {
	private JobExecution jobExecution;
	private Config config;
	private Map<String, String> context;
	private StepContextEventListener contextEventListener;
	
	public StepContext() {
		context = new HashMap<String, String>();
	}
	
	public JobExecution getJobExecution() {
		return jobExecution;
	}
	
	public void setJobExecution(JobExecution jobExecution) {
		this.jobExecution = jobExecution;
	}
	
	public void commit() {
		if (contextEventListener != null) {
			contextEventListener.commit(this);
		}
	}
	
	public Collection<String> keys() {
		return context.keySet();
	}
	
	public String get(String name, String defaultValue) {
		return context.get(name) == null ? defaultValue : context.get(name);
	}
	
	public int getInt(String name, int defaultValue) {
		try {
			return Integer.parseInt(get(name, null));
		} catch (Exception e) {
			return defaultValue;
		}
	}
	
	public void set(String name, String value) {
		context.put(name, value);
	}

	public void remove(String name) {
		context.remove(name);
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

		public boolean getBoolean(String name, boolean defaultValue) {
			try {
				return Boolean.parseBoolean(get(name, null));
			} catch (Exception e) {
				return defaultValue;
			}
		}

		public int getInt(String name, int defaultValue) {
			try {
				return Integer.parseInt(get(name, null));
			} catch (Exception e) {
				return defaultValue;
			}
		}

		public long getLong(String name, long defaultValue) {
			try {
				return Long.parseLong(get(name, null));
			} catch (Exception e) {
				return defaultValue;
			}
		}

		public float getFloat(String name, float defaultValue) {
			try {
				return Float.parseFloat(get(name, null));
			} catch (Exception e) {
				return defaultValue;
			}
		}

		public double getDouble(String name, double defaultValue) {
			try {
				return Double.parseDouble(get(name, null));
			} catch (Exception e) {
				return defaultValue;
			}
		}

		public Collection<String> keys() {
			return parameters.keySet();
		}
	}
	
	public void setContextEventListener(StepContextEventListener listener) {
		this.contextEventListener = listener;
	}
}
