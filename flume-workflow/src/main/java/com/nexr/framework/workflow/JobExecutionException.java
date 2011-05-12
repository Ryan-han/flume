package com.nexr.framework.workflow;

/**
 * @author dani.kim@nexr.com
 */
public class JobExecutionException extends Exception {
	private static final long serialVersionUID = 2580598458798401898L;

	public JobExecutionException() {
		super();
	}

	public JobExecutionException(String message, Throwable cause) {
		super(message, cause);
	}

	public JobExecutionException(String message) {
		super(message);
	}

	public JobExecutionException(Throwable cause) {
		super(cause);
	}
}
