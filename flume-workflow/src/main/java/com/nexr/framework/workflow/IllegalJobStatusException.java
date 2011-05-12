package com.nexr.framework.workflow;

/**
 * 작업 상태가 올바르지 않을 때 사용합니다.
 * 
 * @author dani.kim@nexr.com
 */
public class IllegalJobStatusException extends JobExecutionException {
	private static final long serialVersionUID = -8238452501245192759L;

	public IllegalJobStatusException() {
		super();
	}

	public IllegalJobStatusException(String message, Throwable cause) {
		super(message, cause);
	}

	public IllegalJobStatusException(String message) {
		super(message);
	}

	public IllegalJobStatusException(Throwable cause) {
		super(cause);
	}
}
