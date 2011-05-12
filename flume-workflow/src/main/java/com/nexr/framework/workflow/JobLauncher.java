package com.nexr.framework.workflow;

/**
 * 작업 실행 추상 인터페이스
 * 
 * @author dani.kim@nexr.com
 */
public interface JobLauncher {
	JobExecution run(Job job) throws JobExecutionException;
}
