package com.nexr.framework.workflow;

import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * @author dani.kim@nexr.com
 */
public class SpringWorkflowManager implements WorkflowManager {
	private static ClassPathXmlApplicationContext ctx;
	private static JobFactory jobFactory;
	
	static {
		ctx = new ClassPathXmlApplicationContext("classpath:workflow-app.xml");
		jobFactory = new SpringJobFactory(ctx);
	}
	
	@Override
	public JobLauncher createLauncher() {
		return ctx.getBean(JobLauncher.class);
	}
	
	@Override
	public JobFactory getJobFactory() {
		return jobFactory;
	}
}
