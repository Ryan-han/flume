package com.nexr.framework.workflow;

import org.springframework.context.ApplicationContext;

/**
 * @author dani.kim@nexr.com
 */
public class SpringJobFactory implements JobFactory {
	private ApplicationContext ctx;
	
	SpringJobFactory(ApplicationContext ctx) {
		this.ctx = ctx;
	}

	@Override
	public <T extends Job> T createJob(Class<T> jobClass) {
		return ctx.getBean(jobClass);
	}
}
