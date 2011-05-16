package com.nexr.framework.workflow;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author dani.kim@nexr.com
 */
public class TestFlowWorkflow {
	private static BlockingQueue<String> path = new LinkedBlockingQueue<String>();
	private FlowJob job;
	private Steps steps;
	private JobLauncher launcher;
	
	@Before
	public void init() throws Exception {
		launcher = new JobLauncherImpl();
		steps = new Steps();
		steps.add(new Step("step1", Tasklet1.class));
		steps.add(new Step("step2", Tasklet2.class));
		steps.add(new Step("step3", Tasklet3.class));
		steps.add(new Step("step4", Tasklet4.class));
		steps.add(new Step("step5", Tasklet5.class));
		steps.add(new Step("step6", Tasklet6.class));
	}
	
	@Test
	public void test() throws Exception {
		job = new FlowJob("flow-job", steps);
		job.addParameter("step4", "step4");
		JobExecution execution = launcher.run(job);
		Assert.assertThat(execution.getWorkflow().toString(), CoreMatchers.is("workflow[step1 > step2 > step3 > step4]"));

		job = new FlowJob("flow-job", steps);
		job.addParameter("step4", "step5");
		execution = launcher.run(job);
		Assert.assertThat(execution.getWorkflow().toString(), CoreMatchers.is("workflow[step1 > step2 > step3 > step5]"));

		job = new FlowJob("flow-job", steps);
		job.addParameter("step3", "step5");
		job.addParameter("step4", "step5");
		execution = launcher.run(job);
		Assert.assertThat(execution.getWorkflow().toString(), CoreMatchers.is("workflow[step1 > step2 > step5]"));
	}
	
	static class Tasklet1 implements Tasklet {
		@Override
		public String run(StepContext context) {
			path.add("step1");
			return "step2";
		}
	}
	
	static class Tasklet2 implements Tasklet {
		@Override
		public String run(StepContext context) {
			path.add("step2");
			return context.getConfig().get("step3", "step3");
		}
	}
	
	static class Tasklet3 implements Tasklet {
		@Override
		public String run(StepContext context) {
			path.add("step3");
			return context.getConfig().get("step4", "step4");
		}
	}
	
	static class Tasklet4 implements Tasklet {
		@Override
		public String run(StepContext context) {
			path.add("step4");
			return null;
		}
	}
	
	static class Tasklet5 implements Tasklet {
		@Override
		public String run(StepContext context) {
			path.add("step5");
			return null;
		}
	}
	
	static class Tasklet6 implements Tasklet {
		@Override
		public String run(StepContext context) {
			path.add("step6");
			return null;
		}
	}
}
