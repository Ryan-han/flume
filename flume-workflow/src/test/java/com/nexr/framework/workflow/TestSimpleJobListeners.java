package com.nexr.framework.workflow;

import static org.hamcrest.CoreMatchers.is;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.nexr.framework.workflow.listener.JobExecutionListener;
import com.nexr.framework.workflow.listener.StepExecutionListener;

/**
 * @author dani.kim@nexr.com
 */
public class TestSimpleJobListeners extends JobTestBase {
	private JobLauncher launcher;
	private JobExecutionListener joblistener;
	private StepExecutionListener steplistener;
	private static AtomicInteger counter;

	@Before
	public void init() throws Exception {
		counter = new AtomicInteger();
		launcher = createLauncher(createMockJobExecutionDao(null), null);
		steplistener = new StepExecutionListener() {
			@Override
			public void caught(Step step, StepContext context, Throwable cause) {
			}
			
			@Override
			public void beforeStep(Step step, StepContext context) {
				counter.incrementAndGet();
			}
			
			@Override
			public void afterStep(Step step, StepContext context) {
				counter.incrementAndGet();
			}
		};
		joblistener = new JobExecutionListener() {
			@Override
			public void beforeJob(Job job) {
				counter.incrementAndGet();
			}
			
			@Override
			public void afterJob(Job job) {
				counter.incrementAndGet();
			}
		};
		launcher = createLauncher(createMockJobExecutionDao(null), null);
	}
	
	@Test
	public void testJobExecutionListener() throws Exception {
		JobExecution execution = new JobExecution();
		Job job = createSimpleJob("job-listener1", new Steps().add(new Step("s1", Tasklet1.class)), createMockJobExecutionDao(execution), joblistener);
		execution.setJob(job);
		
		launcher.run(job);

		Assert.assertThat(counter.get(), is(2));
	}
	
	@Test
	public void testStepExecutionListener() throws Exception {
		JobExecution execution = new JobExecution();
		Job job = createSimpleJob("job-listener1", new Steps().add(new Step("s1", Tasklet1.class)).add(new Step("s2", Tasklet1.class)), createMockJobExecutionDao(execution), joblistener, steplistener);
		execution.setJob(job);
		
		launcher.run(job);
		
		Assert.assertThat(counter.get(), is(6));
	}
	
	public static class Tasklet1 implements Tasklet {
		@Override
		public String run(StepContext context) {
			return null;
		}
	}
}
