package com.nexr.framework.workflow;

import static org.hamcrest.CoreMatchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * @author dani.kim@nexr.com
 */
public class TestSimpleWorkflow extends JobTestBase {
	private JobLauncherImpl launcher;
	private JobExecutionDao executionDao;
	private static ThreadLocal<AtomicInteger> counter;

	@Before
	public void init() {
		launcher = new JobLauncherImpl();
		executionDao = mock(JobExecutionDao.class);
		final JobExecution execution = new JobExecution();
		Workflow workflow = new Workflow(new Steps(), new Steps());
		execution.setWorkflow(workflow);
		when(executionDao.saveJobExecution(any(Job.class))).thenReturn(
				execution);
		doAnswer(new Answer<JobExecution>() {
			@Override
			public JobExecution answer(InvocationOnMock invocation)
					throws Throwable {
				JobExecution execution = (JobExecution) invocation
						.getArguments()[0];
				return execution;
			}
		}).when(executionDao).updateJobExecution(any(JobExecution.class));

		launcher.executionDao = executionDao;
		counter = new ThreadLocal<AtomicInteger>();
		counter.set(new AtomicInteger());
	}

	@Test
	public void testRecoveryMode() throws Exception {
		Steps steps = new Steps().add(new Step("step1", Tasklet1.class))
				.add(new Step("step2", Tasklet2.class))
				.add(new Step("step3", Tasklet2.class))
				.add(new Step("step4", Tasklet2.class));
		Job job = createSimpleJob("name", steps, executionDao);

		JobExecution execution = new JobExecution(job);
		Steps footprints = new Steps().add(new Step("step1", Tasklet1.class));
		execution.setWorkflow(new Workflow(steps, footprints));
		when(executionDao.getJobExecution(any(Job.class))).thenReturn(execution);

		launcher.run(job);

		Assert.assertThat(3, is(counter.get().get()));
	}

	@Test
	public void testStep1() throws Exception {
		Job job = createSimpleJob("test-step1", new Steps().add(new Step("s1", null, Tasklet1.class)), executionDao);
		JobExecution execution = launcher.run(job);
		Assert.assertThat(counter.get().get(), is(1));
		verify(executionDao).updateJobExecution(execution);
		Assert.assertThat(execution.getStatus(), is(JobStatus.COMPLETED));
	}

	@Test
	public void testStep2() throws Exception {
		Job job = createSimpleJob("test-step2", new Steps().add(new Step("s1", Tasklet1.class)).add(new Step("s2", Tasklet2.class)), executionDao);
		JobExecution execution = launcher.run(job);
		Assert.assertThat(counter.get().get(), is(2));
		verify(executionDao).updateJobExecution(execution);
		Assert.assertThat(execution.getStatus(), is(JobStatus.COMPLETED));
	}

	@Test
	public void testThrowException() throws Exception {
		Job job = createSimpleJob("test-error", new Steps().add(new Step("s1", ExceptionTasklet.class)), executionDao);
		JobExecution execution = launcher.run(job);
		verify(executionDao).updateJobExecution(execution);
		Assert.assertThat(counter.get().get(), is(1));
		Assert.assertThat(execution.getStatus(), is(JobStatus.FAILED));
	}

	public static class Tasklet1 implements Tasklet {
		@Override
		public String run(StepContext context) {
			counter.get().incrementAndGet();
			return null;
		}
	}

	public static class Tasklet2 implements Tasklet {
		@Override
		public String run(StepContext context) {
			counter.get().incrementAndGet();
			return null;
		}
	}

	public static class ExceptionTasklet implements Tasklet {
		@Override
		public String run(StepContext context) {
			counter.get().incrementAndGet();
			throw new RuntimeException();
		}
	}
}
