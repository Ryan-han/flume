package com.nexr.rolling.workflow;

import static org.hamcrest.CoreMatchers.is;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.nexr.framework.workflow.AbstractJob;
import com.nexr.framework.workflow.Job;
import com.nexr.framework.workflow.JobExecution;
import com.nexr.framework.workflow.JobStatus;
import com.nexr.framework.workflow.JobTestBase;
import com.nexr.framework.workflow.FlowJob;
import com.nexr.framework.workflow.Step;
import com.nexr.framework.workflow.Steps;
import com.nexr.framework.workflow.Workflow;

/**
 * @author dani.kim@nexr.com
 */
public class TestZKJobExecutionDao extends JobTestBase {
	private ZKJobExecutionDao dao;
	private Map<String, String> params;

	@Before
	public void init() {
		dao = new ZKJobExecutionDao();
		params = new HashMap<String, String>();
		params.put("name", "Daegeun Kim");
		params.put("age", "28");
		
		ZkClientFactory.getClient().deleteRecursive("/rolling/jobs");
	}
	
	@Test
	public void testClearFailExecutions() throws Exception {
		Steps steps = new Steps();
		AbstractJob job = createSimpleJob("test-zked", steps, dao);
		JobExecution execution = dao.saveJobExecution(job);
		execution.setStatus(JobStatus.FAILED);
		dao.updateJobExecution(execution);
		Assert.assertThat(dao.findFailExecutions().size(), is(1));
		dao.clearFailExecutions();
		Assert.assertThat(dao.findFailExecutions().size(), is(0));
	}
	
	@Test
	public void test() throws Exception {
		Steps steps = new Steps();
		steps.add(new Step("step1", null));
		steps.add(new Step("step2", null));
		steps.add(new Step("step3", null));
		Job job = new FlowJob("test", steps);
		
		for (Map.Entry<String, String> entry : params.entrySet()) {
			job.addParameter(entry.getKey(), entry.getValue());
		}
		
		Assert.assertNull(dao.getJobExecution(job));
		dao.saveJobExecution(job);
		JobExecution execution = dao.getJobExecution(job);
		Assert.assertThat(job.getName(), is(execution.getJob().getName()));
		for (Map.Entry<String, String> entry : job.getParameters().entrySet()) {
			Assert.assertThat(entry.getValue(), is(execution.getContext().getConfig().get(entry.getKey(), null)));
		}
		Workflow workflow = execution.getWorkflow();
		for (Step step : workflow.getSteps()) {
			System.out.println(step);
		}
		Assert.assertNotNull(dao.getJobExecution(job));
		dao.removeJob("complete", job);
		Assert.assertNull(dao.getJobExecution(job));
	}
}
