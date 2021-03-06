package com.nexr.rolling.workflow;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.retry.RetryCallback;
import org.springframework.batch.retry.RetryContext;
import org.springframework.batch.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.batch.retry.policy.SimpleRetryPolicy;
import org.springframework.batch.retry.support.RetryTemplate;

import com.nexr.framework.workflow.Configuration;
import com.nexr.framework.workflow.Job;
import com.nexr.framework.workflow.JobExecution;
import com.nexr.framework.workflow.JobExecutionDao;
import com.nexr.framework.workflow.JobStatus;
import com.nexr.framework.workflow.Step;
import com.nexr.framework.workflow.StepContext;
import com.nexr.framework.workflow.StepContext.Config;
import com.nexr.framework.workflow.StepExecution;
import com.nexr.framework.workflow.Steps;
import com.nexr.framework.workflow.Tasklet;
import com.nexr.framework.workflow.Workflow;

/**
 * @author dani.kim@nexr.com
 */
public class ZKJobExecutionDao implements JobExecutionDao {
	private Configuration config = Configuration.getInstance();
	private final String COMPLETE = config.getZkWorkflowComplete();
	private final String ABANDONE = config.getZkWorkflowAbandone();
	private final String RUNNING = config.getZkWorkflowRunning();
	private final String OUTAGE = config.getZkWorkflowOutage();

	private Logger LOG = LoggerFactory.getLogger("zk.log");
	
	private ZkClient client = ZkClientFactory.getClient();
	private RetryTemplate retryTemplate;
	
	public ZKJobExecutionDao() {
		retryTemplate = new RetryTemplate();
		retryTemplate.setBackOffPolicy(new ExponentialBackOffPolicy());
		Map<Class<? extends Throwable>, Boolean> retryableExceptions = new HashMap<Class<? extends Throwable>, Boolean>();
		retryableExceptions.put(Exception.class, true);
		retryableExceptions.put(ZkNodeExistsException.class, false);
		retryTemplate.setRetryPolicy(new SimpleRetryPolicy(10, retryableExceptions));
	}
	
	@Override
	public JobExecution findJobExecutionById(String jobId) {
		if (jobId != null) {
			try {
				Object data = client.readData(String.format("%s/%s", RUNNING, jobId));
				JobExecution execution = readJobExecution(data.toString());
				return execution;
			} catch (Exception e) {
			}
		}
		return null;
	}
	
	@Override
	public JobExecution findLastFailExecution() {
		List<JobExecution> executions = findFailExecutions();
		if (executions.size() > 0) {
			return executions.get(executions.size() - 1);
		}
		return null;
	}
	
	@Override
	public List<JobExecution> findFailExecutions() {
		List<JobExecution> executions = new ArrayList<JobExecution>();
		List<String> children = client.getChildren(RUNNING);
		for (String child : children) {
			Object data = client.readData(String.format("%s/%s", RUNNING, child));
			try {
				if (data != null) {
					JobExecution execution = readJobExecution(data.toString());
					if (execution.getStatus() == JobStatus.COMPLETED) {
					} else if (execution.getStatus() == JobStatus.FAILED) {
						executions.add(execution);
					} else if (!client.exists(String.format("%s/%s", OUTAGE, execution.getKey()))) {
						executions.add(execution);
					}
				}
			} catch (Exception e) {
				LOG.warn("Invalid format : " + data, e);
				client.delete(String.format("%s/%s", RUNNING, child));
			}
		}
		return executions;
	}
	
	@Override
	public JobExecution saveJobExecution(Job job) {
		String key = createJobKey(job.getName(), job.getParameters());
		StepContext context = new StepContext();
		context.setConfig(new Config(job.getParameters()));
		
		final JobExecution execution = new JobExecution(job);
		context.setJobExecution(execution);
		execution.setKey(key);
		execution.setStatus(JobStatus.PENDING);
		execution.setContext(context);
		execution.setWorkflow(new Workflow(job));
		try {
			retryTemplate.execute(new RetryCallback<String>() {
				@Override
				public String doWithRetry(RetryContext context) throws Exception {
					String node = createZNodePath(execution);
					if (!client.exists(node)) {
						client.createPersistent(node);
					}
					client.writeData(node, createJobExecution(execution));
					return null;
				}
			});
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return execution;
	}
	
	private String createJobExecution(JobExecution execution) {
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode root = new ObjectNode(mapper.getNodeFactory());
		root.put("key", execution.getKey());
		root.put("name", execution.getJob().getName());
		root.put("status", execution.getStatus().name());
		root.put("timestamp", System.currentTimeMillis());
		root.put("retryCount", execution.getRetryCount());
		root.put("version", 1);
		ObjectNode parameters = root.putObject("parameters");
		for (String key : execution.getContext().getConfig().keys()) {
			parameters.put(key, execution.getContext().getConfig().get(key, null));
		}
		ObjectNode context = root.putObject("context");
		for (String key : execution.getContext().keys()) {
			context.put(key, execution.getContext().get(key, null));
		}
		ObjectNode node = root.putObject("workflow");
		Workflow workflow = execution.getWorkflow();
		node.put("steps", writeSteps(workflow.getSteps()));
		node.put("footprints", writeSteps(workflow.getFootprints()));
		if (LOG.isDebugEnabled()) {
			LOG.debug("text: {}", root.toString());
		}
		return root.toString();
	}

	@Override
	public JobExecution updateJobExecution(final JobExecution execution) {
		try {
			retryTemplate.execute(new RetryCallback<String>() {
				@Override
				public String doWithRetry(RetryContext context) throws Exception {
					if (execution.getStatus() != null && execution.getStatus() == JobStatus.STARTING) {
						String outageNode = String.format("%s/%s", OUTAGE, execution.getKey());
						if (!client.exists(outageNode)) {
							client.createEphemeral(outageNode);
						}
					}
					if (!client.exists(createZNodePath(execution))) {
						client.createPersistent(createZNodePath(execution), true);
					}
					client.writeData(createZNodePath(execution), createJobExecution(execution));
					return null;
				}
			});
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return execution;
	}
	
	@Override
	public JobExecution completeJob(JobExecution execution) {
		execution.setStatus(JobStatus.COMPLETED);
		updateJobExecution(execution);
		removeJob(COMPLETE, execution.getJob());
		return execution;
	}
	
	@Override
	public JobExecution failJob(JobExecution execution) {
		execution.setStatus(JobStatus.ABANDONED);
		updateJobExecution(execution);
		removeJob(COMPLETE, execution.getJob());
		return execution;
	}
	
	@Override
	public List<JobExecution> clearFailExecutions() {
		List<JobExecution> executions = findFailExecutions();
		for (JobExecution execution : executions) {
			removeJob(ABANDONE, execution.getJob());
		}
		return executions;
	}
	
	@Override
	public StepExecution updateStepExecution(final JobExecution execution, final Step step) {
		try {
			return retryTemplate.execute(new RetryCallback<StepExecution>() {
				@Override
				public StepExecution doWithRetry(RetryContext context) throws Exception {
					updateJobExecution(execution);
					return new StepExecution();
				}
			});
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public JobExecution getJobExecution(Job job) {
		String key = createJobKey(job.getName(), job.getParameters());
		Object data = client.readData(createZNodePath(key), true);
		return readJobExecution(data == null ? null : data.toString());
	}
	
	private JobExecution readJobExecution(String data) {
		if (data == null) {
			return null;
		}
		ObjectMapper mapper = new ObjectMapper();
		try {
			final ObjectNode root = (ObjectNode) mapper.readTree(data);

			ObjectNode paramNodes = (ObjectNode) root.path("parameters");
			String jobClassName = paramNodes.path("job.class").getTextValue();
			
			Job job = (Job) Class.forName(jobClassName).newInstance();
			job.setName(root.path("name").getValueAsText());
			job.setSteps(readSteps(root.path("workflow").path("steps")));
			job.setRecoverable(root.path("recovery").getBooleanValue());
			
			Iterator<String> names = paramNodes.getFieldNames();
			for (String name = null; names.hasNext(); ) {
				name = names.next();
				job.addParameter(name, paramNodes.path(name).getTextValue());
			}
			
			JobExecution execution = new JobExecution();
			execution.setKey(root.path("key").getTextValue());
			execution.setJob(job);
			execution.setStatus(JobStatus.valueOf(root.path("status").getTextValue()));
			execution.setWorkflow(new Workflow(job.getSteps(), readSteps(root.path("workflow").path("footprints"))));
			execution.setRetryCount(root.path("retryCount").getIntValue());

			StepContext context = new StepContext();
			context.setJobExecution(execution);
			context.setConfig(new Config(job.getParameters()));
			
			ObjectNode contextNode = (ObjectNode) root.path("context");
			names = contextNode.getFieldNames();
			for (String name = null; names.hasNext(); ) {
				name = names.next();
				context.set(name, contextNode.path(name).getTextValue());
			}
			execution.setContext(context);
			return execution;
		} catch (Exception e) {
			LOG.warn("Exception encountered in readJobExecution", e);
		}
		return null;
	}
	
	@SuppressWarnings("unused")
	private Steps readFootprints(String key) throws IOException {
		Steps steps = new Steps();
		String path = String.format("%s/%s", RUNNING, key);
		for (String child : client.getChildren(path)) {
			String json = client.readData(String.format("%s/%s", path, child)).toString();
			steps.add(readStep(new ObjectMapper().readTree(json)));
		}
		return steps;
	}
	
	private Steps readSteps(JsonNode node) {
		Steps steps = new Steps();
		for (JsonNode n : (ArrayNode) node) {
			steps.add(readStep(n));
		}
		return steps;
	}

	@SuppressWarnings("unchecked")
	private Step readStep(JsonNode node) {
		if (node.isArray()) {
			ArrayNode pair = (ArrayNode) node;
			if (pair.size() != 2) {
				throw new IllegalStateException("Name and tasklet is required.");
			}
			String name = pair.get(0).getTextValue();
			String tasklet = pair.get(1).getTextValue();
			try {
				return new Step(name, (Class<? extends Tasklet>) Class.forName(tasklet));
			} catch (Exception e) {
				throw new IllegalStateException("Tasklet is illegal state.", e);
			}
		} else if (node.isObject()) {
			ObjectNode object = (ObjectNode) node;
			String name = object.path("name").getTextValue();
			String tasklet = object.path("tasklet").getTextValue();
			try {
				return new Step(name, tasklet == null ? null : (Class<? extends Tasklet>) Class.forName(tasklet));
			} catch (Exception e) {
				throw new IllegalStateException("Tasklet is illegal state.", e);
			}
		} else if (node.isTextual()) {
			return new Step(node.getTextValue(), null);
		}
		throw new IllegalStateException("Unsupported json format. \"" + node.toString() + "\"");
	}
	
	private JsonNode writeSteps(Steps steps) {
		ArrayNode nodes = new ArrayNode(new ObjectMapper().getNodeFactory());
		for (Step step : steps) {
			nodes.add(writeStep(step));
		}
		return nodes;
	}
	
	private JsonNode writeStep(Step step) {
		ObjectNode node = new ObjectNode(new ObjectMapper().getNodeFactory());
		node.put("timestamp", System.currentTimeMillis());
		node.put("name", step.getName());
		if (step.getTasklet() != null) {
			node.put("tasklet", step.getTasklet().getName());
		}
		return node;
	}

	public void removeJob(final String path, Job job) {
		final String key = createJobKey(job.getName(), job.getParameters());
		try {
			String backup = retryTemplate.execute(new RetryCallback<String>() {
				@Override
				public String doWithRetry(RetryContext context) throws Exception {
					if (!client.exists(createZNodePath(key))) {
						return null;
					}
					Object data = client.readData(createZNodePath(key));
					String backup = String.format("%s/%s", path, key);
					if (!client.exists(backup)) {
						client.delete(backup);
					}
					client.createPersistent(backup, data);
					client.delete(createZNodePath(key));
					client.delete(String.format("%s/%s", OUTAGE, key));
					return backup;
				}
			});
			LOG.info("Remove job {}, move to {}", key, backup);
		} catch (Exception e) {
			LOG.warn("Exception encountered in removeJob", e);
		}
	}
	
	protected String createZNodePath(String key) {
		return String.format("%s/%s", RUNNING, key);
	}
	
	protected String createZNodePath(JobExecution execution) {
		return String.format("%s/%s", RUNNING, execution.getKey());
	}

	protected String createJobKey(String name, Map<String, String> props) {
		StringBuffer sb = new StringBuffer();
		List<String> keys = new ArrayList<String>(props.keySet());
		Collections.sort(keys);
		sb.append(name).append(";");
		for (String key : keys) {
			String value = props.get(key);
			sb.append(key).append("=").append(value).append(";");
		}
		MessageDigest digest;
		try {
			digest = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalStateException(
					"MD5 algorithm not available.  Fatal (should be in the JDK).");
		}
		try {
			byte[] bytes = digest.digest(sb.toString().getBytes(
					"UTF-8"));
			return String.format("%032x", new BigInteger(1, bytes));
		} catch (UnsupportedEncodingException e) {
			throw new IllegalStateException(
					"UTF-8 encoding not available.  Fatal (should be in the JDK).");
		}
	}
}
