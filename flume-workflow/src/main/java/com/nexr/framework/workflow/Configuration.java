package com.nexr.framework.workflow;

/**
 * @author dani.kim@nexr.com
 */
public class Configuration extends com.nexr.framework.conf.Configuration {
	private static final Configuration instance = new Configuration();
	
	static {
		addDefaultResource("workflow-site.xml");
	}
	
	public static Configuration getInstance() {
		return instance;
	}
	
	public Configuration() {
	}
	
	public String getZkWorkflowBase() {
		return get("workflow.zk.path.base", "/workflow");
	}
	
	public String getZkWorkflowJob() {
		return String.format("%s/jobs", getZkWorkflowBase());
	}
	
	public String getZkWorkflowRunning() {
		return String.format("%s/running", getZkWorkflowBase());
	}
	
	public String getZkWorkflowRunning(String jobId) {
		return String.format("%s/running/%s", getZkWorkflowBase(), jobId);
	}
	
	public String getZkWorkflowComplete() {
		return String.format("%s/complete", getZkWorkflowBase());
	}
	
	public String getZkWorkflowAbandone() {
		return String.format("%s/abandone", getZkWorkflowBase());
	}
	
	public String getZkWorkflowComplete(String jobId) {
		return String.format("%s/complte/%s", getZkWorkflowBase(), jobId);
	}
	
	public String getZkWorkflowOutage() {
		return String.format("%s/outage", getZkWorkflowBase());
	}
	
	public String getZkWorkflowOutage(String jobId) {
		return String.format("%s/outage/%s", getZkWorkflowBase(), jobId);
	}
}
