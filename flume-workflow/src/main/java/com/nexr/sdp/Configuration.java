package com.nexr.sdp;

import java.util.Arrays;
import java.util.List;

/**
 * @author dani.kim@nexr.com
 */
public class Configuration extends com.nexr.framework.conf.Configuration {
	private static Configuration instance = new Configuration();
	
	static {
		addDefaultResource("sdp-default.xml");
		addDefaultResource("sdp-site.xml");
	}
	
	public static Configuration getInstance() {
		return instance;
	}
	
	private Configuration() {
	}
	
	public String getZkServerAsString() {
		return get("zk.servers");
	}
	
	public int getZkTimeout() {
		return getInt("zk.timeout", 10000);
	}
	
	public List<String> getZkServers() {
		return Arrays.asList(getStrings("zk.servers"));
	}

	public String getRollingDir() {
		return get("rolling.dir", "/rolling");
	}
	
	public String getDedupDir() {
		return get("dedup.dir", "/dedup");
	}
	
	public String getInputDir(String root, String type) {
		return String.format("%s/%s/%s", root, type, get("input.dir.name", "input"));
	}
	
	public String getOutputDir(String root, String type) {
		return String.format("%s/%s/%s", root, type, get("output.dir.name", "output"));
	}
	
	public String getResultDir(String root, String type) {
		return String.format("%s/%s/%s", root, type, get("result.dir.name", "result"));
	}
	
	public String getZkRollingBase() {
		return get("rolling.zk.path.base", "/rolling");
	}
	
	public String getZkDedupBase() {
		return get("dedup.zk.path.base", "/dedup");
	}
	
	public String getZkNotifyBase() {
		return get("notify.zk.path.base", "/collector");
	}
	
	public String getZkMemberNode(String name) {
		return String.format("%s/%s", getZkMembersBase(), name);
	}
	
	public String getZkMasterNode(String name) {
		return String.format("%s/%s", getZkMasterBase(), name);
	}
	
	public String getZkMembersBase() {
		return get("members.zk.path.base", "/membership/members");
	}
	
	public String getZkMasterBase() {
		return get("master.zk.path.base", "/membership/master");
	}
	
	public String getZkConfigBase() {
		return get("config.zk.path.base", "/config");
	}

	public String getCollectorSource() {
		return get("collector.source.path");
	}

	public String getScheduleExpression(String type) {
		return get(String.format("schedule.%s", type));
	}
}
