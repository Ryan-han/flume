package com.nexr.rolling.conf;

import java.io.File;

public class RollingConfiguration extends Configuration {

	private static final RollingConfiguration conf = new RollingConfiguration();

	public static final String ZK_SEVERS = "zookeeper.servers";
	public static final String ZK_SESSION_TIMEOUT = "zookeeper.session.timeout";
	public static final String ROLLING_ROOT_PATH = "rolling.root.path";
	public static final String ROLLING_CONFIG_PATH = "rolling.config.path";
	public static final String ROLLING_MEMBER_PATH = "rolling.member.path";
	public static final String ROLLING_MASTER_PATH = "rolling.master.path";
	
	public static RollingConfiguration getInstance() {
		return conf;
	}

	public RollingConfiguration() {
		super("/config.properties");
	}

	public RollingConfiguration(final String path) {
		super(path);
	}

	public RollingConfiguration(final File file) {
		super(file);
	}

	public String getZookeeperServers() {
		return getProperty(ZK_SEVERS);
	}

	public int getZookeeperSessionTimeout() {
		return getInt(ZK_SESSION_TIMEOUT);
	}
	
	public String getRollingRootPath() {
		return getProperty(ROLLING_ROOT_PATH);
	}
	
	public String getRollingConfigPath() {
		return getProperty(ROLLING_CONFIG_PATH);
	}
	
	public String getRollingMemberPath() {
		return getProperty(ROLLING_MEMBER_PATH);
	}
	
	public String getRollingMasterPath() {
		return getProperty(ROLLING_MASTER_PATH);
	}

	
	
}
