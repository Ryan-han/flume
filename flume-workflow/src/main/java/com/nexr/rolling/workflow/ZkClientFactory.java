package com.nexr.rolling.workflow;

import org.I0Itec.zkclient.ZkClient;

import com.nexr.rolling.conf.RollingConfiguration;

/**
 * @author dani.kim@nexr.com
 */
public class ZkClientFactory {
	public static ZkClient getClient() {
		RollingConfiguration conf = new RollingConfiguration();
		ZkClient client = new ZkClient(conf.getZookeeperServers(), 30000, conf.getZookeeperSessionTimeout());
		return client;
	}
}
