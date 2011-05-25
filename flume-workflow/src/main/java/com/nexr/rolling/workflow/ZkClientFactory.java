package com.nexr.rolling.workflow;

import org.I0Itec.zkclient.ZkClient;

import com.nexr.sdp.Configuration;

/**
 * @author dani.kim@nexr.com
 */
public class ZkClientFactory {
	private static Configuration config;
	private static ZkClient client;
	
	static {
		config = Configuration.getInstance();
		client = new ZkClient(config.getZkServerAsString(), 30000, config.getZkTimeout());
	}
	
	public static ZkClient getClient() {
		return client;
	}
}
