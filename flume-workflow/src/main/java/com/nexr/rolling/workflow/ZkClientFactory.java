package com.nexr.rolling.workflow;

import java.util.Collections;
import java.util.List;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;

import com.nexr.sdp.Configuration;

/**
 * @author dani.kim@nexr.com
 */
public class ZkClientFactory {
	private static Configuration config;
//	private static ZkClient client;
	private static ZkLockClient locker;
	
	static {
		config = Configuration.getInstance();
//		client = new ZkClient(config.getZkServerAsString(), 30000, config.getZkTimeout());
		locker = new ZkLockClient();
	}
	
	public static ZkClient getClient() {
		return new ZkClient(config.getZkServerAsString(), 10000, config.getZkTimeout());
	}
	
	public static ZkLockClient getLockClient() {
		return locker;
	}
	
	public static class Lock implements IZkChildListener {
		private ZkClient client;
		private String path;
		private String name;

		Lock(ZkClient client, String name) {
			this.client = client;
			this.path = name;
		}
		
		@Override
		public synchronized void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
			notifyAll();
		}
		
		public synchronized void acquire() {
			if (path == null) {
				return;
			}
			if (!client.exists(path)) {
				client.createPersistent(path, true);
			}
			name = client.createEphemeralSequential(String.format("%s/seq-", path), "");
			client.subscribeChildChanges(path, this);
			while (!validate()) {
				try {
					wait();
				} catch (InterruptedException e) {
				}
			}
		}

		private boolean validate() {
			List<String> children = client.getChildren(path);
			Collections.sort(children);
			if (children.size() > 0) {	
				boolean valid = name != null && name.endsWith(children.get(0));
				if (valid) {
					client.unsubscribeChildChanges(path, this);
				}
				return valid;
			}
			return false;
		}

		public synchronized void unlock() {
			if (path == null) {
				return;
			}
			client.delete(name);
		}
	}
	
	public static class ZkLockClient {
		public ZkLockClient() {
		}
		
		public Lock acquire(String name) {
			Lock lock = new Lock(getClient(), name);
			lock.acquire();
			return lock;
		}
	}
}
