package com.nexr.dedup.workflow;

import org.I0Itec.zkclient.ZkClient;

import com.nexr.rolling.workflow.ZkClientFactory;
import com.nexr.rolling.workflow.job.Duplication;

/**
 * @author dani.kim@nexr.com
 */
public abstract class DedupTestBase {
	protected ZkClient client = ZkClientFactory.getClient();
	
	protected void createDedupJob(Duplication duplication) {
		String json = Duplication.JsonSerializer.serialize(duplication);
		if (!client.exists("/dedup/queue")) {
			client.createPersistent("/dedup/queue", true);
		}
		client.createPersistentSequential(String.format("%s/job-", "/dedup/queue"), json);
	}
}
