package com.nexr.rolling.workflow.job;

import java.io.IOException;

import org.I0Itec.zkclient.ZkClient;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nexr.framework.workflow.StepContext;
import com.nexr.rolling.workflow.RetryableDFSTaskletSupport;
import com.nexr.rolling.workflow.RollingConstants;
import com.nexr.rolling.workflow.ZkClientFactory;

/**
 * @author dani.kim@nexr.com
 */
public class DuplicateTasklet extends RetryableDFSTaskletSupport {
	private static final String DEDUP_QUEUE = "/dedup/queue";
	
	private Logger LOG = LoggerFactory.getLogger(getClass());
	private ZkClient client = ZkClientFactory.getClient();
	
	public String doRun(StepContext context) {
		int count = context.getInt("duplicated.count", 0);
		LOG.info("Start Dedup processing");
		for (int i = 0; i < count; i++) {
			if (!client.exists(DEDUP_QUEUE)) {
				client.createPersistent(DEDUP_QUEUE, true);
			}
			client.createPersistentSequential(String.format("%s/job-", DEDUP_QUEUE), context.get(String.format("duplicated.%s", i), null));
		}
		try {
			fs.delete(new Path(context.get(RollingConstants.INPUT_PATH, null)), true);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return null;
	}
}
