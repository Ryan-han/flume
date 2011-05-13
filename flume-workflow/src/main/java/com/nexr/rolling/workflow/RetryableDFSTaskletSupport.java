package com.nexr.rolling.workflow;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.batch.retry.RetryCallback;
import org.springframework.batch.retry.RetryContext;
import org.springframework.batch.retry.RetryPolicy;
import org.springframework.batch.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.batch.retry.policy.SimpleRetryPolicy;
import org.springframework.batch.retry.support.RetryTemplate;

import com.nexr.framework.workflow.Tasklet;

/**
 * @author dani.kim@nexr.com
 */
public abstract class RetryableDFSTaskletSupport implements Tasklet {
	protected Configuration conf;
	protected FileSystem fs;
	
	protected RetryTemplate retryTemplate;
	
	public RetryableDFSTaskletSupport() {
		retryTemplate = new RetryTemplate();
		retryTemplate.setBackOffPolicy(new ExponentialBackOffPolicy());
		Map<Class<? extends Throwable>, Boolean> retryableExecptions = new HashMap<Class<? extends Throwable>, Boolean>();
		retryableExecptions.put(Exception.class, true);
		RetryPolicy retryPolicy = new SimpleRetryPolicy(10, retryableExecptions);
		retryTemplate.setRetryPolicy(retryPolicy);
		conf = new Configuration();
		try {
			fs = retryTemplate.execute(new RetryCallback<FileSystem>() {
				@Override
				public FileSystem doWithRetry(RetryContext context) throws Exception {
					return FileSystem.get(conf);
				}
			});
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
