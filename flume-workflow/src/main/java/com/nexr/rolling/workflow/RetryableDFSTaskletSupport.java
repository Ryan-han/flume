package com.nexr.rolling.workflow;

import java.io.IOException;
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

import com.nexr.framework.workflow.StepContext;
import com.nexr.framework.workflow.Tasklet;

/**
 * Tasklet 중 Retry 
 * 
 * @author dani.kim@nexr.com
 */
public abstract class RetryableDFSTaskletSupport implements Tasklet {
	protected Configuration conf;
	protected FileSystem fs;
	
	protected RetryTemplate retryTemplate;
	
	public RetryableDFSTaskletSupport() {
		this(10);
	}
	
	public RetryableDFSTaskletSupport(int retryCount) {
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
	
	@Override
	public String run(final StepContext context) {
		try {
			return retryTemplate.execute(new RetryCallback<String>() {
				@Override
				public String doWithRetry(RetryContext retryContext)
						throws Exception {
					return doRun(context);
				}
			});
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
//			try {
//				fs.close();
//			} catch (IOException e) {
//			}
		}
	}

	protected String doRun(StepContext context) {
		throw new RuntimeException();
	}
}
