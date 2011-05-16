package com.nexr.framework.workflow;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.retry.RetryCallback;
import org.springframework.batch.retry.RetryContext;
import org.springframework.batch.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.batch.retry.policy.SimpleRetryPolicy;
import org.springframework.batch.retry.support.RetryTemplate;

public class TestSpringBatch {
	private Logger LOG = LoggerFactory.getLogger(getClass());
	
	@Test
	public void testRetry() throws Exception {
		RetryTemplate template = new RetryTemplate();
		ExponentialBackOffPolicy policy = new ExponentialBackOffPolicy();
		policy.setInitialInterval(1000);
		policy.setMultiplier(5);
		template.setBackOffPolicy(policy);
		Map<Class<? extends Throwable>, Boolean> retryables = new HashMap<Class<? extends Throwable>, Boolean>();
		retryables.put(Exception.class, true);
		template.setRetryPolicy(new SimpleRetryPolicy(5, retryables));
		
		final CountDownLatch counter = new CountDownLatch(5);
		template.execute(new RetryCallback<String>() {
			@Override
			public String doWithRetry(RetryContext context) throws Exception {
				LOG.info("time: {}, retryCount: {}", new Date(), context.getRetryCount());
				counter.countDown();
				if (counter.getCount() == 0) {
					return "Hello World";
				}
				throw new Exception();
			}
		});
		counter.await(1000, TimeUnit.SECONDS);
	}
}
