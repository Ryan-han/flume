package com.nexr.rolling.workflow.job;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.retry.RetryCallback;
import org.springframework.batch.retry.RetryContext;

import com.nexr.framework.workflow.StepContext;
import com.nexr.rolling.workflow.RetryableDFSTaskletSupport;
import com.nexr.rolling.workflow.RollingConstants;

/**
 * INPUT 과 OUTPUT 디렉토리 내용을 모두 비운다.
 * 
 * @author dani.kim@nexr.com
 */
public class CleanUpTasklet extends RetryableDFSTaskletSupport {
	private Logger LOG = LoggerFactory.getLogger(getClass());
	
	@Override
	protected String doRun(StepContext context) {
		final String input = context.get(RollingConstants.INPUT_PATH, null);
		final String output = context.get(RollingConstants.OUTPUT_PATH, null);
		LOG.info("Rolling Job Cleanup. Input: {}, Output: {}", new Object[] { input, output });
		try {
			retryTemplate.execute(new RetryCallback<String>() {
				@Override
				public String doWithRetry(RetryContext context) throws Exception {
					fs.delete(new Path(input), true);
					fs.delete(new Path(output), true);
					return null;
				}
			});
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return null;
	}
}
