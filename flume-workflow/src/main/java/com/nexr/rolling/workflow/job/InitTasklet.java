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
 * INPUT 과 OUTPUT 디렉토리를 체크한다. INPUT 이 없을 경우는 생성을 OUTPUT 이 있을 경우 제거한다.
 * 
 * @author dani.kim@nexr.com
 */
public class InitTasklet extends RetryableDFSTaskletSupport {
	private Logger LOG = LoggerFactory.getLogger(getClass());
	
	public String doRun(StepContext context) {
		String raw = context.getConfig().get(RollingConstants.RAW_PATH, null);
		String input = context.getConfig().get(RollingConstants.INPUT_PATH, null);
		String output = context.getConfig().get(RollingConstants.OUTPUT_PATH, null);
		String result = context.getConfig().get(RollingConstants.RESULT_PATH, null);
		String jobType = context.getConfig().get(RollingConstants.JOB_TYPE, null);
		String key = context.getJobExecution().getKey();
		LOG.info("Initialize Workflow. jobType: {}, jobId: {}", new Object[] { jobType, key });
		
		final Path inputPath = new Path(input);
		final Path outputPath = new Path(output);
		try {
			retryTemplate.execute(new RetryCallback<String>() {
				@Override
				public String doWithRetry(RetryContext context) throws Exception {
					if (!fs.exists(inputPath)) {
						fs.mkdirs(inputPath);
					}
					if (fs.exists(outputPath)) {
						fs.delete(outputPath, true);
					}
					return null;
				}
			});
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		context.set(RollingConstants.RAW_PATH, String.format("%s", raw));
		context.set(RollingConstants.INPUT_PATH, String.format("%s/%s", input, key));
		context.set(RollingConstants.OUTPUT_PATH, String.format("%s/%s", output, key));
		context.set(RollingConstants.RESULT_PATH, String.format("%s", result));
		return "prepare";
	}
}
