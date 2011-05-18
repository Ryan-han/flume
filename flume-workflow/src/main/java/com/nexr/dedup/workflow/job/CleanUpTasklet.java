package com.nexr.dedup.workflow.job;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nexr.dedup.workflow.DedupConstants;
import com.nexr.framework.workflow.StepContext;
import com.nexr.rolling.workflow.RetryableDFSTaskletSupport;

/**
 * MR 에서 사용한 INPUT, OUTPUT 제거
 * 
 * @author dani.kim@nexr.com
 */
public class CleanUpTasklet extends RetryableDFSTaskletSupport {
	private Logger LOG = LoggerFactory.getLogger(getClass());
	
	@Override
	protected String doRun(StepContext context) {
		String input = context.get(DedupConstants.NEW_SOURCE_DIR, null);
		String output = context.get(DedupConstants.OUTPUT_PATH, null);
		LOG.info("Dedup Job Cleanup. Input: {}, Output: {}", new Object[] { input, output });
		try {
			fs.delete(new Path(input), true);
			fs.delete(new Path(output), true);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return null;
	}
}
