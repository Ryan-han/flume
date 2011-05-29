package com.nexr.dedup.workflow.job;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nexr.dedup.workflow.DedupConstants;
import com.nexr.framework.workflow.StepContext;
import com.nexr.rolling.workflow.RetryableDFSTaskletSupport;
import com.nexr.rolling.workflow.RollingConstants;

/**
 * Dedup workflow 작업을 위해 기초 작업
 * 
 * @author dani.kim@nexr.com
 */
public class InitTasklet extends RetryableDFSTaskletSupport {
	private Logger LOG = LoggerFactory.getLogger(getClass());
	
	@Override
	protected String doRun(StepContext context) {
		String path = context.getConfig().get(DedupConstants.PATH, null);
		String newSource = context.getConfig().get(DedupConstants.NEW_SOURCE_DIR, null);
		String source = context.getConfig().get(DedupConstants.SOURCE_DIR, null);
		String output = context.getConfig().get(DedupConstants.OUTPUT_PATH, null);
		String jobType = context.getConfig().get(RollingConstants.JOB_TYPE, null);
		String key = context.getJobExecution().getKey();
		LOG.info("Initialize Dedup. jobType: {}, jobId: {}", new Object[] { jobType, key });
		
		Path outputPath = new Path(output);
		try {
			if (fs.exists(outputPath)) {
				fs.delete(outputPath, true);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
		context.set(DedupConstants.PATH, path);
		context.set(DedupConstants.SOURCE_DIR, source);
		context.set(DedupConstants.NEW_SOURCE_DIR, newSource);
		context.set(DedupConstants.OUTPUT_PATH, String.format("%s/%s", output, context.getJobExecution().getKey()));
		return "prepare";
	}
}
