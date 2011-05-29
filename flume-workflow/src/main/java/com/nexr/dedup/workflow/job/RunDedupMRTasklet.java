package com.nexr.dedup.workflow.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InvalidInputException;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.retry.RetryCallback;
import org.springframework.batch.retry.RetryContext;

import com.nexr.dedup.workflow.DedupConstants;
import com.nexr.framework.workflow.StepContext;
import com.nexr.rolling.workflow.RetryableDFSTaskletSupport;
import com.nexr.rolling.workflow.RollingConstants;
import com.nexr.rolling.workflow.ZkClientFactory;
import com.nexr.rolling.workflow.ZkClientFactory.Lock;
import com.nexr.rolling.workflow.ZkClientFactory.ZkLockClient;

/**
 * Dedup MR 실행해주는 태스클릿
 * 
 * @author dani.kim@nexr.com
 */
public class RunDedupMRTasklet extends RetryableDFSTaskletSupport {
	private Logger LOG = LoggerFactory.getLogger(getClass());
	
	private ZkLockClient lockClient = ZkClientFactory.getLockClient();
	
	@Override
	protected String doRun(StepContext context) {
		String jobType = context.getConfig().get(RollingConstants.JOB_TYPE, null);
		List<String> params = new ArrayList<String>();
		if ("daily".equals(jobType)) {
			params.add(context.get(DedupConstants.SOURCE_DIR, null) + "/*/data");
			params.add(context.get(DedupConstants.NEW_SOURCE_DIR, null) + "/*/data");
		} else {
			params.add(context.get(DedupConstants.SOURCE_DIR, null) + "/*");
			params.add(context.get(DedupConstants.NEW_SOURCE_DIR, null) + "/*");
		}
		params.add(context.get(DedupConstants.OUTPUT_PATH, null));
		params.add(context.getConfig().get(DedupConstants.REDUCER_COUNT, "3"));

		Lock lock = lockClient.acquire(String.format("/lock/%s", context.getConfig().get(DedupConstants.LOCK, null)));
		LOG.info("Running Dedup M/R Job. jobType: {}, jobId: {}", jobType, context.getJobExecution().getKey());
		LOG.info("Running Dedup M/R Input: [{}, {}], Output: {}", params.toArray());
		try {
			String[] args = params.toArray(new String[params.size()]);
			
			String mapReduceClass = context.getConfig().get(DedupConstants.MR_CLASS, null);
			int exitCode = ToolRunner.run(conf, (Tool) Class.forName(mapReduceClass).newInstance(), args);
			if (exitCode != 0) {
				throw new RuntimeException("exitCode != 0");
			}
		} catch (InvalidInputException e) {
			Log.info(e.getMessage());
			try {
				renameTo(fs.listStatus(new Path(context.get(DedupConstants.NEW_SOURCE_DIR, null))), new Path(context.get(DedupConstants.SOURCE_DIR, null)));
			} catch (IOException e1) {
				throw new RuntimeException(e1);
			}
		} catch (Throwable e) {
			throw new RuntimeException(e);
		} finally {
			lock.unlock();
		}
		return "finishing";
	}
	
	private void renameTo(FileStatus[] files, Path destination) throws IOException {
		if (!fs.exists(destination)) {
			fs.mkdirs(destination);
		}
		for (final FileStatus file : files) {
			LOG.info("Find File {}", file.getPath());
			final Path destfile = new Path(destination, file.getPath().getName());
			try {
				boolean rename = retryTemplate.execute(new RetryCallback<Boolean>() {
					@Override
					public Boolean doWithRetry(RetryContext context) throws Exception {
						return fs.rename(file.getPath(), destfile);
					}
				});
				LOG.info("Moving {} to {}. status is {}", new Object[] { file.getPath(), destfile.toString(), rename });
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}
}
