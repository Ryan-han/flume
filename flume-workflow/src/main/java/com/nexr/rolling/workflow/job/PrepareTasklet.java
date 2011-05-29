package com.nexr.rolling.workflow.job;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nexr.framework.workflow.StepContext;
import com.nexr.rolling.workflow.RetryableDFSTaskletSupport;
import com.nexr.rolling.workflow.RollingConstants;
import com.nexr.rolling.workflow.ZkClientFactory;
import com.nexr.rolling.workflow.ZkClientFactory.Lock;
import com.nexr.rolling.workflow.ZkClientFactory.ZkLockClient;

/**
 * 기초 데이터를 넣어주는 작업을 한다.
 * RESULT -> INPUT 으로 경로 변경
 * 
 * @author dani.kim@nexr.com
 */
public class PrepareTasklet extends RetryableDFSTaskletSupport {
	private Logger LOG = LoggerFactory.getLogger(getClass());

	private ZkLockClient lockClient = ZkClientFactory.getLockClient();
	
	final public static PathFilter DATA_FILTER = new PathFilter() {
		public boolean accept(Path file) {
			return file.getName().endsWith(".done");
		}
	};
	
	@Override
	public String doRun(StepContext context) {
		String jobType = context.getConfig().get(RollingConstants.JOB_TYPE, null);
		LOG.info("Prepare for M/R. jobType: {}, jobId: {}", new Object[] { jobType, context.getJobExecution().getKey() });
		Path sourcePath = new Path(context.get(RollingConstants.RAW_PATH, null));
		
		String lockName = context.getConfig().get(RollingConstants.PREV_JOB_TYPE, null);
		Lock lock = lockClient.acquire(lockName == null ? null : String.format("/lock/rolling/%s", lockName));
		try {
			boolean isCollectorSource = context.getConfig().getBoolean(RollingConstants.IS_COLLECTOR_SOURCE, false);
			Stats stats = new Stats();
			int depth = renameTo(fs.listStatus(sourcePath), context.get(RollingConstants.INPUT_PATH, null), isCollectorSource, stats);
			if (stats.count.intValue() == 0) {
				LOG.info("Input Directory is empty. Input: {}", sourcePath.toString());
				return "cleanUp";
			}
			LOG.info("Prepare stats - count: {}, length: {} MB", stats.count.intValue(), stats.length.longValue() / 1024 / 1024);
			context.set(RollingConstants.INPUT_DEPTH, Integer.toString(depth));
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			lock.unlock();
		}
		return "run";
	}

	private int renameTo(FileStatus[] files, String destination, boolean isCollectorSource, Stats stats) throws IOException {
		int depth = 1;
		if (files != null) {
			for (FileStatus file : files) {
				if (file.isDir() && !file.getPath().getName().startsWith("_")) {
					depth += renameTo(fs.listStatus(file.getPath()), String.format("%s/%s", destination, file.getPath().getName()), isCollectorSource, stats);
				} else {
					Path destinationPath = new Path(destination);
					if (!fs.exists(destinationPath)) {
						fs.mkdirs(destinationPath);
					}
					if (isCollectorSource == false || DATA_FILTER.accept(file.getPath())) {
						fs.rename(file.getPath(), destinationPath);
						stats.count.incrementAndGet();
						stats.length.addAndGet(file.getLen());
					}
				}
			}
		}
		return depth;
	}
	
	public static class Stats {
		AtomicInteger count = new AtomicInteger();
		AtomicLong length = new AtomicLong();
	}
}
