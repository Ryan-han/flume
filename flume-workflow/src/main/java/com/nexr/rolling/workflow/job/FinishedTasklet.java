package com.nexr.rolling.workflow.job;

import java.io.IOException;

import org.I0Itec.zkclient.ZkClient;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nexr.framework.workflow.StepContext;
import com.nexr.rolling.workflow.RetryableDFSTaskletSupport;
import com.nexr.rolling.workflow.RollingConstants;
import com.nexr.rolling.workflow.ZkClientFactory;

/**
 * post, hourly, daily 별 finish 작업이후에 할 일이 만약 있으면 기록. 현재는 많이 없을 것 같아서 if 로 분기하는 것으로 처리
 * 
 * @author dani.kim@nexr.com
 */
public class FinishedTasklet extends RetryableDFSTaskletSupport {
	private Logger LOG = LoggerFactory.getLogger(getClass());
	
	@Override
	protected String doRun(StepContext context) {
		String jobType = context.getConfig().get(RollingConstants.JOB_TYPE, null);
		if ("post".equals(jobType)) {
			String sourcePath = context.get(RollingConstants.INPUT_PATH, null);
			String today = context.getConfig().get(RollingConstants.TODAY_PATH, null);
			LOG.info("Renaming (post rolling). source: {}, dest: {}", new Object[] { sourcePath, today });
			if (sourcePath != null) {
				try {
					if (!fs.exists(new Path(today))) {
						fs.mkdirs(new Path(today));
					}
					renameTo(fs.listStatus(new Path(sourcePath)), new Path(today));
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		}
		return "cleanUp";
	}
	
	/**
	 * 파일 및 디렉토리 목록을 목표 디렉토리로 rename.
	 * @param files
	 * @param destination
	 * @throws IOException
	 */
	private void renameTo(FileStatus[] files, Path destination) throws IOException {
		for (FileStatus file : files) {
			if (file.isDir()) {
				renameTo(fs.listStatus(file.getPath()), destination);
			} else {
				LOG.info("Rename {} to {}", file.getPath(), destination);
				fs.rename(file.getPath(), destination);
			}
		}
	}
}
