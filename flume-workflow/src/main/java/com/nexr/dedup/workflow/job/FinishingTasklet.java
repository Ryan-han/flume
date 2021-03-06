package com.nexr.dedup.workflow.job;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.retry.RetryCallback;
import org.springframework.batch.retry.RetryContext;

import com.nexr.dedup.workflow.DedupConstants;
import com.nexr.framework.workflow.StepContext;
import com.nexr.rolling.workflow.RetryableDFSTaskletSupport;

/**
 * Dedup MR 작업 후 결과파일을 원래 위치해야할 디렉토리로 이름변경 작업
 * 
 * @author dani.kim@nexr.com
 */
public class FinishingTasklet extends RetryableDFSTaskletSupport {
	private Logger LOG = LoggerFactory.getLogger(getClass());
	
	@Override
	protected String doRun(StepContext context) {
		String result = context.get(DedupConstants.SOURCE_DIR, null);
		String output = context.get(DedupConstants.OUTPUT_PATH, null);

		try {
			// 삭제되고 rename 되기까지 시간동안 요청이 들어오면 에러가 발생할 것임. 분산락을 고려할 필요가 있습니다.
			fs.delete(new Path(result), true);
			renameTo(fs.listStatus(new Path(output)), new Path(result));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return "cleanUp";
	}

//	private void renameTo(FileStatus[] files, Path resultPath) throws IOException {
//		for (FileStatus file : files) {
//			if (file.isDir()) {
//				renameTo(fs.listStatus(file.getPath()), resultPath);
//			} else {
//				fs.rename(file.getPath(), resultPath);
//			}
//		}
//	}
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
