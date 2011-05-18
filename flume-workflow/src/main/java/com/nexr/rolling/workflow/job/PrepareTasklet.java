package com.nexr.rolling.workflow.job;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nexr.framework.workflow.StepContext;
import com.nexr.rolling.workflow.RetryableDFSTaskletSupport;
import com.nexr.rolling.workflow.RollingConstants;

/**
 * 기초 데이터를 넣어주는 작업을 한다.
 * RESULT -> INPUT 으로 경로 변경
 * 
 * @author dani.kim@nexr.com
 */
public class PrepareTasklet extends RetryableDFSTaskletSupport {
	@SuppressWarnings("unused")
	private Logger LOG = LoggerFactory.getLogger(getClass());

	final public static PathFilter DATA_FILTER = new PathFilter() {
		public boolean accept(Path file) {
			return file.getName().endsWith(".done");
		}
	};
	
	@Override
	public String doRun(StepContext context) {
		Path sourcePath = new Path(context.get(RollingConstants.RAW_PATH, null));
		try {
			FileStatus[] types = fs.listStatus(sourcePath);
			for (FileStatus type : types) {
				String input = context.get(RollingConstants.INPUT_PATH, null);
				if (!fs.exists(new Path(input, type.getPath().getName()))) {
					fs.mkdirs(new Path(input, type.getPath().getName()));
				}
				String isCollectorSource = context.getConfig().get(RollingConstants.IS_COLLECTOR_SOURCE, "false");
				if ("true".equals(isCollectorSource)) {
					FileStatus[] collectorSources = fs.listStatus(new Path(sourcePath, type.getPath().getName()), DATA_FILTER);
					for (FileStatus file : collectorSources) {
						rename(file.getPath(), String.format("%s/%s/%s/%s", input, type.getPath().getName(), System.currentTimeMillis()));
					}
				} else {
					FileStatus[] timegroups = fs.listStatus(new Path(sourcePath, type.getPath().getName()));
					for (FileStatus file : timegroups) {
						rename(file.getPath(), String.format("%s/%s/%s", input, type.getPath().getName()));
					}
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return "run";
	}

	private void rename(Path source, String destDir) throws IOException {
		if (!fs.exists(new Path(destDir))) {
			fs.mkdirs(new Path(destDir));
		}
		fs.rename(source, new Path(destDir));
	}
}
