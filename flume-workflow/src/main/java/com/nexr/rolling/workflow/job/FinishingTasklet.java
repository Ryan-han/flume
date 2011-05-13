package com.nexr.rolling.workflow.job;

import java.io.File;
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
 * Map/Reduce 결과를 이동시킨다.
 * @author dani.kim@nexr.com
 */
public class FinishingTasklet extends RetryableDFSTaskletSupport {
	private Logger LOG = LoggerFactory.getLogger(getClass());

	final public static PathFilter SEQ_FILE_FILTER = new PathFilter() {
		public boolean accept(Path file) {
			return file.getName().startsWith("part");
		}
	};

	@SuppressWarnings("unused")
	@Override
	public String run(StepContext context) {
		String output = context.getConfig().get(RollingConstants.OUTPUT_PATH, null);
		String result = context.getConfig().get(RollingConstants.RESULT_PATH, null);

		Path sourcePath = new Path(output);
		FileStatus[] types = null;
		FileStatus[] timegroups = null;
		FileStatus[] partials = null;
		try {
			if (!fs.exists(new Path(result))) {
				fs.mkdirs(new Path(result));
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		try {
			types = fs.listStatus(sourcePath);
			for (FileStatus type : types) {
				timegroups = fs.listStatus(new Path(sourcePath, type.getPath().getName()));
				for (FileStatus group : timegroups) {
					partials = fs.listStatus(new Path(type.getPath(), group.getPath().getName()), SEQ_FILE_FILTER);
					for (FileStatus file : partials) {
						LOG.info("Find File {}", file.getPath());
						String dirName = group.getPath().getName();
						Path dest = new Path(result, type.getPath().getName() + File.separator + group.getPath().getName());
						if (!fs.exists(dest)) {
							fs.mkdirs(dest);
						}
						boolean rename = fs.rename(file.getPath(), new Path(dest, file.getPath().getName()));
						LOG.info("Moving {} to {}. status is {}", new Object[] { file.getPath(), new Path(dest, file.getPath().getName()).toString(), rename });
					}
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return "cleanUp";
	}
}
