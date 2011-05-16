package com.nexr.rolling.workflow.daily;

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
 * @author dani.kim@nexr.com
 */
@Deprecated
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
		String output = context.getConfig().get(RollingConstants.DAILY_MR_OUTPUT_PATH, null);
		String result = context.getConfig().get(RollingConstants.DAILY_MR_RESULT_PATH, null);

		Path sourcePath = new Path(output);
		FileStatus[] dataType = null;
		FileStatus[] days = null;
		FileStatus[] files = null;
		try {
			if (!fs.exists(new Path(result))) {
				fs.mkdirs(new Path(result));
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		try {
			dataType = fs.listStatus(sourcePath);
			boolean rename;
			String dirName;
			for (FileStatus type : dataType) {
				days = fs.listStatus(new Path(sourcePath, type.getPath()
						.getName()));
				for (FileStatus day : days) {
					files = fs.listStatus(new Path(sourcePath, type.getPath().getName() + File.separator + day.getPath().getName()), SEQ_FILE_FILTER);
					for (FileStatus file : files) {
						LOG.info("Find File " + file.getPath());
						dirName = day.getPath().getName();
						Path dest = new Path(result + File.separator
								+ type.getPath().getName() + File.separator
								+ day.getPath().getName());
						if (!fs.exists(dest)) {
							fs.mkdirs(dest);
							rename = fs.rename(file.getPath(),
									new Path(dest + File.separator
											+ file.getPath().getName()));
						} else {
							rename = fs.rename(file.getPath(),
									new Path(dest + File.separator
											+ file.getPath().getName()));
						}
						LOG.info("Moving "
								+ file.getPath()
								+ " to "
								+ new Path(dest + File.separator
										+ file.getPath().getName()).toString()
								+ " " + rename);
					}
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return "cleanUp";
	}
}
