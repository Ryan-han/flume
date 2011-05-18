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
			boolean isCollectorSource = context.getConfig().getBoolean(RollingConstants.IS_COLLECTOR_SOURCE, false);
			int depth = renameTo(fs.listStatus(sourcePath), context.get(RollingConstants.INPUT_PATH, null), isCollectorSource);
			context.set(RollingConstants.INPUT_DEPTH, Integer.toString(depth));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return "run";
	}

	private int renameTo(FileStatus[] files, String destination, boolean isCollectorSource) throws IOException {
		int depth = 1;
		for (FileStatus file : files) {
			if (file.isDir()) {
				depth += renameTo(fs.listStatus(file.getPath()), String.format("%s/%s", destination, file.getPath().getName()), isCollectorSource);
			} else {
				Path destinationPath = new Path(destination);
				if (!fs.exists(destinationPath)) {
					fs.mkdirs(destinationPath);
				}
				fs.rename(file.getPath(), destinationPath);
			}
		}
		return depth;
	}
}
