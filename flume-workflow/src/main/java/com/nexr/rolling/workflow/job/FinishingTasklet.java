package com.nexr.rolling.workflow.job;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

	@Override
	public String doRun(StepContext context) {
		String jobType = context.getConfig().get(RollingConstants.JOB_TYPE, null);
		String output = context.get(RollingConstants.OUTPUT_PATH, null);
		String result = context.get(RollingConstants.RESULT_PATH, null);

		Path sourcePath = new Path(output);
		try {
			if (!fs.exists(new Path(result))) {
				fs.mkdirs(new Path(result));
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		try {
			List<String> duplicated = new ArrayList<String>();
			
			FileStatus[] types = fs.listStatus(sourcePath);
			for (FileStatus type : types) {
				FileStatus[] timegroups = fs.listStatus(new Path(sourcePath, type.getPath().getName()));
				for (FileStatus group : timegroups) {
					FileStatus[] partials = fs.listStatus(new Path(type.getPath(), group.getPath().getName()), SEQ_FILE_FILTER);
					Path destdir = new Path(result, type.getPath().getName() + File.separator + group.getPath().getName());
					if (!fs.exists(destdir)) {
						fs.mkdirs(destdir);
					}
					if (fs.listStatus(destdir).length > 0) {
						String typeName = type.getPath().getName();
						String groupName = group.getPath().getName();
						LOG.info("Duplication was occured. path: {}/{}", typeName, groupName);
						duplicated.add(Duplication.JsonSerializer.serialize(new Duplication(jobType, output, result, String.format("%s/%s", typeName, groupName))));
						continue;
					}
					for (FileStatus file : partials) {
						LOG.info("Find File {}", file.getPath());
						Path destfile = new Path(destdir, file.getPath().getName());
						boolean rename = fs.rename(file.getPath(), destfile);
						LOG.info("Moving {} to {}. status is {}", new Object[] { file.getPath(), destfile.toString(), rename });
					}
				}
			}
			if (duplicated.size() > 0) {
				context.set("duplicated.count", Integer.toString(duplicated.size()));
				for (int i = 0; i < duplicated.size(); i++) {
					context.set(String.format("duplicated.%s", i), duplicated.get(i));
				}
				return "duplicate";
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return String.format("%s-%s", "finishing", jobType);
	}
}
