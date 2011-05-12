package com.nexr.rolling.workflow;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nexr.framework.workflow.StepContext;
import com.nexr.rolling.workflow.DFSTasklet;
import com.nexr.rolling.workflow.RollingConstants;

/**
 * @author dani.kim@nexr.com
 */
public class PrepareTasklet extends DFSTasklet {
	private Logger LOG = LoggerFactory.getLogger(getClass());

	final public static PathFilter DATA_FILTER = new PathFilter() {
		public boolean accept(Path file) {
			return file.getName().endsWith(".done");
		}
	};

	public PrepareTasklet() {
		super();
	}
	
	@Override
	public String run(StepContext context) {
		Path sourcePath = new Path(context.getConfig().get(RollingConstants.RESULT_PATH, null));
		FileStatus[] dataType = null;
		FileStatus[] hours = null;
		FileStatus[] files = null;
		try {
			dataType = fs.listStatus(sourcePath);
			int count = 0;
			for (FileStatus type : dataType) {
				hours = fs.listStatus(new Path(sourcePath, type.getPath()
						.getName()));
				for (FileStatus hour : hours) {
					files = fs.listStatus(hour.getPath());
					LOG.info("===> " + hour.getPath() + " " + files.length);
					for (FileStatus file : files) {
						String input = context.getConfig().get(RollingConstants.INPUT_PATH, null);
						if (!fs.exists(new Path(input + File.separator + type.getPath().getName()))) {
							fs.mkdirs(new Path(input + File.separator + type.getPath().getName()));
						}
						LOG.info("Find File " + file.getPath());
						boolean rename = fs.rename(file.getPath() , new Path(input + File.separator + type.getPath().getName()));
						LOG.info("Moving " + file.toString() + ", status is: " + rename);
						count++;
					}
				}
			}
			if (count == 0) {
				throw new RuntimeException();
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return "run";
	}
}
