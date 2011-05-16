package com.nexr.rolling.workflow.hourly;

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
public class PrepareTasklet extends RetryableDFSTaskletSupport {
	private Logger LOG = LoggerFactory.getLogger(getClass());

	final public static PathFilter DATA_FILTER = new PathFilter() {
		public boolean accept(Path file) {
			return file.getName().endsWith(".done");
		}
	};

	@Override
	public String run(StepContext context) {
		Path sourcePath = new Path(context.getConfig().get(RollingConstants.HOURLY_MR_RAW_PATH, ""));
		FileStatus[] dataType = null;
		FileStatus[] dataFile = null;
		try {
			dataType = fs.listStatus(sourcePath);
			for (FileStatus type : dataType) {
				dataFile = fs.listStatus(new Path(sourcePath, type.getPath().getName()), DATA_FILTER);
				for (FileStatus file : dataFile) {
					String input = context.getConfig().get(RollingConstants.HOURLY_MR_INPUT_PATH, null);
					if (!fs.exists(new Path(input + File.separator + type.getPath().getName()))) {
						fs.mkdirs(new Path(input + File.separator + type.getPath().getName()));
					}
					
					if(dataFile.length>0){
						LOG.info("Fine File " + file.getPath());
						boolean rename = fs.rename(file.getPath(), new Path(
								input + File.separator
										+ type.getPath().getName() + File.separator
										+ file.getPath().getName()));
						LOG.info("Moving " + file.getPath() + " to "
								+ input + File.separator
								+ type.getPath().getName() + File.separator
								+ file.getPath().getName() + ", status is: "
								+ rename);
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return "run";
	}
}
