package com.nexr.rolling.workflow.daily;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nexr.rolling.workflow.RetryableDFSTaskletSupport;
import com.nexr.rolling.workflow.RollingConstants;

/**
 * @author dani.kim@nexr.com
 */
@Deprecated
public class InitTasklet extends RetryableDFSTaskletSupport {
	private Logger LOG = LoggerFactory.getLogger(getClass());
	
	public String run(com.nexr.framework.workflow.StepContext context) {
		Path input = new Path(context.getConfig().get(RollingConstants.DAILY_MR_INPUT_PATH, null));
		Path output = new Path(context.getConfig().get(RollingConstants.DAILY_MR_OUTPUT_PATH, null));
		try {
			if (!fs.exists(input)) {
				fs.mkdirs(input);
			}
			if (fs.exists(output)) {
				fs.delete(output, true);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		LOG.info("Success Initialization : Create " + input.getName());
		return "prepare";
	}
}
