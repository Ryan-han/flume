package com.nexr.rolling.workflow.daily;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nexr.data.sdp.rolling.mr.DailyRollingMr;
import com.nexr.framework.workflow.StepContext;
import com.nexr.rolling.workflow.RetryableDFSTaskletSupport;
import com.nexr.rolling.workflow.RollingConstants;

/**
 * @author dani.kim@nexr.com
 */
@Deprecated
public class RunDailyTasklet extends RetryableDFSTaskletSupport {
	private Logger LOG = LoggerFactory.getLogger(getClass());
	
	@Override
	public String run(StepContext context) {
		List<String> params = new ArrayList<String>();
		params.add(context.getConfig().get(RollingConstants.DAILY_MR_INPUT_PATH, null) + File.separator + "*" + File.separator + "*" + File.separator + "*");
		params.add(context.getConfig().get(RollingConstants.DAILY_MR_OUTPUT_PATH, null));

		LOG.info("Running M/R Job");
		try {
			String[] args = params.toArray(new String[params.size()]);
			int exitCode = ToolRunner.run(conf, new DailyRollingMr(), args);
			if (exitCode != 0) {
				throw new RuntimeException("exitCode != 0");
			}
		} catch (Throwable e) {
			e.printStackTrace();
		}
		return "finishing";
	}
}
