package com.nexr.rolling.workflow.job;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nexr.framework.workflow.StepContext;
import com.nexr.rolling.workflow.RetryableDFSTaskletSupport;
import com.nexr.rolling.workflow.RollingConstants;

/**
 * INPUT 과 OUTPUT 디렉토리를 체크한다. INPUT 이 없을 경우는 생성을 OUTPUT 이 있을 경우 제거한다.
 * 
 * @author dani.kim@nexr.com
 */
public class InitTasklet extends RetryableDFSTaskletSupport {
	private Logger LOG = LoggerFactory.getLogger(getClass());
	
	public String doRun(StepContext context) {
		Path input = new Path(context.getConfig().get(RollingConstants.INPUT_PATH, null));
		Path output = new Path(context.getConfig().get(RollingConstants.OUTPUT_PATH, null));
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
		LOG.info("Rolling Job Success Initialization : Input: " + input.getName());
		return "prepare";
	}
}
