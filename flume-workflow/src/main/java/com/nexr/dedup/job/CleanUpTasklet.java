package com.nexr.dedup.job;

import com.nexr.framework.workflow.StepContext;
import com.nexr.rolling.workflow.RetryableDFSTaskletSupport;

/**
 * @author dani.kim@nexr.com
 */
public class CleanUpTasklet extends RetryableDFSTaskletSupport {
	@Override
	protected String doRun(StepContext context) {
		return null;
	}
}
