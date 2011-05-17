package com.nexr.dedup.workflow.job;

import com.nexr.framework.workflow.StepContext;
import com.nexr.rolling.workflow.RetryableDFSTaskletSupport;

/**
 * @author dani.kim@nexr.com
 */
public class RunDedupMRTasklet extends RetryableDFSTaskletSupport {
	@Override
	protected String doRun(StepContext context) {
		return "finishing";
	}
}
