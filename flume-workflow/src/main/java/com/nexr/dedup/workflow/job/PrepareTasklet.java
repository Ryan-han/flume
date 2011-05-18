package com.nexr.dedup.workflow.job;

import com.nexr.framework.workflow.StepContext;
import com.nexr.rolling.workflow.RetryableDFSTaskletSupport;

/**
 * @author dani.kim@nexr.com
 */
public class PrepareTasklet extends RetryableDFSTaskletSupport {
	@Override
	public String doRun(StepContext context) {
		return "run";
	}
}
