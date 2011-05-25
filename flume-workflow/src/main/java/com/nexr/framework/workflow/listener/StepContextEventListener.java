package com.nexr.framework.workflow.listener;

import com.nexr.framework.workflow.StepContext;

/**
 * @author dani.kim@nexr.com
 */
public interface StepContextEventListener {
	void commit(StepContext context);
}
