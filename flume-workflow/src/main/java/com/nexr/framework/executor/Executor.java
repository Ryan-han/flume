package com.nexr.framework.executor;

/**
 * @author dani.kim@nexr.com
 */
public interface Executor<T> {
	T execute(ExecutorCallback<T> callback);
}
