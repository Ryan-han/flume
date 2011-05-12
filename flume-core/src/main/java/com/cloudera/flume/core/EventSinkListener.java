package com.cloudera.flume.core;

/**
 * EventSink의 open(), close()메소드가 호출 될때 이벤트를 받을 리스너
 * @author bitaholic
 *
 */
public interface EventSinkListener {
	public void open();
	public void close();

}
