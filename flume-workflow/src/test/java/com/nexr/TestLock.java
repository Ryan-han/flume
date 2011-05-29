package com.nexr;

import org.junit.Test;

import com.nexr.rolling.workflow.ZkClientFactory;
import com.nexr.rolling.workflow.ZkClientFactory.Lock;
import com.nexr.rolling.workflow.ZkClientFactory.ZkLockClient;

/**
 * @author dani.kim@nexr.com
 */
public class TestLock {
	@Test
	public void test() throws Exception {
		ZkLockClient locker = ZkClientFactory.getLockClient();
		Lock lock = locker.acquire("/test/post/2011_01_01");
		System.out.println("acquire lock");
		Thread.sleep(10000);
		lock.unlock();
		System.out.println("unlock");
	}
}
