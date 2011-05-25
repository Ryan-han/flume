package com.nexr.dedup.workflow;

import org.junit.Before;
import org.junit.Test;

import com.nexr.dedup.DedupManager;
import com.nexr.rolling.workflow.job.Duplication;

/**
 * @author dani.kim@nexr.com
 */
public class TestDedupWorkflow extends DedupTestBase {
	private DedupManager manager = new DedupManager();
	
	@Before
	public void init() throws Exception {
		new Thread(manager).start();
		createDedupJob(new Duplication("", "", "", ""));

		while (manager.isRunning()) {
			Thread.sleep(10000);
			manager.stop();
		}
	}
	
	@Test
	public void test() throws Exception {
	}
}
