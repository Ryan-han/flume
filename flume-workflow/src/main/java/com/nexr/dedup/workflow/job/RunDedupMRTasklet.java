package com.nexr.dedup.workflow.job;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nexr.dedup.workflow.DedupConstants;
import com.nexr.framework.workflow.StepContext;
import com.nexr.rolling.workflow.RetryableDFSTaskletSupport;

/**
 * Dedup MR 실행해주는 태스클릿
 * 
 * @author dani.kim@nexr.com
 */
public class RunDedupMRTasklet extends RetryableDFSTaskletSupport {
	private Logger LOG = LoggerFactory.getLogger(getClass());
	
	@Override
	protected String doRun(StepContext context) {
		List<String> params = new ArrayList<String>();
		params.add(context.get(DedupConstants.SOURCE_DIR, null) + File.separator + "*");
		params.add(context.get(DedupConstants.NEW_SOURCE_DIR, null) + File.separator + "*");
		params.add(context.get(DedupConstants.OUTPUT_PATH, null));

		LOG.info("Running Dedup M/R Job");
		try {
			String[] args = params.toArray(new String[params.size()]);
			
			String mapReduceClass = context.getConfig().get(DedupConstants.MR_CLASS, null);
			int exitCode = ToolRunner.run(conf, (Tool) Class.forName(mapReduceClass).newInstance(), args);
			if (exitCode != 0) {
				throw new RuntimeException("exitCode != 0");
			}
		} catch (Throwable e) {
			throw new RuntimeException(e);
		}
		return "finishing";
	}
}
