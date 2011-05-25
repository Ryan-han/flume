package com.nexr.rolling.workflow.job;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nexr.framework.workflow.StepContext;
import com.nexr.rolling.workflow.RetryableDFSTaskletSupport;
import com.nexr.rolling.workflow.RollingConstants;

/**
 * Rolling MR 실행해주는 태스클릿
 * 
 * @author dani.kim@nexr.com
 */
public class RunRollingMRTasklet extends RetryableDFSTaskletSupport {
	private Logger LOG = LoggerFactory.getLogger(getClass());
	
	@Override
	public String doRun(StepContext context) {
		List<String> params = new ArrayList<String>();
		params.add(createInputPath(context.get(RollingConstants.INPUT_PATH, null), context.getInt(RollingConstants.INPUT_DEPTH, 1)));
		params.add(context.get(RollingConstants.OUTPUT_PATH, null));
		
		String jobType = context.getConfig().get(RollingConstants.JOB_TYPE, null);
		LOG.info("Running Rolling M/R Job. jobType: {}, jobId: {}", jobType, context.getJobExecution().getKey());
		LOG.info("Running Rolling M/R Input: {}, Output: {}", params.get(0), params.get(1));
		
		try {
			String[] args = params.toArray(new String[params.size()]);
			
			String mapReduceClass = context.getConfig().get(RollingConstants.MR_CLASS, null);
			int exitCode = ToolRunner.run(conf, (Tool) Class.forName(mapReduceClass).newInstance(), args);
			if (exitCode != 0) {
				throw new RuntimeException("exitCode != 0");
			}
		} catch (Throwable e) {
			throw new RuntimeException(e);
		}
		return "finishing";
	}

	protected String createInputPath(String path, int depth) {
		StringBuilder sb = new StringBuilder();
		sb.append(path);
		for (int i = 0; i < depth; i++) {
			sb.append("/*");
		}
		return sb.toString();
	}
}
