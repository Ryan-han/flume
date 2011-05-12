package com.nexr.rolling.workflow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.nexr.framework.workflow.JobExecution;
import com.nexr.framework.workflow.JobLauncher;
import com.nexr.framework.workflow.SimpleJob;
import com.nexr.framework.workflow.Step;
import com.nexr.framework.workflow.StepContext;
import com.nexr.framework.workflow.Steps;
import com.nexr.framework.workflow.listener.StepExecutionListener;
import com.nexr.rolling.workflow.daily.CleanUpTasklet;
import com.nexr.rolling.workflow.daily.FinishingTasklet;
import com.nexr.rolling.workflow.daily.InitTasklet;
import com.nexr.rolling.workflow.daily.PrepareTasklet;
import com.nexr.rolling.workflow.daily.RunDailyTasklet;

/**
 * @author dani.kim@nexr.com
 */
@Deprecated
public class DailyRollingJob extends SimpleJob {
	private static final Logger LOG = LoggerFactory.getLogger(DailyRollingJob.class);
	private final static Steps steps = new Steps();
	
	static {
		steps.add(new Step("init", InitTasklet.class));
		steps.add(new Step("prepare", PrepareTasklet.class));
		steps.add(new Step("run", RunDailyTasklet.class));
		steps.add(new Step("finishing", FinishingTasklet.class));
		steps.add(new Step("cleanUp", CleanUpTasklet.class));
	}

	public DailyRollingJob() {
		super("dailyRollingJob", steps, true);
	}
	
	public static void main(String[] args) throws Exception {
		ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("classpath:workflow-app.xml");
		JobLauncher launcher = context.getBean(JobLauncher.class);

		DailyRollingJob job = new DailyRollingJob();
		job.setStepExecutionListener(new StepExecutionListener() {
			@Override
			public void beforeStep(Step step, StepContext context) {
				// ZK 상태 변경
			}
			
			@Override
			public void afterStep(Step step, StepContext context) {
				// ZK 상태 변경
			}
			
			@Override
			public void caught(Step step, StepContext context, Throwable cause) {
			}
		});
		job.setExecutionDao(new ZKJobExecutionDao());
		job.addParameter(RollingConstants.HOURLY_MR_RESULT_PATH, "/nexr/rolling/hourly/result");
		job.addParameter(RollingConstants.DAILY_MR_INPUT_PATH, "/nexr/rolling/daily/input");
		job.addParameter(RollingConstants.DAILY_MR_OUTPUT_PATH, "/nexr/rolling/daily/output");
		job.addParameter(RollingConstants.DAILY_MR_RESULT_PATH, "/nexr/rolling/daily/result");
		
		JobExecution execution = launcher.run(job);
		LOG.info("Workflow : {}", execution.getWorkflow());
		
	}
}
