package com.nexr.rolling.workflow.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.I0Itec.zkclient.ZkClient;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.retry.RetryCallback;
import org.springframework.batch.retry.RetryContext;

import com.nexr.framework.workflow.StepContext;
import com.nexr.framework.workflow.StepContext.Config;
import com.nexr.rolling.workflow.RetryableDFSTaskletSupport;
import com.nexr.rolling.workflow.RollingConstants;
import com.nexr.rolling.workflow.ZkClientFactory;
import com.nexr.rolling.workflow.job.Sources.Source;

/**
 * Map/Reduce 결과를 이동시킨다.
 * @author dani.kim@nexr.com
 */
public class FinishingTasklet extends RetryableDFSTaskletSupport {
	private Logger LOG = LoggerFactory.getLogger(getClass());

	private String jobType;
	private String output;
	private String result;
	private String zkRootPath;
	private ZkClient client = ZkClientFactory.getClient();

	private Sources sources;

	final public static PathFilter SEQ_FILE_FILTER = new PathFilter() {
		public boolean accept(Path file) {
			return file.getName().startsWith("part");
		}
	};

	protected String doRun(final StepContext context) {
		Config config = context.getConfig();
		jobType = config.get(RollingConstants.JOB_TYPE, null);
		zkRootPath = config.get(RollingConstants.NOTIFY_ZKPATH_AFTER_ROLLING, "/collector");
		output = context.get(RollingConstants.OUTPUT_PATH, null);
		result = context.get(RollingConstants.RESULT_PATH, null);
		
		LOG.info("Finishing. jobType: {}, jobId: {}", jobType, context.getJobExecution().getKey());
		
		Path sourcePath = new Path(output);
		List<String> duplicated = new ArrayList<String>();
		try {
			sources = getSources(context, sourcePath);
			for (final String group : sources.keys()) {
				try {
					duplicated.addAll(retryTemplate.execute(new RetryCallback<List<String>>() {
						@Override
						public List<String> doWithRetry(RetryContext retryContext) throws Exception {
							List<String> duplications = new ArrayList<String>();
							for (Source source : sources.get(group)) {
								if (source.partials == null || source.partials.length == 0) {
									continue;
								}
								String key = String.format("rolling.lock.%s", source.path);
								Path destdir = new Path(result, String.format("%s/%s", source.type, source.group));
								FileStatus[] chlidren = fs.listStatus(destdir);
								if (chlidren == null || chlidren.length == 0) {
									context.set(key, "InProgress");
									context.commit();
								} else if (context.get(key, null) == null && chlidren.length > 0) {
									context.set(key, "Dedup");
									context.commit();
								}
								if ("InProgress".equals(context.get(key, null))) {
									renameTo(source.partials, destdir);
									context.remove(key);
									context.commit();
								} else if ("Dedup".equals(context.get(key, null))) {
									duplications.add(Duplication.JsonSerializer.serialize(new Duplication(jobType, output, result, source.path)));
								}
							}
							return duplications;
						}
					}));
					if (!"post".equals(jobType)) {
						retryTemplate.execute(new RetryCallback<String>() {
							@Override
							public String doWithRetry(RetryContext context) throws Exception {
									String znode = String.format("%s/%s/%s", zkRootPath, jobType, group);
									StringBuilder json = new StringBuilder();
									json.append("[");
									boolean first = true;
									for (Source source : sources.get(group)) {
										json.append(first ? "" : ",").append(Source.JsonSerializer.serialize(source));
										first = false;
									}
									json.append("]");
									if (!client.exists(znode)) {
										client.createPersistent(znode, true);
									}
									client.writeData(znode, json.toString());
								return null;
							}
						});
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			if (duplicated.size() > 0) {
				context.set("duplicated.count", Integer.toString(duplicated.size()));
				for (int i = 0; i < duplicated.size(); i++) {
					context.set(String.format("duplicated.%s", i), duplicated.get(i));
				}
				return "duplicate";
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return String.format("%s-%s", "finishing", jobType);
	}

	private Sources getSources(final StepContext context, Path sourcePath) throws IOException {
		Sources sources = new Sources();
		if (context.getInt("result.count", -1) == -1) {
			FileStatus[] types = fs.listStatus(sourcePath);
			int count = 0;
			for (FileStatus type : types) {
				FileStatus[] timegroups = fs.listStatus(type.getPath());
				for (FileStatus timegroup : timegroups) {
					if (timegroup.getPath().getName().split("_").length > 2) {
						Source source = new Source(type.getPath().getName(), timegroup.getPath().getName(), fs.listStatus(timegroup.getPath(), SEQ_FILE_FILTER));
						sources.addSource(source);
						context.set(String.format("result.%s", count++), Source.JsonSerializer.serialize(source));
					}
				}
			}
			context.set("result.count", Integer.toString(count));
			context.commit();
		} else {
			int count = context.getInt("result.count", -1);
			for (int i = 0; i < count; i++) {
				Source source = Source.JsonDeserializer.deserialize(context.get(String.format("result.%s", i), null));
				FileStatus[] partials = fs.listStatus(new Path(sourcePath, String.format("%s/%s", source.type, source.group)), SEQ_FILE_FILTER);
				source.setPartials(partials);
				sources.addSource(source.group, source);
			}
		}
		return sources;
	}
	
	private void renameTo(FileStatus[] files, Path destination) throws IOException {
		if (!fs.exists(destination)) {
			fs.mkdirs(destination);
		}
		for (final FileStatus file : files) {
			LOG.info("Find File {}", file.getPath());
			final Path destfile = new Path(destination, file.getPath().getName());
			try {
				boolean rename = retryTemplate.execute(new RetryCallback<Boolean>() {
					@Override
					public Boolean doWithRetry(RetryContext context) throws Exception {
						return fs.rename(file.getPath(), destfile);
					}
				});
				LOG.info("Moving {} to {}. status is {}", new Object[] { file.getPath(), destfile.toString(), rename });
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}
}
