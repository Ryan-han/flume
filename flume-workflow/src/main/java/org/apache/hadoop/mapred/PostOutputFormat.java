/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred;

import java.io.File;
import java.io.IOException;
import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;

public class PostOutputFormat<LogRecordKey extends WritableComparable, LogRecord>
		extends MultipleOutputFormat<LogRecordKey, LogRecord> implements
		JobConfigurable {

	static Logger log = Logger.getLogger(PostOutputFormat.class);
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy_MM_dd_HH_mm");

	private String prefix;
	private SequenceFileOutputFormat<LogRecordKey, LogRecord> output = new SequenceFileOutputFormat();

	@Override
	public void configure(JobConf job) {
		// TODO Auto-generated method stub
		FileOutputCommitter committer = (FileOutputCommitter) job
				.getOutputCommitter();
		Path tempPath = null;
		;
		if (job.get("mapred.task.id") != null) {
			TaskAttemptContext context = new TaskAttemptContext(job,
					TaskAttemptID.forName(job.get("mapred.task.id")));
			tempPath = committer.getTempTaskOutputPath(context);
			prefix = tempPath.toString();

			prefix = prefix.substring(prefix.indexOf("_r_"),
					prefix.lastIndexOf("_"));
			prefix = prefix.replace("_r_", "part-");
			log.info(" part name -- " + prefix);
		}
	}

	@Override
	protected String generateFileNameForKeyValue(LogRecordKey key,
			LogRecord chunk, String name) {

		String time = ((com.nexr.data.sdp.rolling.hdfs.LogRecordKey) key)
				.getTime();
		long t = 0;
		try {
			t = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse(time).getTime();
		} catch (ParseException e) {
			t = Long.parseLong(time);
		}
		return ((com.nexr.data.sdp.rolling.hdfs.LogRecordKey) key)
			.getDataType()
			+ File.separator
			+ sdf.format(t)
			+ File.separator + prefix;
	}

	@Override
	protected RecordWriter<LogRecordKey, LogRecord> getBaseRecordWriter(FileSystem fs,
			JobConf job, String name, Progressable progress) throws IOException {
		// TODO Auto-generated method stub
		return (RecordWriter<LogRecordKey, LogRecord>) output.getRecordWriter(
				fs, job, name, progress);
	}

}
