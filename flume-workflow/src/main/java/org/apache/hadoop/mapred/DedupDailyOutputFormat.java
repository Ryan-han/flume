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
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;
import com.nexr.data.sdp.rolling.hdfs.LogRecord;
import com.nexr.data.sdp.rolling.hdfs.LogRecordKey;

public class DedupDailyOutputFormat<Text extends WritableComparable, LogRecord>
		extends MultipleOutputFormat<Text, LogRecord> implements
		JobConfigurable {

	static Logger log = Logger.getLogger(DedupDailyOutputFormat.class);
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy_MM_dd");

	private String prefix;
	private MapFileOutputFormat output = new MapFileOutputFormat();

	public void configure(JobConf job) {
		FileOutputCommitter committer = (FileOutputCommitter) job
				.getOutputCommitter();
		Path tempPath = null;
		;
		if (job.get("mapred.task.id") != null) {
			TaskAttemptContext context = new TaskAttemptContext(job,
					TaskAttemptID.forName(job.get("mapred.task.id")));
			tempPath = committer.getTempTaskOutputPath(context);
			prefix = tempPath.toString();
			
			prefix = prefix.substring(prefix.indexOf("_r_"), prefix.lastIndexOf("_"));
			prefix = prefix.replace("_r_", "part-");
		}
		
		log.info(" part name -- " + prefix);
		
	}
	
	@Override
	public void checkOutputSpecs(FileSystem ignored, JobConf job) 
    throws FileAlreadyExistsException, 
           InvalidJobConfException, IOException {
		
	}
	
	@Override
	protected String generateFileNameForKeyValue(Text key,
			LogRecord value, String name) {
		System.out.println(value);
		String dataType = key.toString().substring(key.toString().lastIndexOf("_")+1, key.toString().length());
		String tmp = key.toString().substring(0, key.toString().lastIndexOf("_"));
		String timeStamp = tmp.substring(tmp.lastIndexOf("_")+1, tmp.length());
		long t = 0;
		try {
			t = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse(timeStamp).getTime();
		} catch (ParseException e) {
			t = Long.parseLong(timeStamp);
		}
		return dataType+File.separator+ sdf.format(new Date(t))+File.separator+prefix;
	}

	@Override
	protected RecordWriter<Text, LogRecord> getBaseRecordWriter(
			FileSystem fs, JobConf job, String name, Progressable progress)
			throws IOException {

		return (RecordWriter<Text, LogRecord>) output.getRecordWriter(
				fs, job, name, progress);
	}
}
