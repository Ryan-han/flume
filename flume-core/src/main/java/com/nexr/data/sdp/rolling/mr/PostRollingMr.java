package com.nexr.data.sdp.rolling.mr;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.PostOutputFormat;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.nexr.data.sdp.rolling.hdfs.LogRecord;
import com.nexr.data.sdp.rolling.hdfs.LogRecordKey;

public class PostRollingMr extends Configured implements Tool {

	static class UniqueKeyReduce extends MapReduceBase implements
			Reducer<LogRecordKey, LogRecord, LogRecordKey, LogRecord> {

		/**
		 * Outputs exactly one value for each key; this suppresses duplicates
		 */
		@Override
		public void reduce(LogRecordKey key, Iterator<LogRecord> vals,
				OutputCollector<LogRecordKey, LogRecord> out, Reporter r)
				throws IOException {
			LogRecord i = vals.next();
			out.collect(key, i);
			int dups = 0;
			while (vals.hasNext()) {
				vals.next();
				dups++;
			}
			r.incrCounter("app", "duplicate Event", dups);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		System.out.println("Input " + args[0]);
		JobConf jobConf = new JobConf(getConf(), PostRollingMr.class);
		jobConf.setInputFormat(SequenceFileInputFormat.class);
		jobConf.setMapperClass(IdentityMapper.class);
		jobConf.setReducerClass(UniqueKeyReduce.class);
		jobConf.setJobName("PostRollong");
		jobConf.setPartitionerClass(PostPartitioner.class);
		jobConf.setOutputFormat(PostOutputFormat.class);
		jobConf.setOutputKeyClass(LogRecordKey.class);
		jobConf.setOutputValueClass(LogRecord.class);
		jobConf.setNumReduceTasks(3);
		FileInputFormat.setInputPaths(jobConf, args[0]);
		FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));
		JobClient.runJob(jobConf);
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new PostRollingMr(),
	        args);
	    System.exit(res);
	  }
}
