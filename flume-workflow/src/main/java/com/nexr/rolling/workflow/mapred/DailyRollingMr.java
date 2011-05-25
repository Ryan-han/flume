package com.nexr.rolling.workflow.mapred;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.nexr.data.sdp.rolling.hdfs.LogRecord;
import com.nexr.data.sdp.rolling.hdfs.LogRecordKey;


public class DailyRollingMr extends Configured implements Tool {

	static class DailyMapper<LogRecordKey, LogRecord> extends MapReduceBase implements
			Mapper<LogRecordKey, LogRecord, Text, LogRecord> {

		@Override
		public void map(LogRecordKey key, LogRecord val,
				OutputCollector<Text, LogRecord> output, Reporter reporter)
				throws IOException {
			String mapKey = ((com.nexr.data.sdp.rolling.hdfs.LogRecordKey) key).getLogId();
			String time = ((com.nexr.data.sdp.rolling.hdfs.LogRecordKey) key).getTime();
			String type = ((com.nexr.data.sdp.rolling.hdfs.LogRecordKey) key).getDataType();
			String lkey = mapKey+"_"+time+"_"+type;
			Text lid = new Text(lkey.getBytes());
			
			output.collect(lid, val);
		}
	}

	static class UniqueKeyReduce extends MapReduceBase implements
			Reducer<Text, LogRecord, Text, LogRecord> {

		/**
		 * Outputs exactly one value for each key; this suppresses duplicates
		 */
		@Override
		public void reduce(Text key, Iterator<LogRecord> vals,
				OutputCollector<Text, LogRecord> out, Reporter r)
				throws IOException {
			LogRecord i = vals.next();

			// out.collect(new Text(key.getKey()), i);
			out.collect(key, i);
			int dups = 0;
			while (vals.hasNext()) {
				vals.next();
				dups++;
			}
			r.incrCounter("app", "duplicate chunks", dups);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		JobConf jobConf = new JobConf(getConf(), DailyRollingMr.class);
		jobConf.setInputFormat(SequenceFileInputFormat.class);
		jobConf.setMapperClass(DailyMapper.class);
		jobConf.setMapOutputValueClass(LogRecord.class);
		jobConf.setMapOutputKeyClass(Text.class);
		jobConf.setReducerClass(UniqueKeyReduce.class);
		jobConf.setJobName("DailyRolling");
		jobConf.setPartitionerClass(DailyPartitioner.class);
		jobConf.setOutputFormat(org.apache.hadoop.mapred.DailyOutputFormat.class);
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(LogRecord.class);
		jobConf.setNumReduceTasks(3);
		FileInputFormat.setInputPaths(jobConf, args[0]);
		FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));
		JobClient.runJob(jobConf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new DailyRollingMr(),
				args);
		System.exit(res);
	}

}
