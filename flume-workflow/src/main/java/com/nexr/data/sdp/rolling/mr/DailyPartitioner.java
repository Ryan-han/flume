package com.nexr.data.sdp.rolling.mr;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

import com.nexr.data.sdp.rolling.hdfs.LogRecord;

public class DailyPartitioner<K, V> implements
		Partitioner<Text, LogRecord> {

	public void configure(JobConf arg0) {
	}

	public int getPartition(Text key, LogRecord chunl,
			int numReduceTasks) {
		return key.toString().hashCode()
		& Integer.MAX_VALUE % numReduceTasks;
	}

}
