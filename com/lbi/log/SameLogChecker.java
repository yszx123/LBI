package com.autonavi.log;

import com.aliyun.odps.io.TableInfo;
import com.aliyun.odps.io.TableInputFormat;
import com.aliyun.odps.io.TableOutputFormat;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.mapreduce.JobClient;
import com.aliyun.odps.mapreduce.JobConf;

public class SameLogChecker {

	
	public static int fieldcount;

	public static void main(String[] args) throws Exception {
		JobConf job = new JobConf();

		job.setMapperClass(SameLogMapper.class);
		job.setReducerClass(SameLogReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);		
		job.setNumReduceTasks(50);

		TableInputFormat.addInput(new TableInfo(args[0], "ds=" + args[3]), job); 
		TableInputFormat.addInput(new TableInfo(args[1], "ds=" + args[3]), job); 
		TableOutputFormat.addOutput(new TableInfo(args[2]),job);// s_autonavi_location_log_transit
		JobClient.runJob(job);
	}
}
