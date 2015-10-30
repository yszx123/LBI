package com.autonavi.lbi.mr;

import java.io.IOException;

import com.aliyun.odps.io.TableInfo;
import com.aliyun.odps.io.TableInputFormat;
import com.aliyun.odps.io.TableOutputFormat;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.mapreduce.JobClient;
import com.aliyun.odps.mapreduce.JobConf;
import com.autonavi.lbi.ImeiShareMapperFilter;
import com.autonavi.lbi.ImeiShareReducerFilter;
//@author bailu
public class ImeiFilter {
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws Exception {
		JobConf job=new JobConf();
		job.setMapperClass(ImeiShareMapperFilter.class);
		job.setReducerClass(ImeiShareReducerFilter.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(100);
		TableInputFormat.addInput(new TableInfo(args[0],"ds="+args[2]),job);
		TableOutputFormat.addOutput(new TableInfo(args[1],"ds="+args[2]), job);
		JobClient.runJob(job);
	}

}
;