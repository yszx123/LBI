package com.autonavi.lbi.mr;

import java.io.IOException;

import com.aliyun.odps.io.TableInfo;
import com.aliyun.odps.io.TableInputFormat;
import com.aliyun.odps.io.TableOutputFormat;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.mapreduce.JobClient;
import com.aliyun.odps.mapreduce.JobConf;
import com.autonavi.lbi.ImeiShareMapper;
import com.autonavi.lbi.ImeiShareReducer;
//@author bailu
public class ImeiShareDriver {


	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws Exception {
		JobConf job=new JobConf();
		
		job.setMapperClass(ImeiShareMapper.class);
		job.setReducerClass(ImeiShareReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(100);
		//多级分区的添加，外部ODPS表操作
//		TableInputFormat.addInput(new TableInfo(args[0],"ds="+args[2]),job);
//		TableOutputFormat.addOutput(new TableInfo(args[1],"ds="+args[3]+"/day="+args[4]),true, job);
//		一级分区的添加
//		TableInputFormat.addInput(new TableInfo(args[0],"ds="+args[2]),job);
//		TableOutputFormat.addOutput(new TableInfo(args[1],"ds="+args[2]), job);
//		生产环境job操作
		TableInputFormat.addInput(new TableInfo(args[0],"ds="+args[2]),job);
		TableOutputFormat.addOutput(new TableInfo(args[1]),job);
		JobClient.runJob(job);
	}

}
;