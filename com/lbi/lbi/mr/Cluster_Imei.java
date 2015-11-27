package com.lbi.lbi.mr;

import com.aliyun.odps.io.TableInfo;
import com.aliyun.odps.io.TableInputFormat;
import com.aliyun.odps.io.TableOutputFormat;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.mapreduce.JobClient;
import com.aliyun.odps.mapreduce.JobConf;
//import com.lbi.lbi.ClusterImeiCombiner;
import com.lbi.lbi.ClusterImeiMapperClass;
import com.lbi.lbi.ClusterImeiReducerClass;
import com.lbi.util.TimeUtil;

public class Cluster_Imei {
	
	 public static void main(String[] args) throws Exception {
		    if (args.length != 5) {
		      System.err.println("Usage: wordcount <in_table> <out_table>");
		      System.exit(2);
		    }

		    JobConf job = new JobConf();
		    
		    job.setMapperClass(ClusterImeiMapperClass.class);
//		    job.setCombinerClass(ClusterImeiCombiner.class);
		    job.setReducerClass(ClusterImeiReducerClass.class);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(Text.class);
//		    job.setNumReduceTasks(100);
		    String inDate = args[3];
		    boolean hasData = TableJudgment.judgeDateCount(inDate,args[0],"lbi_ods");
			while(!hasData)
			{
				 inDate = TimeUtil.getPreDayTimeStamp(inDate, 7);
				 hasData = TableJudgment.judgeDateCount(inDate,args[0],"lbi_ods");
				 System.out.println("业务日期数据无,取前一天试一下!");
			}
		    TableInputFormat.addInput(new TableInfo(args[0],"ds=" + args[3]), job);  //location
		    TableInputFormat.addInput(new TableInfo(args[1],"ds=" + args[3]), job);//gidindex
		    TableOutputFormat.addOutput(new TableInfo(args[2],"ds=" + args[4]), job); //cluster
		    JobClient.runJob(job);
		  }
}
