package com.autonavi.lbi.mr;

import com.aliyun.odps.io.TableInfo;
import com.aliyun.odps.io.TableInputFormat;
import com.aliyun.odps.io.TableOutputFormat;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.mapreduce.JobClient;
import com.aliyun.odps.mapreduce.JobConf;
import com.autonavi.lbi.TransitImeMapperClass;
import com.autonavi.lbi.TransitImeReducerClass;
import com.autonavi.util.TimeUtil;

/**
 * 根据定位日志的imei和公交线路、公交站点的1.5km半径缓冲区分析得到用户与公交关系表
 * input table s_autonavi_address_location_log transit_gidindex 
 * output table imei_gidindex
 * run:
 * jar -libjars json.jar,esri-geometry-api.jar,odps_cluster_qj.jar,autonavi-map-1.0.jar,esri_tool.jar,jts-1.13.jar,spatial-sdk-hadoop.jar -classpath autonavi/odps_cluster_qj.jar com.autonavi.lbi.mr.TransitImeiDriver s_autonavi_address_location_log_utf_8  transit_gidindex imei_gidindex 20131101 20131101;
 * @author shuaimin.yang 
 */
public class TransitImeiDriver {
	public static int fieldcount;

	public static void main(String[] args) throws Exception {
		
		JobConf job = new JobConf();

		job.set("key.seperator", "@");
		
		job.setMapperClass(TransitImeMapperClass.class);
		job.setReducerClass(TransitImeReducerClass.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);	
		String inDate = args[3];
		String outDate = args[4];
		
		job.setNumReduceTasks(100);
		
		boolean hasData = TableJudgment.judgeDateCount(inDate,args[0],"autonavi_ods");
		while(!hasData)
		{
			 inDate = TimeUtil.getPreDayTimeStamp(inDate, 7);
			 hasData = TableJudgment.judgeDateCount(inDate,args[0],"autonavi_ods");
			 System.out.println("业务日期数据无,取上星期该天试一下!");
		}
		job.set("key.date", inDate);
		TableInputFormat.addInput(new TableInfo(args[0],"ds=" + inDate), job);
		TableInputFormat.addInput(new TableInfo(args[1]), job);  //transit_gidindex
		TableOutputFormat.addOutput(new TableInfo(args[2],"ds=" + outDate),  //imei_gidindex
				job);
		JobClient.runJob(job);
	}
}
