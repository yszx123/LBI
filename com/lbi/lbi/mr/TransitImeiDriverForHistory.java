package com.lbi.lbi.mr;

import com.aliyun.odps.io.TableInfo;
import com.aliyun.odps.io.TableInputFormat;
import com.aliyun.odps.io.TableOutputFormat;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.mapreduce.JobClient;
import com.aliyun.odps.mapreduce.JobConf;
import com.lbi.lbi.TransitImeiMapper;
import com.lbi.lbi.TransitImeiReducer;
import com.lbi.util.TimeUtil;

/**
 * 根据定位日志的imei和公交线路、公交站点的1.5km半径缓冲区分析得到用户与公交关系表
 * input table s_lbi_address_location_log_utf_8 transit_gidindex 
 * output table imei_gidindex
 * run:
 * jar -libjars json.jar,esri-geometry-api.jar,odps_cluster_qj.jar,lbi-map-1.0.jar,esri_tool.jar,jts-1.13.jar,spatial-sdk-hadoop.jar -classpath lbi/odps_cluster_qj.jar com.lbi.lbi.mr.TransitImeiDriver s_lbi_address_location_log_utf_8  transit_gidindex imei_gidindex 20131101 20131101;
 * @author shuaimin.yang 
 */
public class TransitImeiDriverForHistory {
	public static int fieldcount;

	public static void main(String[] args) throws Exception {
		
		JobConf job = new JobConf();

		job.set("key.seperator", "@");		
		job.setMapperClass(TransitImeiMapper.class);
		job.setReducerClass(TransitImeiReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		String current = args[3];
		int d_size = Integer.parseInt(args[4]);
		String[] parts = new String[d_size];
		String date = "";
		for(int i=0;i<d_size;i++)
		{
		    String pre = TimeUtil.getPreDayTimeStamp(current, 1);
		    parts[i] = pre;
		    date = date + "," + pre;
		    current = pre;
		}
		job.setNumReduceTasks(d_size*50);
		job.set("output.parts", date.substring(1, date.length()));
		for(String p : parts){
			TableInputFormat.addInput(new TableInfo(args[0],"ds=" + p), job);  //location
		}
//		TableInputFormat.addInput(new TableInfo(args[1]), job);  //transit_gidindex
		for(String d : parts){
			TableOutputFormat.addOutput(new TableInfo(args[2],"ds=" + d), d,  //imei_gidindex
					job);
		}
		JobClient.runJob(job);
		
	}
}
