package com.autonavi.lbi.mr;

import com.aliyun.odps.io.TableInfo;
import com.aliyun.odps.io.TableInputFormat;
import com.aliyun.odps.io.TableOutputFormat;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.mapreduce.JobClient;
import com.aliyun.odps.mapreduce.JobConf;
import com.autonavi.lbi.TransitMapperClass;
import com.autonavi.lbi.TransitReducerClass;

/**
 * 生成公交线路、公交站点的1.5km缓冲区分析的gid列表
 * input table transit_line_info_d transit_station_info_d 
 * output table transit_gidindex
 * run:
 * jar -libjars json.jar,esri-geometry-api.jar,odps_cluster_qj.jar,autonavi-map-1.0.jar,esri_tool.jar,jts-1.13.jar,spatial-sdk-hadoop.jar -classpath autonavi/odps_cluster_qj.jar com.autonavi.lbi.mr.TransitDriver transit_line_info_d transit_station_info_d transit_gidindex; 
 * @author shuaimin.yang 
 */
public class TransitDriver {
	public static int fieldcount;

	public static void main(String[] args) throws Exception {
		JobConf job = new JobConf();
		
		job.set("key.seperator", "@");
		job.setMapperClass(TransitMapperClass.class);
		job.setReducerClass(TransitReducerClass.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(50);

		TableInputFormat.addInput(new TableInfo(args[0]), job);  //transit_line
		TableInputFormat.addInput(new TableInfo(args[1]), job);  //transit_station
		TableOutputFormat.addOutput(new TableInfo(args[2]),
				job);
		JobClient.runJob(job);
	}
}
