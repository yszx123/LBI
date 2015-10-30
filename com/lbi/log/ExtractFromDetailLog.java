package com.autonavi.log;

import com.aliyun.odps.io.TableInfo;
import com.aliyun.odps.io.TableInputFormat;
import com.aliyun.odps.io.TableOutputFormat;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.mapreduce.JobClient;
import com.aliyun.odps.mapreduce.JobConf;

/**
 * 定位日志精简表提取程序
 * input table s_autonavi_location_log_gidindex transit_gidindex inputds outputds
 * output table s_autonavi_location_log_transit [cluster_id line_ids transit_ids] 
 * run:
 * jar -libjars json.jar,esri-geometry-api.jar,odps_cluster_qj.jar,autonavi-map-1.0.jar,esri_tool.jar,jts-1.13.jar,spatial-sdk-hadoop.jar -classpath autonavi/odps_cluster_qj.jar com.autonavi.lbi.mr.LocationTransitDriver s_autonavi_location_log_gidindex  transit_gidindex s_autonavi_location_log_transit 20131101 20131101;
 * @author shuaimin.yang
 * 
 */
public class ExtractFromDetailLog {
	public static int fieldcount;

	public static void main(String[] args) throws Exception {
		JobConf job = new JobConf();

		job.setMapperClass(LogMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setNumReduceTasks(50);

		TableInputFormat.addInput(new TableInfo(args[0], "dt=" + args[2]), job); 
		TableOutputFormat.addOutput(new TableInfo(args[1], "ds=" + args[2]),
				job);// s_autonavi_location_log_transit
		JobClient.runJob(job);
	}
}
