package com.autonavi.lbi.mr;

import com.aliyun.odps.io.TableInfo;
import com.aliyun.odps.io.TableInputFormat;
import com.aliyun.odps.io.TableOutputFormat;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.mapreduce.JobClient;
import com.aliyun.odps.mapreduce.JobConf;
import com.autonavi.lbi.MapperClass;
import com.autonavi.lbi.ReducerClass;

/**
 * 整合cluster和transit信息
 * input table s_autonavi_location_log_cluster s_autonavi_location_log_transit 
 * output table s_autonavi_location_log_cluster_info
 * run:
 * jar -libjars json.jar,esri-geometry-api.jar,odps_cluster_qj.jar,autonavi-map-1.0.jar,esri_tool.jar,jts-1.13.jar,spatial-sdk-hadoop.jar -classpath autonavi/odps_cluster_qj.jar com.autonavi.lbi.mr.LocationInfoDriver s_autonavi_location_log_cluster  s_autonavi_location_log_transit s_autonavi_location_log_cluster_info 20131101 20131101;
 * @author shuaimin.yang
 *
 */
public class LocationInfoDriver {
	public static int fieldcount;

	public static void main(String[] args) throws Exception {
		
		JobConf job = new JobConf();

		job.setMapperClass(MapperClass.class);
//	    job.setCombinerClass(GidCombiner.class);
		job.setReducerClass(ReducerClass.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setNumReduceTasks(50);

		TableInputFormat.addInput(new TableInfo(args[0], "ds=" + args[3]), job); // s_autonavi_location_log_gidindex
		TableInputFormat.addInput(new TableInfo(args[1], "ds=" + args[3]), job); // transit_gidindex
		TableOutputFormat.addOutput(new TableInfo(args[2], "ds=" + args[4]),
				job);// s_autonavi_location_log_transit
		JobClient.runJob(job);
	}
	
}
