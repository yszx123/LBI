package com.autonavi.lbi.mr;

import com.aliyun.odps.io.TableInfo;
import com.aliyun.odps.io.TableInputFormat;
import com.aliyun.odps.io.TableOutputFormat;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.mapreduce.JobClient;
import com.aliyun.odps.mapreduce.JobConf;
import com.autonavi.lbi.GidMapper;
import com.autonavi.lbi.GidReducer;

/**
 * 定位聚类面与公交信息的融合
 * input table s_autonavi_location_log_gidindex transit_gidindex inputds outputds
 * output table s_autonavi_location_log_transit [cluster_id line_ids transit_ids] 
 * run:
 * jar -libjars json.jar,esri-geometry-api.jar,odps_cluster_qj.jar,autonavi-map-1.0.jar,esri_tool.jar,jts-1.13.jar,spatial-sdk-hadoop.jar -classpath autonavi/odps_cluster_qj.jar com.autonavi.lbi.mr.LocationTransitDriver s_autonavi_location_log_gidindex  transit_gidindex s_autonavi_location_log_transit 20131101 20131101;
 * @author shuaimin.yang
 * 
 */
public class LocationTransitDriver {
	public static int fieldcount;

	public static void main(String[] args) throws Exception {
		if (args.length != 5) {
			System.err.println("Usage: LocationTransitDriver <in_table> <out_table>");
			System.exit(2);
		}

		JobConf job = new JobConf();

		job.setMapperClass(GidMapper.class);
//	    job.setCombinerClass(GidCombiner.class);
		job.setReducerClass(GidReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setNumReduceTasks(50);

		TableInputFormat.addInput(new TableInfo(args[0], "ds=" + args[3]), job); // s_autonavi_location_log_gidindex
		TableInputFormat.addInput(new TableInfo(args[1]), job); // transit_gidindex
		TableOutputFormat.addOutput(new TableInfo(args[2], "ds=" + args[4]),
				job);// s_autonavi_location_log_transit
		JobClient.runJob(job);
	}
}
