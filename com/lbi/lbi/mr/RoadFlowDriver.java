package com.lbi.lbi.mr;

import com.aliyun.odps.io.TableInfo;
import com.aliyun.odps.io.TableInputFormat;
import com.aliyun.odps.io.TableOutputFormat;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.mapreduce.JobClient;
import com.aliyun.odps.mapreduce.JobConf;
import com.lbi.lbi.RoadFlowReducerClass;
import com.lbi.lbi.RoadFlowMapperClass;

/**
 * 5城市的公交关联的道路表road_fivecities road_id,mesh,geometry_wkt
 * 在map setup中对road_fivecitites的geometry进行索引
 * 日志坐标按给出的范围，查询最近的road，输出imei和road 标识
 * road_id,mesh,hour,flow_size
 * input table s_lbi_address_location_log 
 * output table roadflow
 * run:
 * jar -resources roadindex -libjars json.jar,esri-geometry-api.jar,odps_cluster_qj.jar,lbi-map-1.0.jar,esri_tool.jar,jts-1.13.jar,spatial-sdk-hadoop.jar -classpath lbi/odps_cluster_qj.jar com.lbi.lbi.mr.RoadFlowDriver s_lbi_address_location_log roadflow 20140506; 
 * @author shuaimin.yang 
 */
public class RoadFlowDriver {
	public static int fieldcount;

	public static void main(String[] args) throws Exception {
		JobConf job = new JobConf();
		
		job.set("key.seperator", "@");
		job.setMapperClass(RoadFlowMapperClass.class);
		job.setReducerClass(RoadFlowReducerClass.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(100);

		TableInputFormat.addInput(new TableInfo(args[0],"ds=" + args[2]), job);  //location
		TableOutputFormat.addOutput(new TableInfo(args[1],"ds=" + args[2]), job); //roadflow
		JobClient.runJob(job);
	}
}
