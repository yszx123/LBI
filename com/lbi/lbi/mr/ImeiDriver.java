package com.lbi.lbi.mr;

import com.aliyun.odps.io.TableInfo;
import com.aliyun.odps.io.TableInputFormat;
import com.aliyun.odps.io.TableOutputFormat;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.mapreduce.JobClient;
import com.aliyun.odps.mapreduce.JobConf;
import com.lbi.lbi.ImeiMapperClass;
import com.lbi.lbi.ImeiReducerClass;
import com.lbi.util.TimeUtil;

/**
 * for DMP use
 * imei的gid 商圈id关联列表
 * input table s_lbi_address_location_log 
 * resource: business_region_info(biz_id,geom),black_imei 
 * output table s_autoanvi_imei_gid 
 * run:
 * jar -libjars json.jar,esri-geometry-api.jar,odps_cluster_qj.jar,lbi-map-1.0.jar,esri_tool.jar,jts-1.13.jar,spatial-sdk-hadoop.jar -classpath lbi/odps_cluster_qj.jar com.lbi.lbi.mr.TransitDriver transit_line_info_d transit_station_info_d transit_gidindex; 
 * @author shuaimin.yang 
 */
public class ImeiDriver {
	public static int fieldcount;

	public static void main(String[] args) throws Exception {
		JobConf job = new JobConf();
		
		job.set("key.seperator", "@");
		job.setMapperClass(ImeiMapperClass.class);
		job.setReducerClass(ImeiReducerClass.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		String current = args[2];
		int d_size = Integer.parseInt(args[3]);
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
		TableInputFormat.addInput(new TableInfo(args[0]), job);  //transit_line
		TableOutputFormat.addOutput(new TableInfo(args[1],"dt=" + parts[parts.length-1] + "-" + parts[0]),
				job);
		JobClient.runJob(job);
	}
}

