package com.autonavi.lbi.mr;

import com.aliyun.odps.io.TableInfo;
import com.aliyun.odps.io.TableInputFormat;
import com.aliyun.odps.io.TableOutputFormat;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.mapreduce.JobClient;
import com.aliyun.odps.mapreduce.JobConf;
import com.autonavi.lbi.ClusterMapperClass;
import com.autonavi.lbi.ClusterReducerClass;
import com.autonavi.util.TimeUtil;

/**
 * 通过定位日志聚类形成人流量密集面及其索引表
 * input table s_autonavi_address_location_log_utf_8
 * output table s_autonavi_location_log_gidindex s_autonavi_location_log_cluster
 * run:
 * jar -libjars json.jar,esri-geometry-api.jar,odps_cluster_qj.jar,autonavi-map-1.0.jar,esri_tool.jar,jts-1.13.jar,spatial-sdk-hadoop.jar -resources wkt.txt -classpath autonavi/odps_cluster_qj.jar com.autonavi.lbi.mr.ClusterDriver s_autonavi_address_location_log_utf_8 s_autonavi_location_log_gidindex s_autonavi_location_log_cluster 25 20140116 20140116; 
 * @author shuaimin.yang *
 */
public class ClusterDriver {
	
	  public static int fieldcount;
	  public static void main(String[] args) throws Exception {
		    if (args.length != 6) {
		      System.err.println("Usage: wordcount <in_table> <out_table>");
		      System.exit(2);
		    }

		    JobConf job = new JobConf();
		    job.setInt("simplify.radius", args[3]==null?30:Integer.parseInt((args[3])));
		    job.set("key.seperator", "@");
		    job.setMapperClass(ClusterMapperClass.class);
//		    job.setCombinerClass(SumCombiner.class);
		    job.setReducerClass(ClusterReducerClass.class);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(Text.class);
		    job.setNumReduceTasks(500);
		    String inDate = args[4];
		    boolean hasData = TableJudgment.judgeDateCount(inDate,args[0],"autonavi_ods");
			while(!hasData)
			{
				 inDate = TimeUtil.getPreDayTimeStamp(inDate, 7);
				 hasData = TableJudgment.judgeDateCount(inDate,args[0],"autonavi_ods");
				 System.out.println("业务日期数据无,取前一天试一下!");
			}
		    TableInputFormat.addInput(new TableInfo(args[0],"ds=" + inDate), job);  //location
		    TableOutputFormat.addOutput(new TableInfo(args[1],"ds=" + args[5]), job);//gidindex
            TableOutputFormat.addOutput(new TableInfo(args[2],"ds=" + args[5]), "cluster", job); //cluster
//            TableOutputFormat
		    JobClient.runJob(job);
		  }

	  /*private void readTable(){
		  TableInfo  ti = context.getCacheTableInfo("transit_line_info_d");
			String[] fields = ti.getPartSpecMap().keySet().toArray(new String[0]);
			for(String f : fields){
				System.out.println("f:"+f);	
			}	
	  }*/
	  
}
