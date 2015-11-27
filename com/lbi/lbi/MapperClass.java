package com.lbi.lbi;

import java.io.IOException;

import com.aliyun.odps.Record;
import com.aliyun.odps.io.DoubleWritable;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.mapreduce.MapContext;
import com.aliyun.odps.mapreduce.Mapper;

public class MapperClass extends Mapper<Text, Text> {
	private String log_table_name = "log_cluster";
	private Text word = new Text();

	private String keySeperator = "@";
	@Override
	public void map(LongWritable recordNum, Record record,
			MapContext<Text, Text> context) throws IOException,
			InterruptedException {
		Text k = new Text();
		String cluster_id = "";
		String tableName = context.getInputTableInfo().getTableName();
		if (tableName.indexOf(log_table_name) != -1) {
			cluster_id = record.get("cluster_id").toString();
			String geom = record.get("coords").toString();
			long size = ((LongWritable)record.get("cluster_size")).get();
			String time = record.get("time").toString();
			String dimension = record.get("dimension").toString();
			double radius = ((DoubleWritable)record.get("radius")).get();
			word.set(new Text("location" + keySeperator + geom + keySeperator + size + keySeperator 
			+ time + keySeperator + dimension + keySeperator + radius));
		} else {
			cluster_id = record.get("cluster_id").toString();
			String lid = record.get("line_ids").toString();
			String sid = record.get("station_ids").toString();			
			word.set(new Text("transit" + keySeperator + lid + keySeperator + sid));
		}
		k.set(cluster_id);
		context.write(k, word);
	}
}
