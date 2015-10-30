package com.autonavi.lbi;

import java.io.IOException;

import com.aliyun.odps.Record;
import com.aliyun.odps.io.IntWritable;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.mapreduce.MapContext;
import com.aliyun.odps.mapreduce.Mapper;

public class GidMapper extends Mapper<Text, Text> {
	private String log_table_name = "location";
	private Text word = new Text();

	private String keySeperator = "@";
	@Override
	public void map(LongWritable recordNum, Record record,
			MapContext<Text, Text> context) throws IOException,
			InterruptedException {
		Text k = new Text();
		String tableName = context.getInputTableInfo().getTableName();
		if (tableName.indexOf(log_table_name) != -1) {
			String gid = record.get("gid").toString();
			String cluster_id = record.get("cluster_id").toString();
			k.set(gid);
			word.set(new Text("location" + keySeperator + cluster_id));
		} else {
			String gid = record.get("gid").toString();
			String line_ids = record.get("line_ids").toString();
			String station_ids = record.get("station_ids").toString();
			k.set(gid);
			word.set(new Text("transit" + keySeperator + line_ids + keySeperator
					+ station_ids));
		}
		context.write(k, word);
	}
}
