package com.lbi.log;

import java.io.IOException;
import java.util.HashSet;

import com.aliyun.odps.Record;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.mapreduce.MapContext;
import com.aliyun.odps.mapreduce.Mapper;

public class SameLogMapper extends Mapper<Text, Text>{
	private Text word = new Text();
	private Text grid = new Text();
	private String keySeperator = "@";
	@Override
	public void map(LongWritable recordNum, Record record,
			MapContext<Text, Text> context) throws IOException,
			InterruptedException {
		String lon = record.get("lon").toString();
		String lat = record.get("lat").toString();		
		String accesstime = record.get("accesstime").toString();
		String imei = record.get("imei").toString();
		String imsi = record.get("imsi").toString();
		word.set(lon+keySeperator+lat );
		grid.set(accesstime+keySeperator+imei+keySeperator+imsi);
		context.write(grid, word);
	}
}
