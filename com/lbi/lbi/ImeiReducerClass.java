package com.autonavi.lbi;

import java.io.IOException;
import java.util.HashSet;

import com.aliyun.odps.Record;
import com.aliyun.odps.io.DoubleWritable;
import com.aliyun.odps.io.IntWritable;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.mapreduce.ReduceContext;
import com.aliyun.odps.mapreduce.Reducer;

public class ImeiReducerClass extends Reducer<Text, Text> {

	String keySeperator = "";
	Record result = null;
	@Override
	protected void setup(ReduceContext<Text, Text> context) throws IOException,
			InterruptedException {
		keySeperator = context.getConfiguration().get("key.seperator");
		result = context.createOutputRecord();
	}
	
	@Override
	public void reduce(Text key, Iterable<Text> values,
			ReduceContext<Text, Text> context) throws IOException,
			InterruptedException {
		String[] imei_gid = key.toString().split(keySeperator);
		int count = 0;
//		HashSet<String> biz = new HashSet<String>();
		String biz = "";
		for (Text val : values) {
			count++;
			biz = val.toString();
		}
		String imei = imei_gid[0];
		String gid = imei_gid[1];
//		for(String b : biz)
//		{
			String[] bs = biz.split(keySeperator);
			result.set(0, new Text(imei));// imei
			result.set(1, new Text(gid));// gid
			result.set(2, new Text(bs[0]));// gid_1
			result.set(3, new Text(bs[1]));// gid_2
//			result.set(4, new Text(bs[0]));//biz_id
			result.set(4, new IntWritable(count));//频次
			context.write(result);
//		}		
	}
}
