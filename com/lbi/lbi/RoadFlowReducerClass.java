package com.autonavi.lbi;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import com.aliyun.odps.Record;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.mapreduce.ReduceContext;
import com.aliyun.odps.mapreduce.Reducer;

public class RoadFlowReducerClass extends Reducer<Text, Text> {
	private HashMap<String, Record> resultMap = new HashMap<String, Record>();

	private String keySeperator = ",";

	private Record result = null;

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
		Set<String> imei = new HashSet<String>();
		for (Text val : values) {
			if (!val.toString().equalsIgnoreCase("")) {
				imei.add(val.toString());
			}
		}		
		if (imei.size() > 0) {
			System.out.println(key.toString() + " " + imei.size());
			String[] ks = key.toString().split(":");
            result.set(0, new LongWritable(Integer.parseInt(ks[0])));
            result.set(1, new Text(ks[1]));
            result.set(2, new Text(ks[2]));
            result.set(3, new LongWritable(imei.size()));
            context.write(result);
		}

	}

}
