package com.lbi.lbi;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.aliyun.odps.Record;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.mapreduce.ReduceContext;
import com.aliyun.odps.mapreduce.Reducer;

public class RoadFlowCountReducerClass extends Reducer<Text, Text> {

	private Record result = null;
	
	@Override
	protected void setup(ReduceContext<Text, Text> context) throws IOException,InterruptedException {
		result = context.createOutputRecord();
		
	}

	@Override
	public void reduce(Text key, Iterable<Text> values,
			ReduceContext<Text, Text> context) throws IOException,
			InterruptedException {
		Map<String,Set<String>> map = new HashMap<String,Set<String>>();
		for (Text val : values) {
			String[] str = val.toString().split(":::");
			if(str.length==2){
				if(map.get(str[0])!=null){
					map.get(str[0]).add(str[1]);
				}else{
					Set<String> imei = new HashSet<String>();
					imei.add(str[1]);
					map.put(str[0], imei);
				}
			}
		}		
		String[] ks = key.toString().split(":");
		result.set(0, new LongWritable(Integer.parseInt(ks[0])));
		result.set(1, new Text(ks[1]));
        result.set(2, new Text(ks[2]));
        int index = 3;
        if(map.size()>0 && context.getConfiguration().get("key.dist.times")!=null){
			int times = Integer.parseInt(context.getConfiguration().get("key.dist.times"));
			for(int i=1;i<=times;i++){
				String dist = context.getConfiguration().get("key.dist."+i);
				if(map.get(dist)!=null){
					result.set(index++, new LongWritable(map.get(dist).size()));
				}else{
					result.set(index++, new LongWritable(0));
				}
			}
			context.write(result);
        }
	}
	
}
