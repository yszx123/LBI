package com.lbi.lbi;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import com.aliyun.odps.Record;
import com.aliyun.odps.io.DoubleWritable;
import com.aliyun.odps.io.IntWritable;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.mapreduce.ReduceContext;
import com.aliyun.odps.mapreduce.Reducer;
import com.lbi.util.StringUtil;

public class ReducerClass  extends Reducer<Text, Text> {
	private String keySeperator = "@";
	private Record result = null;
	public void reduce(Text key, Iterable<Text> values,
			ReduceContext<Text, Text> context) throws IOException,
			InterruptedException {
		String transits;
		Set<String> lineSet = new HashSet<String>();
		Set<String> stopSet = new HashSet<String>();
		String coords = "";
		int size = 0;
		String time = "";
		String dimension = "";
		double radius = 0.0;
		for (Text val : values) {
			transits = val.toString();
			String[] strs = transits.split(keySeperator);
			if(strs[0].equalsIgnoreCase("location")){
				coords = strs[1];
				size = Integer.parseInt(strs[2]);
				time = strs[3];
				dimension = strs[4];
				radius = Double.parseDouble(strs[5]);
			}else{
				String[] line_ids = strs[1].split(",");
				for(String l : line_ids){
					lineSet.add(l.trim());
				}
				if(strs.length == 3){
					String[] stop_ids = strs[2].split(",");
					for(String l : stop_ids){
						stopSet.add(l.trim());
					}
				}				
			}			
		}
		if(lineSet.size() > 0 || stopSet.size() > 0){
			String lineids = lineSet.toString().substring(1,
					lineSet.toString().length() - 1);
			String stopids = "";
			if(stopSet.size() > 0){
				stopids = stopSet.toString().substring(1,	stopSet.toString().length() - 1);
			}
			result.set(0, key);
			result.set(1, new Text(coords));
			result.set(2, new IntWritable(size));
			result.set(3, new Text(time));
			result.set(4, new Text(dimension));
			result.set(5, new DoubleWritable(radius));
			result.set(6, new Text(lineids.replaceAll(" ", "")));
			result.set(7, new Text(stopids.replaceAll(" ", "")));
			context.write(result);
		}		
	}
	

	@Override
	protected void setup(ReduceContext<Text, Text> context) throws IOException,
			InterruptedException {
		result = context.createOutputRecord();
	}
}
