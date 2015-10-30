package com.autonavi.log;

import java.io.IOException;
import java.util.ArrayList;

import com.aliyun.odps.Record;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.mapreduce.ReduceContext;
import com.aliyun.odps.mapreduce.Reducer;
import com.autonavi.odps.udf.LngLatOperator;

public class SameLogReducer extends Reducer<Text, Text>{
	private Record result = null;
	private String keySeperator = "@";
	LngLatOperator oper = new LngLatOperator();
	@Override
	protected void setup(ReduceContext<Text, Text> context) throws IOException,
			InterruptedException {
		result = context.createOutputRecord();
	}

	@Override
	public void reduce(Text key, Iterable<Text> values,
			ReduceContext<Text, Text> context) throws IOException,
			InterruptedException {
		long count = 0;
		ArrayList<Double> lls = new ArrayList<Double>();
		for (Text val : values) {
			String[] xy = val.toString().split(keySeperator);
			lls.add(Double.parseDouble(xy[0]));
			lls.add(Double.parseDouble(xy[1]));
			count++;
		}
		if(count==2)
		{			
		   double dist = oper.evaluate(lls.get(3),lls.get(2),lls.get(1),lls.get(0));
		   if(dist < 10)
		   {
			    String[] keys = key.toString().split(keySeperator);
			    result.set(0, new Text(keys[0]));// accesstime				
			    result.set(1, new Text(keys.length>1?keys[1]:""));// imei
			    result.set(2, new Text(keys.length==3?keys[2]:""));// imsi
			    context.write(result);
		   }
		}else if(count > 2){
			
		}
	}
}
