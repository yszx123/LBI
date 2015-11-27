package com.lbi.lbi;

import java.io.IOException;
import java.util.ArrayList;

import com.aliyun.odps.Record;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.mapreduce.ReduceContext;
import com.aliyun.odps.mapreduce.Reducer;

public class TransitReducerClass extends Reducer<Text, Text> {
	private Record result = null;

	private String keySeperator = ",";

	@Override
	protected void setup(ReduceContext<Text, Text> context) throws IOException,
			InterruptedException {
		result = context.createOutputRecord();
	}

	@Override
	public void reduce(Text key, Iterable<Text> values,
			ReduceContext<Text, Text> context) throws IOException,
			InterruptedException {
		ArrayList<String> lines = new ArrayList<String>();
		ArrayList<String> stations = new ArrayList<String>();
		String[] gids = key.toString().split(",");
		for (Text val : values) {
			String[] rs = val.toString().split(keySeperator);
			int type = Integer.parseInt(rs[1]);
			if (type == 0) {
				lines.add(rs[0]);
			} else {
				stations.add(rs[0]);
			}
		}
		String lineids = lines.size() > 0 ? lines.toString().substring(1,
				lines.toString().length() - 1) : "";
		String stationids = stations.size() > 0 ? stations.toString().substring(1,
				stations.toString().length() - 1) :"";
		if (lines.size() > 0 || stations.size() > 0) {
            result.set(0, new Text(gids[0]));
            result.set(1, new Text(gids[1]));
            result.set(2, new Text(gids[2]));
            result.set(3, new Text(lineids));
            result.set(4, new Text(stationids));
			context.write(result);
		}
	}

}
