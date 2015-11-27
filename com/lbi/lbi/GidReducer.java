package com.lbi.lbi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import com.aliyun.odps.Record;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.mapreduce.ReduceContext;
import com.aliyun.odps.mapreduce.Reducer;

public class GidReducer extends Reducer<Text, Text> {
	private String keySeperator = "@";
	private Record result = null;

	@Override
	protected void setup(ReduceContext<Text, Text> context) throws IOException,
			InterruptedException {
		result = context.createOutputRecord();
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values,
			ReduceContext<Text, Text> context) throws IOException,
			InterruptedException {
		String lonlat;
		HashMap<String, HashSet<String>> lineMap = new HashMap<String, HashSet<String>>(); // gid
		HashMap<String, HashSet<String>> stopMap = new HashMap<String, HashSet<String>>(); // gid
		Set<String> clusters = new HashSet<String>();
		for (Text val : values) {
			lonlat = val.toString();
			if (lonlat.indexOf("transit") != -1) {
				String[] transits = lonlat.split(keySeperator);
				if (!transits[1].equalsIgnoreCase("")) {
					if (stopMap.get(key.toString()) == null) {
						HashSet<String> lList = new HashSet<String>();
						lList.add(transits[1]);
						stopMap.put(key.toString(), lList);
					} else {
						stopMap.get(key.toString()).add(transits[1]);
					}
				}
				if (transits.length > 2 && !transits[2].equalsIgnoreCase("")) {
					if (stopMap.get(key.toString()) == null) {
						HashSet<String> lList = new HashSet<String>();
						lList.add(transits[2]);
						stopMap.put(key.toString(), lList);
					} else {
						stopMap.get(key.toString()).add(transits[2]);
					}
				}
			} else {
				String[] strs = lonlat.split(keySeperator);
				clusters.add(strs[1]);
			}
		}
		HashSet<String> line_ids = lineMap.get(key.toString());
		String lineid = "";
		if (line_ids != null) {
			lineid = line_ids.toString().substring(1,
					line_ids.toString().length() - 1);
		}
		HashSet<String> station_ids = stopMap.get(key.toString());
		String stationid = "";
		if (station_ids != null) {
			stationid = station_ids.toString().substring(1,
					station_ids.toString().length() - 1);
		}
		if (clusters != null && clusters.size() > 0
				&& (lineid != "" || stationid != "")) {
			for (String cluster_id : clusters) {
				result.set(0, new Text(cluster_id));
				result.set(1, new Text(lineid));
				result.set(2, new Text(stationid));
				context.write(result);
			}
		}
	}

}
