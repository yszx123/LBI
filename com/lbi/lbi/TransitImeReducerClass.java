package com.autonavi.lbi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import com.aliyun.odps.Record;
import com.aliyun.odps.io.DoubleWritable;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.mapreduce.ReduceContext;
import com.aliyun.odps.mapreduce.Reducer;
import com.autonavi.lbi.model.Imei;
import com.autonavi.lbi.model.Transit;

public class TransitImeReducerClass extends Reducer<Text, Text> {
	private Record result = null;
	private String keySeperator = ",";
	private String ds ="";
	@Override
	protected void setup(ReduceContext<Text, Text> context) throws IOException,
			InterruptedException {
		keySeperator = context.getConfiguration().get("key.seperator");
		ds = context.getConfiguration().get("key.date");
		result = context.createOutputRecord();
	}

	@Override
	public void reduce(Text key, Iterable<Text> values,
			ReduceContext<Text, Text> context) throws IOException,
			InterruptedException {
		ArrayList<Transit> transits = new ArrayList<Transit>();
		ArrayList<Imei> imeis = new ArrayList<Imei>();
		long count = 0;
		for (Text val : values) {
			String[] rs = val.toString().split(keySeperator);
			if (rs[0].equalsIgnoreCase("location")) {
				Imei item = new Imei();
				item.imei = rs[1];
				item.accesstime = rs[2];
				item.lon = rs[3];
				item.lat = rs[4];
				imeis.add(item);
				count++;
			} else { //可能是对应多个transit_gidindex记录，				
				if(transits.size() == 0){
					Transit t = new Transit();
					t.gid = key.toString();
					t.line_id = rs[1];
					t.station_id = rs[2];
					t.gid1 = rs[3];
					t.gid2 = rs[4];
					transits.add(t);
				}	
			}
		}
		if (transits.size() > 0 && count > 0) {
			for(Imei i: imeis){
				if( result != null && isDouble(i.lon) && isDouble(i.lat)){
					    Transit t = transits.get(0);						
					    result.set(0, new Text(i.imei));// imei				
					    result.set(1, new Text(t.line_id));// line_id
					    result.set(2, new Text(t.station_id));// station_id
					    result.set(3, new Text(i.accesstime));// accesstime
					    result.set(4, new DoubleWritable(Double.parseDouble(i.lon)));
					    result.set(5, new DoubleWritable(Double.parseDouble(i.lat)));
					    result.set(6, new Text(t.gid));
					    result.set(7, new Text(t.gid1));
					    result.set(8, new Text(t.gid2));	
						context.write(result);
				}					
			}
		}
	}
	
	static boolean isDouble(String x) {
		try {
			Double.parseDouble(x);
			return true;
		} catch (NumberFormatException ex) {
		}
		return false;
	}
	
}


