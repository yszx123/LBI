package com.autonavi.lbi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

import com.aliyun.odps.Record;
import com.aliyun.odps.io.DoubleWritable;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.mapreduce.ReduceContext;
import com.aliyun.odps.mapreduce.Reducer;
import com.autonavi.lbi.model.Imei;
import com.autonavi.lbi.model.Transit;
import com.autonavi.tmap.cloud.common.utils.TextUtil;

public class TransitImeiReducer extends Reducer<Text, Text> {
	private HashMap<String, Record> resultMap = new HashMap<String, Record>();

	private String keySeperator = ",";

	@Override
	protected void setup(ReduceContext<Text, Text> context) throws IOException,
			InterruptedException {
		keySeperator = context.getConfiguration().get("key.seperator");
		String[] outputs = context.getConfiguration().get("output.parts")
				.split(",");
		for (String out : outputs) {
			Record result = context.createOutputRecord(out);
			resultMap.put(out, result);
		}
	}

	@Override
	public void reduce(Text key, Iterable<Text> values,
			ReduceContext<Text, Text> context) throws IOException,
			InterruptedException {
		ArrayList<Transit> transits = new ArrayList<Transit>();
		ArrayList<Imei> imeis = new ArrayList<Imei>();
		String[] ks = key.toString().split(keySeperator);	
		long count = 0;
		for (Text val : values) {
				String[] rs = val.toString().split(keySeperator);//TextUtil.split(val.toString(), keySeperator, 5);
				if (rs[0].equalsIgnoreCase("location")) {
					Imei item = new Imei();
					item.imei = rs[1];
					item.accesstime = rs[2];
					item.lon = rs[3];
					item.lat = rs[4];
					imeis.add(item);
				} else { //只有一条有效记录，				
					if(transits.size() == 0){
						Transit t = new Transit();
						t.gid = ks[1];
						t.line_id = rs[1];
						t.station_id = rs[2];
						t.gid1 = rs[3];
						t.gid2 = rs[4];
						transits.add(t);
					}					
				}
//			 count++;  
		}
		
		if (transits.size() > 0 && imeis.size() > 0) {
			for (Imei i : imeis) {
				String ds = ks[0];
				if (ds.indexOf("201") != -1 && resultMap.get(ds) != null) {
					    Transit t = transits.get(0);
						resultMap.get(ds).set(0, new Text(i.imei));// imei
						resultMap.get(ds).set(1, new Text(t.line_id!=null?t.line_id:""));// line_id
						resultMap.get(ds).set(2, new Text(t.station_id!=null?t.station_id:""));// station_id
						resultMap.get(ds).set(3, new Text(i.accesstime));// accesstime
						resultMap.get(ds).set(4,
								new DoubleWritable(Double.parseDouble(i.lon)));
						resultMap.get(ds).set(5,
								new DoubleWritable(Double.parseDouble(i.lat)));
						resultMap.get(ds).set(6, new Text(t.gid));
						resultMap.get(ds).set(7, new Text(t.gid1));
						resultMap.get(ds).set(8, new Text(t.gid2));
						context.write(resultMap.get(ds), ds);
				/*resultMap.get(ds).set(0, new Text(ds));// imei
				resultMap.get(ds).set(1, new Text(ks[1]));// line_id
				resultMap.get(ds).set(2, new Text(ds));// station_id
				resultMap.get(ds).set(3, new Text(ds));// accesstime
				resultMap.get(ds).set(4,
						new DoubleWritable(0.0));
				resultMap.get(ds).set(5,
						new DoubleWritable(0.0));
				resultMap.get(ds).set(6, new Text(String.valueOf(count)));
				resultMap.get(ds).set(7, new Text(String.valueOf(count)));
				resultMap.get(ds).set(8, new Text(String.valueOf(count)));
						context.write(resultMap.get(ds), ds);*/
				}
			}
		}
	}

}
