package com.autonavi.lbi;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import com.aliyun.odps.Record;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.mapreduce.MapContext;
import com.aliyun.odps.mapreduce.Mapper;
import com.autonavi.lbi.model.Transit;
import com.autonavi.map.LngLat;
import com.autonavi.map.TMap;

/**
 * 灏唕ecord 缁忕含搴︾偣鍒嗛厤鍒板悎鐞嗙殑鑱氱被鍖洪棿 Record(x,y,...)
 * 
 * @author shuaimin.yang
 * 
 */
public class TransitImeiMapper extends Mapper<Text, Text> {

	private Text word = new Text();

	private String keySeperator = ",";

	private int GIDSIZE = 300;

	private String log_table_name = "address_location_log";

	private HashMap<String, String> transitMap = new HashMap<String, String>();

	@Override
	public void map(LongWritable recordNum, Record record,
			MapContext<Text, Text> context) throws IOException,
			InterruptedException {
		Text k = new Text();
		String tableName = context.getInputTableInfo().getTableName();
		String gid = "";
				
		if (tableName.indexOf(log_table_name) != -1) {
			double lon = Double.parseDouble(record.get("lon").toString());
			double lat = Double.parseDouble(record.get("lat").toString());
			String accesstime = record.get("accesstime").toString();
			String imei = record.get("imei").toString();
			com.autonavi.map.Point pt = TMap.lonLat2Mercator(new LngLat(lon,
					lat));
			int gx = (int) Math.ceil(pt.X / GIDSIZE);
			int gy = (int) Math.ceil(pt.Y / GIDSIZE);
			String ke = accesstime.substring(0, 10).replaceAll("-", "");
			gid =  gx + "_" + gy;			
			if (!ke.equalsIgnoreCase("")) {							
				if (transitMap.get(gid) != null) {		
					k.set(new Text(ke + keySeperator + gid));
					word.set(new Text("location" + keySeperator + imei
							+ keySeperator + accesstime + keySeperator + lon
							+ keySeperator + lat));			
					context.write(k, word);
					String transit = transitMap.get(gid);
					Text word1 = new Text();
					word1.set(new Text("transit" + keySeperator + transit));					
					context.write(k, word1);
				}
			}
		} 
	}

	@Override
	protected void setup(MapContext<Text, Text> context) throws IOException,
			InterruptedException {
		keySeperator = context.getConfiguration().get("key.seperator");
		Iterator<Record> records = context.readCacheTable("transit_gidindex")
				.iterator();
		String[] outputs = context.getConfiguration().get("output.parts")
				.split(",");
		while (records.hasNext()) {
			Record r = records.next();
			String gid = r.get("gid").toString();
			String line_ids = r.get("line_ids").toString();
			String station_ids = r.get("station_ids").toString();
			String gid1 = r.get("gid_1").toString();
			String gid2 = r.get("gid_2").toString();
			Transit t = new Transit();
			t.gid = gid;
			t.gid1 = gid1;
			t.gid2 = gid2;
			t.line_id = line_ids;
			t.station_id = station_ids;
			// if(transitMap.get(gid) == null){
			// Set<String> val = new HashSet<String>();
			// val.add(t.toString());
			// transitMap.put(gid, val);
			// }else{
			// transitMap.get(gid).add(t.toString());
			// }
			// val.add(t.toString());
			transitMap.put(gid, t.toString());
		}

	}

}
