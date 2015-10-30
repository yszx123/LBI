package com.autonavi.lbi;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Set;

import com.aliyun.odps.Record;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.mapreduce.MapContext;
import com.aliyun.odps.mapreduce.Mapper;
import com.autonavi.map.LngLat;
import com.autonavi.map.TMap;

/**
 * 将record 经纬度点分配到合理的聚类区间 Record(x,y,...)
 * 
 * @author shuaimin.yang
 * 
 */
public class TransitImeMapperClass extends Mapper<Text, Text> {

	private Text word = new Text();

	private String keySeperator = ",";

	private String keyDate = "";

	private int GIDSIZE = 300;

	private String log_table_name = "address_location_log";

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
			String day = accesstime.substring(0, 10).replaceAll("-", "");
			String imei = record.get("imei").toString();
			if (day.equalsIgnoreCase(keyDate) && imei.indexOf(keySeperator)==-1) {
				com.autonavi.map.Point pt = TMap.lonLat2Mercator(new LngLat(
						lon, lat));
				int gx = (int) Math.ceil(pt.X / GIDSIZE);
				int gy = (int) Math.ceil(pt.Y / GIDSIZE);
				gid = gx + "_" + gy;
				k.set(gid);
				word.set(new Text("location" + keySeperator + imei
						+ keySeperator + accesstime + keySeperator + lon
						+ keySeperator + lat));
				context.write(k, word);
			}
			// }
		} else {
			gid = record.get("gid").toString();
			String line_ids = record.get("line_ids").toString();
			String station_ids = record.get("station_ids").toString();
			String gid1 = record.get("gid_1").toString();
			String gid2 = record.get("gid_2").toString();
			k.set(gid);
			word.set(new Text("transit" + keySeperator + line_ids
					+ keySeperator + station_ids + keySeperator + gid1
					+ keySeperator + gid2));
			context.write(k, word);
		}

	}

	@Override
	protected void setup(MapContext<Text, Text> context) throws IOException,
			InterruptedException {
		keySeperator = context.getConfiguration().get("key.seperator");
		keyDate = context.getConfiguration().get("key.date");
	}

}
