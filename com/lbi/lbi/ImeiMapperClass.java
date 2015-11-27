package com.lbi.lbi;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;

import com.aliyun.odps.Record;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.mapreduce.MapContext;
import com.aliyun.odps.mapreduce.Mapper;
import com.lbi.map.LngLat;
import com.lbi.map.Point;
import com.lbi.map.TMap;

public class ImeiMapperClass extends Mapper<Text, Text>{
	private Text grid = new Text();
    private Text result = new Text();
	private String log_table_name = "address_location_log";
    private String keySeperator = "";
	private HashSet<String> black_list = new HashSet<String>();
	
	static boolean isDouble(String x, String y) {
		try {
			Double.parseDouble(x);
			Double.parseDouble(y);
			return true;
		} catch (NumberFormatException ex) {
		}
		return false;
	}

	@Override
	public void map(LongWritable recordNum, Record record,
			MapContext<Text, Text> context) throws IOException {
		    String gid = "";
			String imei = record.get("imei").toString().trim();
			if (!black_list.contains(imei) && isDouble(record.get("lon").toString(), record.get("lat")
					.toString())) {
				
				LngLat pt = new LngLat(Double.parseDouble(record.get("lon").toString()), 
						Double.parseDouble(record.get("lat").toString()));
				// 经纬度转墨卡托坐标
				Point point = TMap.lonLat2Mercator(pt);
				int x = (int) Math.ceil(point.X / 300);
				int y = (int) Math.ceil(point.Y / 300);
				
				int x1 = (int)Math.ceil(x/2);
				int y1 = (int)Math.ceil(y/2);
				
				int x2 = (int)Math.ceil(x/4);
				int y2 = (int)Math.ceil(y/4);
				
				// 点所在网格gid
				gid = imei + keySeperator + x + "_" + y;
				
				//商圈id				
				result.set(x1 + "_" + y1 + keySeperator + x2 + "_" + y2 );
				grid.set(gid);
				context.write(grid, result);
			}
		
	}

	@Override
	protected void setup(MapContext<Text, Text> context) throws IOException,
			InterruptedException {
		keySeperator = context.getConfiguration().get("key.seperator");
		BufferedInputStream inputStream = context
				.readCacheFileAsStream("black_imei");
		InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
		BufferedReader input = new BufferedReader(inputStreamReader);
		String line = "";
		while ((line = input.readLine()) != null)//
		{
			black_list.add(line.split(",")[0]);
		}
	}

	
}
