package com.autonavi.lbi;

import java.io.IOException;

import com.aliyun.odps.Record;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.mapreduce.MapContext;
import com.aliyun.odps.mapreduce.Mapper;
import com.autonavi.map.LngLat;
import com.autonavi.map.Point;
import com.autonavi.map.TMap;

public class ClusterImeiMapperClass extends Mapper<Text, Text> {

    private Text grid = new Text();
    private Text result = new Text();
	private String log_table_name = "address_location_log";

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
		String tableName = context.getInputTableInfo().getTableName();
		String gid = "";
		if (tableName.indexOf(log_table_name) != -1) {

			if (isDouble(record.get("lon").toString(), record.get("lat")
					.toString())) {
				// -----1、经纬度获取gid------
				// 经纬度点
				LngLat pt = new LngLat(Double.parseDouble(record.get("lon").toString()), 
						Double.parseDouble(record.get("lat").toString()));
				// 经纬度转墨卡托坐标
				Point point = TMap.lonLat2Mercator(pt);
				int x = (int) Math.ceil(point.X / 300);
				int y = (int) Math.ceil(point.Y / 300);
				// 点所在网格gid
				gid = x + "_" + y;
				String imei_time = "111:" + record.get("imei") + "_"
						+ record.get("accesstime");
				result.set(imei_time);
				grid.set(gid);
				context.write(grid, result);
			}
		} else {
			gid = record.get("gid").toString();
			String cluster = "000:" + record.get("cluster_id").toString();
			result.set(cluster);
			grid.set(gid);
			context.write(grid, result);
		}
	}

	@Override
	protected void setup(MapContext<Text, Text> context) throws IOException,
			InterruptedException {
	}

}
