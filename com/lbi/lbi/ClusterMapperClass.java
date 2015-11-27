package com.lbi.lbi;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import com.aliyun.odps.Record;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.mapreduce.MapContext;
import com.aliyun.odps.mapreduce.Mapper;
import com.lbi.lbi.model.OptimConstants;
import com.lbi.map.LngLat;
import com.lbi.map.TMap;
import com.lbi.spatial.EsriAlgorithmsUtil;
import com.esri.core.geometry.Point;

/**
 * 将record 经纬度点分配到合理的聚类区间 Record(x,y,...)
 * 
 * @author shuaimin.yang
 * 
 */
public class ClusterMapperClass extends Mapper<Text, Text> {

	private Text word = new Text();

	private String keySeperator = ",";

	private double gridWidth = 0.000025; // 2.38米);

	private double splitLL = 2;

	private EsriAlgorithmsUtil util = null;

	private String log_table_name = "address_location_log";

	/*
	 * @Override public void map(LongWritable recordNum, Record record,
	 * MapContext<Text, Text> context) throws IOException, InterruptedException
	 * { Text k = new Text();
	 * 
	 * word.set(record.get("x") + keySeperator + record.get("y")); LngLat pt =
	 * new LngLat(Double.parseDouble(record.get("x").toString()),
	 * Double.parseDouble(record.get("y").toString())); Point point =
	 * TMap.lonLat2Mercator(pt); long XN = (long) Math.ceil(point.X / 10000);
	 * long YN = (long) Math.ceil(point.Y / 10000); Grid grid = new Grid(XN, YN,
	 * 300); String name = record.get("adcode").toString().substring(0, 3); //
	 * String name = grid.getCode(); k.set(name); context.write(k, word); }
	 */

	@Override
	public void map(LongWritable recordNum, Record record,
			MapContext<Text, Text> context) throws IOException,
			InterruptedException {
		Text k = new Text();
		String tableName = context.getInputTableInfo().getTableName();
		if (tableName.indexOf(log_table_name) != -1) {
			
			double lon = Double.parseDouble(record.get("lon").toString());
			double lat = Double.parseDouble(record.get("lat").toString());

			long XN = 0;//(long) Math.ceil(lon / OptimConstants.gridWidth);
			long YN = 0;//(long) Math.ceil(lat / OptimConstants.gridWidth);

			long KX = (long) Math.ceil(lon / splitLL);
			long KY = (long) Math.ceil(lat / splitLL);
			// double fx = (XN + 0.5) * gridWidth;
			// double fy = (YN + 0.5) * gridWidth;
			String name = record.get("accesstime").toString().substring(0, 10);// 10
																				// 日
																				// ，13
																				// 时
			String[] poi_desc = record.get("poi_desc").toString().split(" ");
			// String nameIndex = util.queryPoint(new Point(lon,lat));
			com.lbi.map.Point pt = TMap.lonLat2Mercator(new LngLat(lon,lat));
		    
			if (name.indexOf("201") != -1 && poi_desc.length > 1 && lon >= -180
					&& lon < 180 && lat >= -90 && lat < 90) {
				String ad = poi_desc[0] + keySeperator + poi_desc[1];
				ad = (poi_desc[0].indexOf("市") != -1
						|| poi_desc[0].indexOf("特别行政") != -1 ? poi_desc[0] : ad);
				XN = (long) Math.ceil(pt.X / OptimConstants.gridWidth);
				YN = (long) Math.ceil(pt.Y / OptimConstants.gridWidth);
				// ad = KX + "-" + KY;
				ad = String.valueOf(util.queryQuadTree(new Point(lon, lat)));
				if (ad.equalsIgnoreCase("-1")) {
					// System.out.println("outside china extent = " + lon + " "
					// +
					// lat);
				} else {
					word.set(XN + keySeperator + YN);
					k.set(ad); // 市
					context.write(k, word);
				}

			}
		}
	}

	@Override
	protected void setup(MapContext<Text, Text> context) throws IOException,
			InterruptedException {
		keySeperator = context.getConfiguration().get("key.seperator");
		BufferedInputStream inputStream = context
				.readCacheFileAsStream("wkt.txt");
		InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
		BufferedReader input = new BufferedReader(inputStreamReader);
		String line = "";
		ArrayList<String> fs = new ArrayList<String>();
		while ((line = input.readLine()) != null)//
		{
			fs.add(line);
		}
		util = new EsriAlgorithmsUtil();
		util.init(fs);
	}

}
