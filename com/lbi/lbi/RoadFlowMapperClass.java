package com.lbi.lbi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import com.aliyun.odps.Record;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.mapreduce.MapContext;
import com.aliyun.odps.mapreduce.Mapper;
import com.lbi.lbi.model.OptimConstants;
import com.lbi.map.LngLat;
import com.lbi.map.TMap;
import com.lbi.spatial.EsriAlgorithmsUtil;
import com.lbi.spatial.GeomAlgorithmUtil;
import com.esri.core.geometry.Point;

public class RoadFlowMapperClass extends Mapper<Text, Text> {

	private Text word = new Text();

	private String keySeperator = "@";
	
	private EsriAlgorithmsUtil util = null;	

	GeomAlgorithmUtil gUtil = new GeomAlgorithmUtil();
	
	@Override
	public void map(LongWritable recordNum, Record record,
			MapContext<Text, Text> context) throws IOException,
			InterruptedException {
		    Text k = new Text();					
			double lon = Double.parseDouble(record.get("lon").toString());
			double lat = Double.parseDouble(record.get("lat").toString());
			String imei = record.get("imei").toString();
			String name = record.get("accesstime").toString().split(" ")[1].substring(0, 2);
			String[] poi_desc = record.get("poi_desc").toString().split(" ");
			String is_rota = record.get("is_rota").toString();
			com.lbi.map.Point pt = TMap.lonLat2Mercator(new LngLat(lon,lat));
			if (poi_desc.length > 1 && lon >= -180
					&& lon < 180 && lat >= -90 && lat < 90 && is_rota.equalsIgnoreCase("0")) {
				String ad = poi_desc[0] + keySeperator + poi_desc[1];
				ad = (poi_desc[0].indexOf("市") != -1
						|| poi_desc[0].indexOf("特别行政") != -1 ? poi_desc[0] : ad);
				// ad = KX + "-" + KY;
				ad = String.valueOf(util.queryNearest(new Point(pt.X,pt.Y),500));
				if (ad.equalsIgnoreCase("-1")) {
//					 System.out.println("outside china extent = " + lon + " " + lat);
				} else {
					word.set(imei);
					k.set(ad+":"+name); //
					context.write(k, word);
				}

			}
	}

	@Override
	protected void setup(MapContext<Text, Text> context) throws IOException,
			InterruptedException {
		Iterator<Record> records = context.readCacheTable("roadindex")
				.iterator();
		util = new EsriAlgorithmsUtil();
		ArrayList<Record> rs = new ArrayList<Record>();
		while(records.hasNext()){
			Record r = records.next();
			rs.add(r);
		}		
		util.buildWKTFeature(rs);		
	}

}

