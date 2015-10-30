package com.autonavi.log;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;

import com.aliyun.odps.Record;
import com.aliyun.odps.io.DoubleWritable;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.mapreduce.MapContext;
import com.aliyun.odps.mapreduce.Mapper;
import com.autonavi.aps.util.GpsOffset;

public class LogMapper extends Mapper<Text, Text> {
	private Text word = new Text();
	private Record result = null;
    private HashSet<String> license_offset = new HashSet<String>(); 
	private String keySeperator = "@";
//	private String location_flag0 = "0320a06086a84cb9e12e019e013290d9";
	private String location_flag = "b2dddf583bbf62c09bef0605fff491d6";
	//0320a06086a84cb9e12e019e013290d9 被动定位key
	@Override
	public void map(LongWritable recordNum, Record record,
			MapContext<Text, Text> context) throws IOException,
			InterruptedException {
		Text k = new Text();
		String lon = record.get("res_cenx").toString();
		String lat = record.get("res_ceny").toString();		
		String accesstime = record.get("accesstime").toString();
		String imei = record.get("imei").toString();
		String imsi = record.get("imsi").toString();
		String appname = record.get("appname").toString();
		String radius = record.get("res_radius").toString();
		String res_desc = record.get("res_desc").toString();
		String license = record.get("license").toString();
		String is_rota = "0";
		String flag = "0";
		boolean lon_b = (lon==null || lon.equalsIgnoreCase("null") 
				|| lon.equalsIgnoreCase("\\N") || lon.equalsIgnoreCase("") || Double.parseDouble(lon) <=0.0);
		boolean lat_b = (lat==null || lat.equalsIgnoreCase("null") 
				|| lat.equalsIgnoreCase("\\N") || lat.equalsIgnoreCase("") || Double.parseDouble(lat) <=0.0);
		if(lon_b || lat_b){
			is_rota = "0";
		}else{
			if(license_offset.contains(license))
			{
				if((!lon_b) && (!lat_b)){
					//System.out.println(lon + " " + lat);
					try{
//						String xy_origin = lon+"_"+lat;
						String offset = GpsOffset.offsetFrom(Double.parseDouble(lon),Double.parseDouble(lat));
						String[] xy = offset.split("\\|");
						lon = xy[0];
						lat = xy[1];
						is_rota = "1";
					}catch(NullPointerException e){
						is_rota = "0";
						e.printStackTrace();
					}
				}
			}else{
				is_rota = "1";
			}
		}
		String mapkey = record.get("mapapikey").toString();
		if(location_flag.equalsIgnoreCase(mapkey))
		{
			flag = "1";
		}
		//| accesstime | imei       | imsi       | appname    | lon        | lat        | radius     | poi_desc   | is_rota    | ds         |
		//accesstime,imei,imsi,appname,res_cenx,res_ceny,res_radius,res_desc
		if(!is_rota.equalsIgnoreCase("0")){
			result.set(0, new Text(accesstime));
			result.set(1, new Text(imei));
			result.set(2, new Text(imsi));
			result.set(3, new Text(appname));
			result.set(4, new DoubleWritable(Double.parseDouble(lon_b ?"0":lon)));
			result.set(5, new DoubleWritable(Double.parseDouble(lat_b ?"0":lat)));
			result.set(6, new DoubleWritable(Double.parseDouble((radius==null || radius.equalsIgnoreCase("null") 
					|| radius.equalsIgnoreCase("\\N") || radius.equalsIgnoreCase("")) ?"0":radius)));
			result.set(7, new Text(res_desc));
//			result.set(8, new Text(is_rota));
			result.set(8, new Text(flag));  //主动为0 ，被动为1
			context.write(result);
		}				
	}
	
	@Override
	protected void setup(MapContext<Text, Text> context) throws IOException,
			InterruptedException {
		BufferedInputStream inputStream = context
				.readCacheFileAsStream("license_offset.txt");
		InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
		BufferedReader input = new BufferedReader(inputStreamReader);
		String line = "";
		while ((line = input.readLine()) != null)//
		{
			license_offset.add(line);
		}
		result = context.createOutputRecord();
	}

}
