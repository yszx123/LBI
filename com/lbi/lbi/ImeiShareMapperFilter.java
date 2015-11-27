package com.lbi.lbi;

import java.io.IOException;

import com.aliyun.odps.Record;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.mapreduce.MapContext;
import com.aliyun.odps.mapreduce.Mapper;
//@author bailu
public class ImeiShareMapperFilter extends Mapper<Text, Text> {
	String imei_share;

	public void setup(MapContext<Text, Text> context) throws IOException,
			InterruptedException {
		imei_share = "888888888888888,352315050191630,null.10000:CMCC,null.10000:中国移动:,111111111111111,null.10000::null,:4.0.4:CNLAUNCH:yuanzheng77_3778:600*976:WIFI,004999010640000,352273017386340,null.10000:MY CELCOM:,null.10000:中国联通:,null.10000:中國移動:,354847011887169";

	}

	public void map(LongWritable recnum, Record record,
			MapContext<Text, Text> context) throws IOException {
		Text key = new Text();
		Text value = new Text();
		String imei = record.get("imei").toString();
		String accesstime = record.get("accesstime").toString();
		Double latitude = Double.parseDouble(record.get("lat").toString());
		Double lontitude = Double.parseDouble(record.get("lon").toString());
		 String appname=record.get("appname").toString();
		 String is_rota=record.get("is_rota").toString();
		 Double radius=Double.parseDouble(record.get("radius").toString());
		if (imei.length() >= 15) {
			imei = imei.substring(0, 15).toUpperCase();
//			// 十五位中第十五位是字母的截取前十四位
			if (imei.substring(14, 15).matches("^[0-9]*$") == false
					&& imei.substring(0, 14).toUpperCase()
							.matches("^[A-F][0-9A-F]{13}$")
					&& imei_share.indexOf(imei.substring(0, 14)) < 0) {
				imei = imei.substring(0, 14);
				key.set(imei);
				 String val = accesstime + "#" + latitude + "#" +
				 lontitude+"#"+appname+"#"+is_rota+"#"+radius;
//				String val = accesstime + "#" + latitude + "#" + lontitude;
				value.set(val);
				context.write(key, value);

			}
			
			if (imei.matches("^[0-9]*$") && imei_share.indexOf(imei) < 0) {
				key.set(imei);
				 String val = accesstime + "#" + latitude + "#" +
				 lontitude+"#"+appname+"#"+is_rota+"#"+radius;
//				String val = accesstime + "#" + latitude + "#" + lontitude;
				value.set(val);
				context.write(key, value);
			}

		}

		if (imei.length() == 14
				&& imei.toUpperCase().matches("^[A-F][0-9A-F]{13}$")
				&& imei_share.indexOf(imei) < 0)

		{
			key.set(imei);
			 String val = accesstime + "#" + latitude + "#" +
			 lontitude+"#"+appname+"#"+is_rota+"#"+radius;
//			String val = accesstime + "#" + latitude + "#" + lontitude;
			value.set(val);
			context.write(key, value);
		}

	}

}
