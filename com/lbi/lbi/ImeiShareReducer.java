package com.lbi.lbi;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import com.aliyun.odps.Record;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.mapreduce.ReduceContext;
import com.aliyun.odps.mapreduce.Reducer;
//@author bailu
public class ImeiShareReducer extends Reducer<Text, Text> {
	Record result = null;
	DateFormat date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	public void setup(ReduceContext<Text, Text> context) throws IOException,
			InterruptedException {
		result = context.createOutputRecord();
	}

	public void reduce(Text key, Iterable<Text> values,
			ReduceContext<Text, Text> context) throws IOException {
		List<String> list = new ArrayList<String>();
		for (Text val : values) {
			list.add(val.toString());
		}
		Collections.sort(list);
		Date date1 = null;
		Date date2 = null;
		for (int i = 0; i < list.size() - 1; i++) {
			String[] time_lat_lon1 = list.get(i).split("#");
			String[] time_lat_lon2 = list.get(i + 1).split("#");
		try {
			double lat1 = Double.parseDouble(time_lat_lon1[1]);
			double lon1 = Double.parseDouble(time_lat_lon1[2]);
			double lat2 = Double.parseDouble(time_lat_lon2[1]);
			double lon2 = Double.parseDouble(time_lat_lon2[2]);
			double distance = evaluate(lat1, lon1, lat2, lon2);
			
				date1 = date.parse(time_lat_lon1[0]);
				date2 = date.parse(time_lat_lon2[0]);
				
				long time = (date2.getTime() - date1.getTime()) / 1000;
				if (time == 0) {
					if (distance >1000) {
						result.set(0, key);
//						result.set(1, new Text(date3.parse(time_lat_lon1[0]).toString()));
//						result.set(2, new Text(String.valueOf(time)));
//						result.set(3, new DoubleWritable(distance));
//						result.set(4, new Text(String.valueOf(list.size())));
						context.write(result);
						break;
					}
				} else {
					double velocity = distance / time;
					if (velocity >1000) {
						result.set(0, key);
//						result.set(1, new DoubleWritable(distance));
//						result.set(2, new Text(String.valueOf(time)));
//						result.set(3, new DoubleWritable(velocity));
//						result.set(4, new Text(String.valueOf(list.size())));
						context.write(result);
						break;
					}
				}
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}
		context.write(result);
	}

	public static double EARTH_RADIUS = 6378137;

	private static double rad(double d) {
		return d * Math.PI / 180.0;
	}

	public Double evaluate(Double lat1, Double lng1, Double lat2, Double lng2) {
		double radLat1 = rad(lat1);
		double radLat2 = rad(lat2);
		double a = radLat1 - radLat2;
		double b = rad(lng1) - rad(lng2);
		double s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2)
				+ Math.cos(radLat1) * Math.cos(radLat2)
				* Math.pow(Math.sin(b / 2), 2)));
		s = s * EARTH_RADIUS;
		return s;
	}
}
