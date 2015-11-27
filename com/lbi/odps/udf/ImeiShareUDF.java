package com.lbi.odps.udf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import com.aliyun.odps.udf.UDF;
//@author bailu
public class ImeiShareUDF extends UDF{
		public String evaluate(String imei,String accesstime,double lat,double lon){
			HashMap<String,String> map=new HashMap<String,String>();	
			String value=null;
			if(map.containsKey(imei)){
				value+=accesstime+"#"+lat+"#"+lon+"@";
				map.put(imei, value);
			}
			else{
				value=accesstime+"#"+lat+"#"+lon+"@";
				map.put(imei, value);
			}
			
			return imei+"#distance#time#count";
		}	

		private static double rad(double d) {
			return d * Math.PI / 180.0;
		}

		public static Double evaluatedistance(Double lat1, Double lng1, Double lat2, Double lng2) {
			double EARTH_RADIUS = 6378137;
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
class Type{
	Set<String> imei;
	ArrayList<String> latlon;
	public void setvalue(Set<String> imei,ArrayList<String> latlon){
		this.imei=imei;
		this.latlon=latlon;
	}
}


