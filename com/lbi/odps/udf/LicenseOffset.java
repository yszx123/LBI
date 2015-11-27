package com.lbi.odps.udf;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;

import com.aliyun.odps.cache.DistributedCache;
import com.aliyun.odps.udf.UDF;
import com.lbi.aps.util.GpsOffset;

public class LicenseOffset extends UDF {
	
	
	private HashSet<String> license_offset = new HashSet<String>(); 
	public LicenseOffset(){
		try {
			this.initLicenseOffset();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public String evaluate(String license,String lon,String lat,String splitString) {
		
		boolean lon_b = (lon==null || lon.equalsIgnoreCase("null") || lon.equalsIgnoreCase("\\N") || lon.equalsIgnoreCase("") || Double.parseDouble(lon) <=0.0);
		boolean lat_b = (lat==null || lat.equalsIgnoreCase("null") || lat.equalsIgnoreCase("\\N") || lat.equalsIgnoreCase("") || Double.parseDouble(lat) <=0.0);
		if(lon_b || lat_b){
			return "";
		}else{
			if(license_offset!=null && license_offset.contains(license)){
				try{
					
					String offset = GpsOffset.offsetFrom(Double.parseDouble(lon),Double.parseDouble(lat));
					String[] xy = offset.split("\\|");
					lon = xy[0];
					lat = xy[1];
				}catch(NullPointerException e){
					e.printStackTrace();
					return "";
				}
			}
		}
		return lon+splitString+lat;
	}

	private void initLicenseOffset() throws IOException {
		
		BufferedInputStream inputStream = DistributedCache.readCacheFileAsStream("license_offset.txt");
		InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
		BufferedReader input = new BufferedReader(inputStreamReader);
		String line = "";
		while ((line = input.readLine()) != null)//
		{
			license_offset.add(line);
		}
	}
}
