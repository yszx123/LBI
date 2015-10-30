package com.autonavi.odps.udf;

import com.aliyun.odps.udf.UDF;
import com.autonavi.map.LngLat;
import com.autonavi.map.Point;
import com.autonavi.map.TMap;

public class WKTMercator extends UDF{
	
	public String evaluate(String wkt) {
		int i=1;
		String left = "";
		for(;i<10;i++)
		{
			String cur = String.valueOf(wkt.charAt(wkt.length()-i));
			if(!cur.equalsIgnoreCase(")")){
		       break;		
			}else{
			   left+="(";
			}
		}
		int sIndex = wkt.indexOf(left);
		i--;
		String coords = wkt.substring(sIndex+i, wkt.length()-i);
		String[] list = coords.split(", ");
		String crds = "";
		for(String lls : list)
		{
			String[] ll = lls.split(" ");
			Point pt = TMap.lonLat2Mercator(new LngLat(Double.parseDouble(ll[0]),Double.parseDouble(ll[1])));
			crds += Math.round(pt.X*1000.000)/1000.000 + " "+ Math.round(pt.Y*1000.000)/1000.000 + ", "; 
		}
		crds = crds.substring(0, crds.length()-2);
		return wkt.replace(coords, crds);
    }

}
