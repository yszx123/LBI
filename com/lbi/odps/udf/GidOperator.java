package com.lbi.odps.udf;

import com.aliyun.odps.udf.UDF;
import com.lbi.map.LngLat;
import com.lbi.map.Point;
import com.lbi.map.TMap;

public class GidOperator extends UDF{
	
	public String evaluate(Double lng1, Double lat1) {
		LngLat pt = new LngLat(lng1,lat1);
		Point point = TMap.lonLat2Mercator(pt);
		int x = (int) Math.ceil(point.X / 300);
		int y = (int) Math.ceil(point.Y / 300);
		return x + "_" + y;
    }

}

