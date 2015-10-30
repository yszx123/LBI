package com.autonavi.odps.udf;

import com.aliyun.odps.udf.UDF;
import com.autonavi.map.LngLat;
import com.autonavi.map.Point;
import com.autonavi.map.TMap;

public class GidOperator extends UDF{
	
	public String evaluate(Double lng1, Double lat1) {
		LngLat pt = new LngLat(lng1,lat1);
		Point point = TMap.lonLat2Mercator(pt);
		int x = (int) Math.ceil(point.X / 300);
		int y = (int) Math.ceil(point.Y / 300);
		return x + "_" + y;
    }

}

