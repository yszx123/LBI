package com.lbi.odps.udf;

import com.aliyun.odps.udf.UDF;
import com.lbi.map.LngLat;
import com.lbi.map.Point;
import com.lbi.map.TMap;

public class GridLngLatOperator extends UDF{
	
	public String evaluate(String gid,Double gridWidth) {
		String[] gids = gid.split("_");
		double fx = (Double.parseDouble(gids[0]) + 0.5) * gridWidth;
		double fy = (Double.parseDouble(gids[1]) + 0.5) * gridWidth;
		Point pt = new Point(fx,fy);
		LngLat ll = TMap.Mercator2lonLat(pt);
		return ll.lng + "," + ll.lat;
	}

}
