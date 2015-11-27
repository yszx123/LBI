package com.lbi.lbi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.aliyun.odps.Record;
import com.aliyun.odps.io.DoubleWritable;
import com.aliyun.odps.io.IntWritable;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.mapreduce.ReduceContext;
import com.aliyun.odps.mapreduce.Reducer;
import com.lbi.lbi.model.OptimConstants;
import com.lbi.tmap.cloud.common.datamodel.spatial.Point;
import com.lbi.tmap.cloud.common.datamodel.spatial.cluster.m.ClusterModel;
import com.lbi.tmap.cloud.common.datamodel.spatial.cluster.m.Gid;
import com.lbi.tmap.cloud.common.datamodel.spatial.util.Kernel;
import com.lbi.tmap.cloud.common.datamodel.spatial.util.SpatialUtil;
import com.lbi.tmap.cloud.process.geographicstatistic.ClusterCreator;
//import org.apache.hadoop.io.NullWritable;
//import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * 对一个区间的点集进行聚类
 * 
 * @author shuaimin.yang
 * 
 */
public class ClusterReducerClass extends Reducer<Text, Text> {
	private Record result = null;
	private Record result2 = null;
	ClusterCreator creator = new ClusterCreator();
	private String keySeperator = null;
	private int radius = 50;
	private int topn = 10;
	private int area = 500;
	private int gridpoints = 300000;
	private int filter_number = 1;

	@Override
	protected void setup(ReduceContext<Text, Text> context) throws IOException,
			InterruptedException {
		result = context.createOutputRecord();
		result2 = context.createOutputRecord("cluster");
		keySeperator = context.getConfiguration().get("key.seperator");
		radius = context.getConfiguration().getInt("simplify.radius", 50);
	}

	public void reduce(Text key, Iterable<Text> values,
			ReduceContext<Text, Text> context) throws IOException,
			InterruptedException {
		String lonlat;
		HashMap<String, Integer> mapSet = new HashMap<String, Integer>();
		int dup = 0;
		System.out.println("reduce handle key is " + key.toString());
		for (Text val : values) {
			lonlat = val.toString();
			if (mapSet.get(lonlat) == null) {
					mapSet.put(lonlat, 1);
				} else {
					mapSet.put(lonlat, mapSet.get(lonlat) + 1);
				}
//			}
		}

		String[] keys = mapSet.keySet().toArray(new String[0]);
		List<Point> result = new ArrayList<Point>();
		List<String> isolates = new ArrayList<String>();

		for (int i = 0; i < keys.length; i++) {
			if (mapSet.get(keys[i]) == 1) {
				// 需要考虑加权到其周围的points上，可能需要排序，找到最大的来加权
				isolates.add(keys[i]);
			}
		}
		/*for (int k = 0; k < isolates.size(); k++) {
			Kernel l = SpatialUtil.snapToNearestPoint(isolates.get(k), mapSet,
					OptimConstants.gridWidth);
			if (l != null) {
				mapSet.put(l.code, l.value + 1);
			}
		}*/
		if (keys.length > 1000000) {
			filter_number = 5;
		}
		for (int i = 0; i < keys.length; i++) {
			String s[] = keys[i].split(this.keySeperator);
			if (mapSet.get(keys[i]) > filter_number) {
				double xy[] = SpatialUtil.encode(Integer.parseInt(s[0]), Integer.parseInt(s[1]), OptimConstants.gridWidth);
				result.add(new Point(xy[0], xy[1], mapSet.get(keys[i])));
			}
		}
		// System.out.println(key.toString() + "," + result.size() + ","
		// + mapSet.keySet().toArray(new String[0]).length +
		// ", snap to nearest size=" +dup);
		mapSet = null;
		if (result.size() > 0) {
			cluster(key, result, context, radius);
		}
	}

	// @Override
	/*
	 * public void reduce(Text key, Iterable<Text> values, ReduceContext<Text,
	 * Text> context) throws IOException, InterruptedException {
	 * ArrayList<String> list = new ArrayList<String>(); String lonlat;
	 * HashMap<String, Integer> mapSet = new HashMap<String, Integer>(); int dup
	 * = 0; for (Text val : values) { lonlat = val.toString(); if
	 * (mapSet.get(lonlat) == null) { mapSet.put(lonlat, 1); list.add(lonlat); }
	 * else { mapSet.put(lonlat, mapSet.get(lonlat) + 1); dup++; } } List<Point>
	 * result = new ArrayList<Point>(); if (dup > 0) { String[] keys =
	 * mapSet.keySet().toArray(new String[0]); for (int i = 0; i < keys.length;
	 * i++) { String s[] = keys[i].split(this.keySeperator); if
	 * (mapSet.get(keys[i]) > 0) { result.add(new
	 * Point(Double.parseDouble(s[0]), Double .parseDouble(s[1]),
	 * mapSet.get(keys[i]))); } } } // context.write(result); List<Point> pts =
	 * simplifyByGrid(result); if(pts !=null){ cluster(key, pts, context); } }
	 */

	/*
	 * private List<Point> simplifyByGrid(List<Point> pts) { Map<String,
	 * ArrayList<Point>> group = new HashMap<String, ArrayList<Point>>(); double
	 * gridWidth = 0.000025;// 0.00005 -> 5 mi 0.000012 -> 1.19mi for (int i =
	 * 0; i < pts.size(); i++) { Point pt = pts.get(i); long XN = (long)
	 * Math.ceil(pt.x / gridWidth); long YN = (long) Math.ceil(pt.y /
	 * gridWidth); String code = XN + "_" + YN; if (!group.containsKey(code)) {
	 * group.put(code, new ArrayList<Point>()); } group.get(code).add(pt); }
	 * List<Point> result = new ArrayList<Point>(); for (String key :
	 * group.keySet()) { if (group.get(key).size() > 1) { Point center =
	 * SpatialUtil.getCenter(group.get(key)); result.add(center); }
	 * group.put(key, null); } System.out.println(pts.size() +
	 * " ---------lnglat-------- " + result.size()); if(result.size() == 0) {
	 * return null; } return result; }
	 */

	private void cluster(Text key, List<Point> list, ReduceContext ctx,
			int radius) throws IOException,
			InterruptedException {
		List<ClusterModel> cx = creator.getClusterModels(list, radius, topn,
				area, gridpoints, keySeperator, key.toString());// CXDataForLngLat(list);

		if (cx.size() > 0) {
			for (ClusterModel ll : cx) {
				Set<String> line_ids = new HashSet<String>();
				Set<String> stop_ids = new HashSet<String>();
				for (Gid g : ll.getGids()) {
					result.set(0, new Text(ll.getCluster_id()));
					result.set(1, new Text(g.getGid()));
					result.set(2, new Text(g.getGid1()));
					result.set(3, new Text(g.getGid2()));
					ctx.write(result);
				}
				result2.set(0, new Text(ll.getCluster_id()));
				result2.set(1, new Text(ll.getGeom()));
				result2.set(2, new IntWritable(ll.getCluster_number()));
				result2.set(3, new Text(String.valueOf(ll.getArea())));
				result2.set(4, new Text(String.valueOf(ll.getFactor())));
				result2.set(5, new DoubleWritable(radius));
				ctx.write(result2, "cluster");
			}
		}
	}
}
