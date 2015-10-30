package com.autonavi.lbi;

import java.io.IOException;
import java.util.Iterator;

import com.aliyun.odps.Record;
import com.aliyun.odps.io.IntWritable;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.mapreduce.MapContext;
import com.aliyun.odps.mapreduce.Mapper;
import com.autonavi.spatial.EsriAlgorithmsUtil;
import com.autonavi.spatial.GeomAlgorithmUtil;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.Operator;
import com.esri.core.geometry.OperatorBuffer;
import com.esri.core.geometry.OperatorFactoryLocal;
import com.esri.core.geometry.OperatorImportFromWkt;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.Polygon;
import com.esri.core.geometry.Polyline;
import com.esri.core.geometry.SpatialReference;

/**
 * 将record 经纬度点分配到合理的聚类区间 Record(x,y,...)
 * 
 * @author shuaimin.yang
 * 
 */
public class TransitMapperClass extends Mapper<Text, Text> {

	private Text word = new Text();

	private String keySeperator = ",";

	private double gridWidth = 0.000025; // 2.38米);

	private double splitLL = 2;

	private EsriAlgorithmsUtil util = null;


//	private Record result = null;
	
	GeomAlgorithmUtil gUtil = new GeomAlgorithmUtil();
	@Override
	public void map(LongWritable recordNum, Record record,
			MapContext<Text, Text> context) throws IOException,
			InterruptedException {
		Text k = new Text();
		String tableName = context.getInputTableInfo().getTableName();
		SpatialReference spatialReference = SpatialReference.create(3857);
		OperatorImportFromWkt importerWKT = (OperatorImportFromWkt) OperatorFactoryLocal
				.getInstance().getOperator(Operator.Type.ImportFromWkt);
		Polygon bufferPoly = null; 
		String transit_id = "";
		int transit_type = 0;
		if(tableName.indexOf("transit_line") != -1){			
			Polyline polyline = (Polyline) (importerWKT.execute(0, Geometry.Type.Unknown,
					record.get("geometry").toString(), null));
			OperatorBuffer buffer = (OperatorBuffer) OperatorFactoryLocal
					.getInstance().getOperator(Operator.Type.Buffer);
			Geometry result = buffer.execute(polyline, spatialReference, 1500.0, null);
			bufferPoly = (Polygon) result;
			transit_id = record.get("line_id").toString();
		}else{
			Point point = (Point) (importerWKT.execute(0, Geometry.Type.Point,
					record.get("geometry").toString(), null));
			OperatorBuffer buffer = (OperatorBuffer) OperatorFactoryLocal
					.getInstance().getOperator(Operator.Type.Buffer);
			Geometry result = buffer.execute(point, spatialReference, 1500.0, null);
			bufferPoly = (Polygon) result;
			transit_id = record.get("businfo_id").toString();
			transit_type = 1;
		}
		Iterator<String> gid300 = gUtil.polygon2gids(bufferPoly,300).iterator();
		while (gid300.hasNext()) {
			 String gid = gid300.next();
			 String[] XY = gid.split("_");
			 String gid1 = (int)Math.ceil(Integer.parseInt(XY[0])/2)+"_"+(int)Math.ceil(Integer.parseInt(XY[1])/2);
			 String gid2 = (int)Math.ceil(Integer.parseInt(XY[0])/4)+"_"+(int)Math.ceil(Integer.parseInt(XY[1])/4);
//			 result.set(0, new Text(transit_id));
//			 result.set(1, new IntWritable(transit_type));
//			 result.set(2, new Text(gid));
//			 String[] XY = gid.split("_");
//			 result.set(3, new Text(Math.ceil(Integer.parseInt(XY[0])/2)+"_"+Math.ceil(Integer.parseInt(XY[1])/2)));
//			 result.set(4, new Text(Math.ceil(Integer.parseInt(XY[0])/4)+"_"+Math.ceil(Integer.parseInt(XY[1])/4)));
			 k.set(gid + ","  + gid1 + "," + gid2);
			 word.set(new Text(transit_id + "," + transit_type  ));
			 context.write(k, word);
		}
	}

	@Override
	protected void setup(MapContext<Text, Text> context) throws IOException,
			InterruptedException {
//		result = context.createOutputRecord();
////		BufferedInputStream inputStream = context
////				.readCacheFileAsStream("wkt.txt");
//		Iterable<Record> itr = context.readCacheTable("transit_line_info_d");
//		ArrayList<Record> rs = new ArrayList<Record>();
//		if(itr.iterator().hasNext()){
//			Record r = itr.iterator().next();
//			rs.add(r);
//		}		
//		/*InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
//		BufferedReader input = new BufferedReader(inputStreamReader);
//		String line = "";
//		ArrayList<String> fs = new ArrayList<String>();
//		while ((line = input.readLine()) != null)//
//		{
//			fs.add(line);
//		}*/
//		util = new EsriAlgorithmsUtil();
//		// util.initIndex();
//		// util.addIndexRegion(fs);
//		util.initRecords(rs,"line_id");
	}

}
