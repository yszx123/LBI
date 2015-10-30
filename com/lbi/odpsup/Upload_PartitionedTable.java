package com.autonavi.odpsup;

import java.io.IOException;

import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.feature.FeatureCollection;
import org.opengis.feature.simple.SimpleFeature;

import com.alibaba.odps.tunnel.Column;
import com.alibaba.odps.tunnel.Configuration;
import com.alibaba.odps.tunnel.DataTunnel;
import com.alibaba.odps.tunnel.RecordSchema;
import com.alibaba.odps.tunnel.TunnelException;
import com.alibaba.odps.tunnel.Upload;
import com.alibaba.odps.tunnel.Upload.Status;
import com.alibaba.odps.tunnel.io.Record;
import com.alibaba.odps.tunnel.io.RecordWriter;
import com.autonavi.odpsup.shp.ShpReader;

public class Upload_PartitionedTable {
	private static String endpoint = "http://dt.odps.aliyun.com";//"http://dt.odps.aliyun.com";
	private static String accessId = "DFTvkXWruShVfqjx";
	private static String accessKey = "zqeHhP3vyqTwczdVdzFulfMH6TZ9Nl";

	private static String project = "mapabc_lbi";
	private static String table = "s_autonavi_road_info";
	private static String partition = "ds=20140415";//"ds=20140110";

	static ShpReader reader = new ShpReader();
	
	public static void main(String[] args) {
		long time = System.currentTimeMillis();
		
		Configuration cfg = new Configuration(accessId, accessKey, endpoint);
		DataTunnel tunnel = new DataTunnel(cfg);
		String flname = "five_cities_point.shp";
		if(args[0]!=null)
		{
			table = args[0];
		}
		if(args[1]!=null)
		{
			partition = args[1];
		}
		if(args[2]!=null)
		{
			flname= args[2];
		}
		try {
			Upload up = tunnel.createUpload(project, table, partition);
			String id = up.getUploadId();
			System.out.println("UploadId = " + id);

			RecordSchema schema = up.getSchema();
			System.out.println("Schema is: " + schema.toJsonString());

			Status status = up.getStatus();
			System.out.println("Status is: " + status.toString());

			RecordWriter writer = up.openRecordWriter(0);
			Record r = new Record(schema.getColumnCount());

			
//			File fl = new File(PropertiesUtil.getValue("csvpath")
//					+ "/synchronization/" + flname);
			FeatureCollection list = reader.getLayerFeatures(flname);
			SimpleFeatureIterator iterator = (SimpleFeatureIterator)list.features();
			if (list.size() > 0) {
				int count = 0;
				if( count % 1000 == 0)
				{
					System.out.println(count);
				}
				while (iterator.hasNext()) {
					SimpleFeature feature = iterator.next();
					for (int i = 0; i < schema.getColumnCount(); i++) {						
						String columnName = schema.getColumnName(i);
						if(columnName.equalsIgnoreCase("geometry")){
							columnName = "the_geom";
						}else{
							columnName = columnName.toUpperCase();
						}
						Object o = feature.getAttribute(columnName);
						if(o == null)
						{
							continue;
						}
						Column.Type t = schema.getColumnType(i);
						switch (t) {
						case ODPS_DOUBLE:
							r.setDouble(i, Double.parseDouble(o.toString()));
							break;
						case ODPS_STRING:
							String value = o.toString();
							r.setString(i, value);
							break;
						case ODPS_BIGINT:
							Long v = Long.parseLong(o.toString());
							r.setBigint(i, v);
							break;
						default:
							throw new RuntimeException("Unknown column type: "
									+ t);
						}
					}
                    count++;
					writer.write(r);
				}
				System.out.println("record count " + count);
				System.out.println("all time === "
						+ (System.currentTimeMillis() - time));

				writer.close();
				up.complete();
			}

		} catch (TunnelException e) {
			System.out.println(e);
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
