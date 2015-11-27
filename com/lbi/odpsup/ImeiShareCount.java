package com.lbi.odpsup;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.alibaba.odps.tunnel.Column;
import com.alibaba.odps.tunnel.Configuration;
import com.alibaba.odps.tunnel.DataTunnel;
import com.alibaba.odps.tunnel.Download;
import com.alibaba.odps.tunnel.RecordSchema;
import com.alibaba.odps.tunnel.TunnelException;
import com.alibaba.odps.tunnel.Download.Status;
import com.alibaba.odps.tunnel.io.Record;
import com.alibaba.odps.tunnel.io.RecordReader;
//@author bailu
public class ImeiShareCount {
	private static String endpoint = "http://dt.odps.aliyun.com";
	private static String accessId = "2w1mbUbPwhgbqLnu";
	private static String accessKey = "zpW6QAd30aDJYBuIeu1XrcCyoxllhE";

	private static String project = "lbi";
	private static String table = "lbi_bl_imei_accesstime_lat_lon";
	private static String partition = "20140401";
	
	@SuppressWarnings("deprecation")
	public static void main(String args[]) throws ParseException {
		Configuration cfg = new Configuration(accessId, accessKey, endpoint);
		DataTunnel tunnel = new DataTunnel(cfg);
		
		try {
			
			Download down = tunnel.createDownload(project, table, partition);
			String id = down.getDownloadId();
			System.out.println("DownloadId = " + id);

			RecordSchema schema = down.getSchema();
			System.out.println("Schema is: " + schema.toJsonString());

			Status status = down.getStatus();
			System.out.println("Status is: " + status.toString());

			long count = down.getRecordCount();
			System.out.println("RecordCount is: " + count);

			RecordReader reader = down.openRecordReader(0, count);
			Record rec1=new Record(schema.getColumnCount());
			Record rec2=new Record(schema.getColumnCount());
			rec2=reader.read();
//			Record r = new Record(schema.getColumnCount());
			DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			
			while ((rec1 = reader.read()) != null) {
				//20140603@author bailu
				if(rec2!=null){
				rec2=reader.read();				
				String value1=consumeRecord(rec1,schema);
				String value2=consumeRecord(rec2,schema);
				String[] column1=value1.split("#");
				String[] column2=value2.split("#");
					if(column1[0]==column2[0]){
						Double distance=evaluate(Double.parseDouble(column1[2]),Double.parseDouble(column1[3]),Double.parseDouble(column2[2]),Double.parseDouble(column2[3]));
						Date gameDate1 = dateFormat.parse(column2[1]);
						Date gameDate2=dateFormat.parse(column1[1]);
						//高铁最高时速按300km/h这算为m/s,时间差的计算方法,5s的时间内移动的距离大于416.66666699999996
						if(gameDate1.getTime()-gameDate2.getTime()<5000&&distance>416.66666699999996){
							
						}
					}
				}
				}
			reader.close();
			down.complete();
		} catch (TunnelException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
private  static String consumeRecord(Record r, RecordSchema schema) {
		//表中字段顺序imei,accesstime,lat,lon
		String colValue = null;
		for (int i = 0; i < schema.getColumnCount(); i++) {
			Column.Type t = schema.getColumnType(i);
			
			switch (t) {
			case ODPS_BIGINT: {
				Long v = r.getBigint(i);
				colValue = v == null? null : v.toString();
				break;
			}
			case ODPS_DOUBLE: {
				Double v = r.getDouble(i);
				colValue = v == null ? null : v.toString();
				break;
			}
			case ODPS_DATETIME: {
				Date v = r.getDatetime(i);
				colValue = v == null ? null : v.toString();
				break;
			}
			case ODPS_BOOLEAN: {
				Boolean v = r.getBoolean(i);
				colValue = v == null ? null : v.toString();
				break;
			}
			case ODPS_STRING:
				String v = r.getString(i);
				colValue = v == null ? null : v.toString();
				break;
			default:
				throw new RuntimeException("Unknown column type: " + t);
			}
//			System.out.print(colValue == null? "null" : colValue);
//			if(i != schema.getColumnCount())
//				System.out.print("\t");
			//添加分隔符#便于获取imei，lon，lat
			colValue=colValue+"#";			
		}
//		System.out.println();
		return colValue;
		
	}

public static double EARTH_RADIUS = 6378137; 
private static double rad(double d) 
{ 
   return d * Math.PI / 180.0; 
} 
public static Double evaluate(Double lat1, Double lng1, Double lat2, Double lng2) {
	
	double radLat1 = rad(lat1); 
    double radLat2 = rad(lat2); 
    double a = radLat1 - radLat2; 
    double b = rad(lng1) - rad(lng2); 
    double s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a/2),2) + 
    Math.cos(radLat1)*Math.cos(radLat2)*Math.pow(Math.sin(b/2),2))); 
    s = s * EARTH_RADIUS; 
    return s; 
}
}
