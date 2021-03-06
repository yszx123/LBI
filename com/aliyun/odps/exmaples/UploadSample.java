package com.aliyun.odps.exmaples;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;

import com.alibaba.odps.tunnel.Column;
import com.alibaba.odps.tunnel.Configuration;
import com.alibaba.odps.tunnel.DataTunnel;
import com.alibaba.odps.tunnel.RecordSchema;
import com.alibaba.odps.tunnel.TunnelException;
import com.alibaba.odps.tunnel.Upload;
import com.alibaba.odps.tunnel.Upload.Status;
import com.alibaba.odps.tunnel.io.Record;
import com.alibaba.odps.tunnel.io.RecordWriter;
import com.autonavi.util.PropertiesUtil;

public class UploadSample {
	private static String endpoint = "http://dt.odps.aliyun.com";
	private static String accessId = "2w1mbUbPwhgbqLnu";
	private static String accessKey = "zpW6QAd30aDJYBuIeu1XrcCyoxllhE";

	private static String project = "autonavi";
	private static String table = "streetno_info";
	private static String partition = null;

	public static void main(String args[]) {
//	public static void upload(){
		long time = System.currentTimeMillis();
		Configuration cfg = new Configuration(accessId, accessKey, endpoint);
		DataTunnel tunnel = new DataTunnel(cfg);

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
			
			String flname="streetno.csv";
			File fl=new File(PropertiesUtil.getValue("csvpath")+"/synchronization/"+flname);
			if(fl.exists()){
				BufferedReader br = new BufferedReader(new FileReader(fl));
				String line = "";
				int count = 0;
				while((line = br.readLine()) != null){
					count++;
					if(count==100000) System.out.println("ex 100000 time : "+(System.currentTimeMillis()-time));
					int j = 0;
					if(count==1) continue;
					
					String[] column = line.split(",",13);
					
					for (int i = 1; i < schema.getColumnCount(); i++) {

						if (2 == i) {
							r.setString(i, "RoadNo");
							continue;
						}
						if (5 == i || 10 == i)
							continue;
						Column.Type t = schema.getColumnType(i);
						j++;
						switch (t) {
						case ODPS_BIGINT:
							r.setBigint(i, Long.parseLong(column[j-1]));
							break;
						case ODPS_DOUBLE:
							r.setDouble(i, Double.parseDouble(column[j-1]));
							break;
						case ODPS_DATETIME:
							r.setDatetime(i, new Date());
							break;
						case ODPS_BOOLEAN:
							r.setBoolean(i,
									Boolean.parseBoolean(column[j-1]));
							break;
						case ODPS_STRING:
							r.setString(i, column[j-1]);
							break;
						default:
							throw new RuntimeException("Unknown column type: "
									+ t);
						}
					}
				
					writer.write(r);
				}
				System.out.println("record count "+ count);
				System.out.println("all time === "+(System.currentTimeMillis()-time));
				
//				writer.write(r);	
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

//StringTokenizer st = new StringTokenizer(line,",");
//while(st.hasMoreTokens()){
//st.nextToken();
//
//}



//for (int i = 0; i < schema.getColumnCount(); i++) {
//	Column.Type t = schema.getColumnType(i);
//	switch (t) {
//	case ODPS_BIGINT:
//		r.setBigint(i, Long.parseLong(column[i]));
//		break;
//	case ODPS_DOUBLE:
//		r.setDouble(i, Double.parseDouble(column[i]));
//		break;
//	case ODPS_DATETIME:
//		r.setDatetime(i,new Date() );
//		break;
////	case ODPS_BOOLEAN:
////		r.setBoolean(i, true);
////		break;
//	case ODPS_STRING:
//		r.setString(i, column[i]);
//		break;
//	default:
//		throw new RuntimeException("Unknown column type: " + t);
//	}
//}	