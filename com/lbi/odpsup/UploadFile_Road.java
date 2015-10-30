package com.autonavi.odpsup;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

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
//@author bailu
public class UploadFile_Road {

	private static String endpoint = "http://dt-ext.odps.aliyun-inc.com";// "http://dt.odps.aliyun.com";
	private static String accessId = "2w1mbUbPwhgbqLnu";
	private static String accessKey = "zpW6QAd30aDJYBuIeu1XrcCyoxllhE";

	private static String project = "autonavi";
	private static String table = "road";
	private static String partition = "ds=20140417";// "ds=20140417";

	public static void main(String[] args) {
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
			
			/*
			 * String flname = "transit_stations.csv";
			 *  File fl = new File(PropertiesUtil.getValue("csvpath") + "/synchronization/" +
			 * flname);
			 */
			// 获取目录下所有文件名14.4.24@author bailu
			String filepath = PropertiesUtil.getValue("csvpath")
					+ "/synchronization/road/";
			File file = new File(filepath);
			File[] files = file.listFiles();			
			
			for (File f : files) {				
				File fl = new File(filepath + f.getName());	
				//System.out.println(fl.exists());为什么是false？？？？？,你少加了一个/路径
				if (fl.exists()) {
					
					BufferedReader br = new BufferedReader(new FileReader(fl));
					String line = "";
					int count = 0;
					while ((line = br.readLine()) != null) {
						count++;
						if (count == 1) {
							
							//System.out.println(line);
							continue;
						}
						// 2014.4.24 author bailu
						String[] column = line.split(",", 9);
						for (int i = 0; i < schema.getColumnCount(); i++) {
							// 2014.4.24 author bailu
							/*if (column.length != 9) {
								System.out.println(line);
								continue;
							}*/
							Column.Type t = schema.getColumnType(i);
							//System.out.println(t);
							switch (t) {
							case ODPS_DOUBLE:
								r.setDouble(i, Double.parseDouble(column[i]));
								break;
							case ODPS_STRING:
								String value = column[i];
								r.setString(i, value);
								break;
							// 2014.4.24 author bailu
							case ODPS_BIGINT:
								r.setBigint(i, Long.parseLong(column[i]));
							//default:
								//throw new RuntimeException(
										//"Unknown column type: " + t);
							}
						}
						writer.write(r);
					}
					System.out.println(f.getName()+"\trecord count " + count);
					System.out.println(f.getName()+"\tall time === "+
							(System.currentTimeMillis() - time));					
				}				
			}
			writer.close();			
			up.complete();
		} catch (TunnelException e) {
			System.out.println(e);
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
