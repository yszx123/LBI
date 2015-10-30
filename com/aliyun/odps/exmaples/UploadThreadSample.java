package com.aliyun.odps.exmaples;

import java.io.IOException;
import java.util.Date;

import com.alibaba.odps.tunnel.Column;
import com.alibaba.odps.tunnel.Configuration;
import com.alibaba.odps.tunnel.DataTunnel;
import com.alibaba.odps.tunnel.RecordSchema;
import com.alibaba.odps.tunnel.TunnelException;
import com.alibaba.odps.tunnel.Upload;
import com.alibaba.odps.tunnel.io.Record;
import com.alibaba.odps.tunnel.io.RecordWriter;

class UploadThread extends Thread{
	private Upload up;
	private long index;
	
	public UploadThread(Upload up, long index) {
		this.up = up;
		this.index = index;
	}

	public void run() {
		try {
			RecordWriter writer = up.openRecordWriter(index);
	        Record r = buildRecord(up.getSchema());
	        for(int i = 0; i < 10; i++) {
	            writer.write(r);
	        }
	        writer.close();
		} catch (TunnelException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private Record buildRecord(RecordSchema schema) throws TunnelException{
		Record r = new Record(schema.getColumnCount());
		for (int i = 0; i < schema.getColumnCount(); i++) {
			Column.Type t = schema.getColumnType(i);
			switch (t) {
			case ODPS_BIGINT:
				r.setBigint(i, 1L);
				break;
			case ODPS_DOUBLE:
				r.setDouble(i, 0.0);
				break;
			case ODPS_DATETIME:
				r.setDatetime(i, new Date());
				break;
			case ODPS_BOOLEAN:
				r.setBoolean(i, true);
				break;
			case ODPS_STRING:
				r.setString(i, "sample code");
				break;
			default:
				throw new RuntimeException("Unknown column type: " + t);
			}
		}
		return r;
	}
}

public class UploadThreadSample {

	private static String endpoint = "http://tunnelserver/";
	private static String accessId = "<your access id>";
	private static String accessKey = "<your access Key>";

	private static String project = "<your project>";
	private static String table = "<your table name>";
	private static String partition = "<your partition spec>";
	
	public static void main(String args[]) {
		Configuration cfg = new Configuration(accessId, accessKey, endpoint);
		DataTunnel tunnel = new DataTunnel(cfg);

		try {
			Upload up = tunnel.createUpload(project, table, partition);
			String id = up.getUploadId();
			UploadThread[] threads = new UploadThread[10];
			for (int i = 0; i < 10; i++) {
				Upload upload = tunnel.createUpload(project, table, partition, id);
				threads[i] = new UploadThread(upload, i);
				threads[i].start();
			}
			for (int i = 0; i < 10; i++) {
				threads[i].join();
			}
			up.complete();
		} catch (TunnelException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
