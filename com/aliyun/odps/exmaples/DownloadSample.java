package com.aliyun.odps.exmaples;

import java.io.IOException;
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

public class DownloadSample {

	private static String endpoint = "http://dt.odps.aliyun.com";
	private static String accessId = "2w1mbUbPwhgbqLnu";
	private static String accessKey = "zpW6QAd30aDJYBuIeu1XrcCyoxllhE";

	private static String project = "autonavi";
	private static String table = "location_s";
	private static String partition = "";

	public static void main(String args[]) {
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
			Record r = new Record(schema.getColumnCount());
			while ((r = reader.read()) != null) {
				consumeRecord(r, schema);
			}
			reader.close();
			down.complete();
		} catch (TunnelException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void consumeRecord(Record r, RecordSchema schema) {
		
		for (int i = 0; i < schema.getColumnCount(); i++) {
			Column.Type t = schema.getColumnType(i);
			String colValue = null;
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
			System.out.print(colValue == null? "null" : colValue);
			if(i != schema.getColumnCount())
				System.out.print("\t");
		}
		System.out.println();
	}
}
