package com.lbi.odpsup;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Date;

import com.alibaba.odps.tunnel.Column;
import com.alibaba.odps.tunnel.Configuration;
import com.alibaba.odps.tunnel.DataTunnel;
import com.alibaba.odps.tunnel.RecordSchema;
import com.alibaba.odps.tunnel.Upload;
import com.alibaba.odps.tunnel.Upload.Status;
import com.alibaba.odps.tunnel.io.Record;
import com.alibaba.odps.tunnel.io.RecordWriter;

public class BuildingDB {

	private static String endpoint = "http://dt.odps.aliyun.com";
	private static String accessId = "2w1mbUbPwhgbqLnu";
	private static String accessKey = "zpW6QAd30aDJYBuIeu1XrcCyoxllhE";

	private static String project = "lbi";
	private static String table = "building_street_no";
	private static String partition = null;

	/** PostgreSql数据库连接URL */
	private final static String URL = "jdbc:postgresql://115.28.160.143:5432/postgis";
	/** PostgreSql数据库连接驱动 */
	private final static String DRIVER = "org.postgresql.Driver";
	/** 数据库用户名 */
	private final static String USERNAME = "postgres";
	/** 数据库密码 */
	private final static String PASSWORD = "postgres";

	

	public static void main(String args[]) {
		String sql = "select * from streetno order by rn_id limit 10000 offset " + args[0];
		long t1 = System.currentTimeMillis();
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
			System.out.println("schema.getColumnCount(): "
					+ schema.getColumnCount());

			ResultSet rs = null;
			int count = 0;

			try {
				Class.forName(DRIVER);
				Connection conn = DriverManager.getConnection(URL, USERNAME,
						PASSWORD);
				Statement sm = conn.createStatement();
				System.out.println("success link postgis");
				rs = sm.executeQuery(sql);

			} catch (Exception e) {
				e.printStackTrace();
			}

			while (rs.next()) {

				count++;
				for (int i = 0; i < schema.getColumnCount(); i++) {
					int j = 0;
					if (1 == i || 8 == i)
						continue;
					if (15 == i) {
						r.setString(i, "HouseNo");
						break;
					}

					Column.Type t = schema.getColumnType(i);
					j++;
					switch (t) {
					case ODPS_BIGINT:
						r.setBigint(i, Long.parseLong(rs.getString(j)));
						break;
					case ODPS_DOUBLE:
						r.setDouble(i, Double.parseDouble(rs.getString(j)));
						break;
					case ODPS_DATETIME:
						r.setDatetime(i, new Date());
						break;
					case ODPS_BOOLEAN:
						r.setBoolean(i, Boolean.parseBoolean(rs.getString(j)));
						break;
					case ODPS_STRING:
						r.setString(i, rs.getString(j));
						break;
					default:
						throw new RuntimeException("Unknown column type: " + t);
					}
				}
				writer.write(r);
			}

			writer.close();
			up.complete();
			System.out.println("record count " + count);
			System.out
					.println("all time: " + (System.currentTimeMillis() - t1));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
