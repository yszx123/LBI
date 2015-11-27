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

public class UpDataToODPS {

	private static String endpoint = "http://dt.odps.aliyun.com";
	private static String accessId = "2w1mbUbPwhgbqLnu";
	private static String accessKey = "zpW6QAd30aDJYBuIeu1XrcCyoxllhE";

	private static String project = "lbi";
	private static String table = "add_back01";
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
		long t1 = System.currentTimeMillis();
		int count = 0;

		Configuration cfg = new Configuration(accessId, accessKey, endpoint);
		DataTunnel tunnel = new DataTunnel(cfg);

		for (int m = 0; m < 345; m++) {

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

				ResultSet rs = UpDataToODPS.down(m * 10000);
				System.out.println("m====" + m);
				while (rs.next()) {
					int j = 0;
					count++;
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
							r.setBigint(i, Long.parseLong(rs.getString(j)));
							break;
						case ODPS_DOUBLE:
							r.setDouble(i, Double.parseDouble(rs.getString(j)));
							break;
						case ODPS_DATETIME:
							r.setDatetime(i, new Date());
							break;
						case ODPS_BOOLEAN:
							r.setBoolean(i,
									Boolean.parseBoolean(rs.getString(j)));
							break;
						case ODPS_STRING:
							r.setString(i, rs.getString(j));
							break;
						default:
							throw new RuntimeException("Unknown column type: "
									+ t);
						}
					}
					writer.write(r);
				}

				System.out.println("record count " + count);
				System.out.println("all time: "
						+ (System.currentTimeMillis() - t1));

				writer.close();
				up.complete();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static ResultSet down(int offset) {
		long sqlstart = System.currentTimeMillis();
		String sql = "select * from streetno  rn_id limit 10000 offset "
				+ offset;

		try {
			Class.forName(DRIVER);
			Connection conn = DriverManager.getConnection(URL, USERNAME,
					PASSWORD);
			Statement stmt = conn.createStatement();
			ResultSet rset = stmt.executeQuery(sql);
			System.out.println("sql ex time === "
					+ (System.currentTimeMillis() - sqlstart));

			return rset;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
}
