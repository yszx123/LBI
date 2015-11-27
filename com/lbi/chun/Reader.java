package com.lbi.chun;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import com.alibaba.odps.tunnel.Column;
import com.alibaba.odps.tunnel.RecordSchema;
import com.alibaba.odps.tunnel.Upload;
import com.alibaba.odps.tunnel.io.Record;

public class Reader extends Thread {

	private File file;
	private Upload up;
	
	public Reader(Upload up,File file) {
		this.file = file;
		this.up=up;
	}
	
	@Override
	public void run() {
		try {
			RecordSchema schema = up.getSchema();
			if (file.isFile()) {
				long time = System.currentTimeMillis();
				System.out.println("start read file : "+file.getName());
				int count = 0;
				String line = "";
				BufferedReader br = new BufferedReader(new FileReader(file));
				while ((line = br.readLine()) != null) {
					Record r = new Record(schema.getColumnCount());
					String[] cols = line.split("", 8);
					for (int i = 0; i < schema.getColumnCount(); i++) {
						Column.Type t = schema.getColumnType(i);
						switch (t) {
						case ODPS_DOUBLE:
								//need add logic
								r.setDouble(i, Double.parseDouble(cols[i]==null || cols[i].equalsIgnoreCase("null") 
								|| cols[i].equalsIgnoreCase("\\N") ?"0":cols[i]));
							break;
						case ODPS_STRING:
							r.setString(i, cols[i]==null?"":cols[i]);
							break;
						case ODPS_BIGINT:
							r.setBigint(i, Long.parseLong(cols[i]));
							break;
						default:
							throw new RuntimeException("Unknown column type: "
									+ t);
						}
					}
					Cache.stack.offer(r);
					count++;
					if(count%1000000==0){
						System.out.println("read count: "+count+" read r: "+r);
					}
				}
				System.out.println("******* read count *******"+count+"\n"+(System.currentTimeMillis()-time));
				br.close();
			}
		} catch (Exception e) {
			System.out.println("Read file [" + file + "] error "+e);
		}
	}

}
