package com.lbi.odpsup;

import java.io.FileOutputStream;
import java.io.IOException;
import com.alibaba.odps.tunnel.Configuration;
import com.alibaba.odps.tunnel.DataTunnel;
import com.alibaba.odps.tunnel.Download;
import com.alibaba.odps.tunnel.TunnelException;
import com.alibaba.odps.tunnel.io.Record;
import com.alibaba.odps.tunnel.io.RecordReader;
import com.alibaba.odps.tunnel.io.TextRecordWriter;

class DownloadThread extends Thread{
	private long start;
	private long count;
	private Download down;
	private int index;
	
	public DownloadThread(Download down, long start, long count, int index) {
		this.start = start;
		this.count = count;
		this.down = down;
		this.index = index;
	}

	public void run() {
		try {
			RecordReader reader = down.openRecordReader(start, count);
			FileOutputStream out = new FileOutputStream(index + ".txt");
	        TextRecordWriter writer = new TextRecordWriter(out, down.getSchema(), '@', '\n');
	        Record r = null;
	        while ((r = reader.read()) != null) {
	            writer.write(r);
	        }
	        reader.close();
	        writer.close();
		} catch (TunnelException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

public class DownTable {	
	private static String endpoint = "http://dt.odps.aliyun.com";//"http://dt.odps.aliyun.com";
	private static String accessId = "2w1mbUbPwhgbqLnu";
	private static String accessKey = "zpW6QAd30aDJYBuIeu1XrcCyoxllhE";

	private static String project = "lbi";
	private static String table = "s_lbi_location_log_cluster";
	private static String partition = "ds=";//"ds=20140110";
	
	public static void main(String args[]) {
		Configuration cfg = new Configuration(accessId, accessKey, endpoint);
		DataTunnel tunnel = new DataTunnel(cfg);
		
		partition = partition + args[1];
		table = args[0];
		try {
			Download down = tunnel.createDownload(project, table, partition);
			String id = down.getDownloadId();
			long count = down.getRecordCount();
			DownloadThread[] threads = new DownloadThread[10];
			long step = count / 10;
			for (int i = 0; i < 10; i++) {
				Download download = tunnel.createDownload(project, table, partition, id);
				if(i==9)
				{
					threads[i] = new DownloadThread(download, step * i, count - step*i, i);
				}else{
					threads[i] = new DownloadThread(download, step * i, step, i);
				}				
				threads[i].start();
			}
			for (int i = 0; i < 10; i++) {
				threads[i].join();
			}
			down.complete();
			
		} catch (TunnelException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
