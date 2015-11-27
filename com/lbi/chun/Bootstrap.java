package com.lbi.chun;


import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;

import com.alibaba.odps.tunnel.Configuration;
import com.alibaba.odps.tunnel.DataTunnel;
import com.alibaba.odps.tunnel.Upload;
public class Bootstrap {

	private static String endpoint = "http://dt-ext.odps.aliyun-inc.com";//"http://dt.odps.aliyun.com";
	private static String accessId = "2w1mbUbPwhgbqLnu";
	private static String accessKey = "zpW6QAd30aDJYBuIeu1XrcCyoxllhE";

	private static String project = "lbi";
	private static String table ="s_lbi_address_location_log_utf_8";//s_lbi_address_location_log_mul
	private static String partition = "ds=";

	public static final AtomicBoolean running = new AtomicBoolean(false);
	public static final AtomicBoolean readerover = new AtomicBoolean(false);

	public static void main(String[] args) {
		 
		long time = System.currentTimeMillis();
		File srcFile = new File(args[0]);
		File srcFile1 = new File(args[1]);
		File[] files = { srcFile,srcFile1};
        partition = partition + args[2];
		Configuration cfg = new Configuration(accessId, accessKey, endpoint);
		DataTunnel tunnel = new DataTunnel(cfg);

		try {
			System.out.println("partition:"+partition);
			System.out.println("filename print0: "+srcFile.getName()+" "+srcFile1.getName());
			Upload up = tunnel.createUpload(project, table, partition);
			String id = up.getUploadId();
			System.out.println("filename print: "+srcFile.getName()+" "+srcFile1.getName());
			// 多线程读文件，写到缓存队列中
			Reader[] read=new Reader[2];
			Thread[] readThread = new Thread[2];
			for (int i = 0; i < files.length; i++) {
//				Reader read = new Reader(up, files[i]);
//				Thread readThread = new Thread(read);
//				readThread.setDaemon(true);
//				readThread.start();
				System.out.println("files " + i + files[i].getName());
				read[i] = new Reader(up, files[i]);
				readThread[i] = new Thread(read[i]);
				readThread[i].setDaemon(true);
				readThread[i].start();
			}
			
			Writer[] writer = new Writer[15];
			Thread[] writerThread = new Thread[15];
//			List<Long> ids=new ArrayList<Long>();
			for (int i = 0; i < 15; i++) {
//				ids.add(Long.parseLong(String.valueOf(i)));
				Upload upload = tunnel.createUpload(project, table, partition,
						id);
				// 监听线程，不断从队列中读数据写到目的文件
				writer[i] = new Writer( upload, i);
				writerThread[i]=new Thread(writer[i]);
				writerThread[i].start();
			}
//			Long[] flagList = new Long[ids.size()];
			for(int i=0;i<15;i++){
				writerThread[i].join();
			}
			up.complete();//ids.toArray(flagList)
		} catch (Exception e) {
			System.out.println("try exception: "+e.getMessage());
		}finally{
			System.out.println("why : ");
		}

		System.out.println("bootstrap time : "+(System.currentTimeMillis() - time)+" "+files[0].getName()+" "+files[1].getName());
	}
}
