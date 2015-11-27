package com.lbi.chun;


import java.io.IOException;

import com.alibaba.odps.tunnel.TunnelException;
import com.alibaba.odps.tunnel.Upload;
import com.alibaba.odps.tunnel.io.RecordWriter;
import com.alibaba.odps.tunnel.io.Record;

/**
 * д�ļ��������ϴ� ��File��Ϊodps
 * 
 * @author Liang
 * 
 */
public class Writer extends Thread {
	
	private Upload up;
	private Long index;

	public Writer(Upload up,long index) {
		this.up=up;
		this.index=index;
	}

	@Override
	public void run() {
		RecordWriter writer = null;
		try {
			writer = up.openRecordWriter(index);
			Record r = null;
			int count = 0;
			long time = System.currentTimeMillis();
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			while (true) {
				r=Cache.stack.poll();
				if(r!=null){
					count++;
					writer.write(r);
				}else{
					try {
						Thread.sleep(5000);
						//缓存为空，休息3秒，如果缓存仍为空，标示读进程已经结束，则表现文件已经读完
						if(Cache.stack.isEmpty()){
							System.out.println("Stop Writer");
//							writer.close();
							break;
						}
					}catch (InterruptedException e) {
						System.out.println("Sleep error"+e);
					}
				}
				if(count%1000000==0){
					System.out.println("write count "+count+"\n"+(System.currentTimeMillis()-time)+" writer r: "+r);
				}
			}
		} catch (TunnelException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
