package com.autonavi.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


public class CollectAddressService {

	public CollectAddressService() {
		// TODO Auto-generated constructor stub
	}

	public void loadData() {
		boolean bl1 = false;
		FtpUtil ftp = null;
		// String flname="result_"+this.GetNowDate()+".csv";
		String flname = "result_20131109.csv";
		File fl = new File(PropertiesUtil.getValue("csvpath")
				+ "/synchronization/" + flname);
		if (fl.exists()) {
			bl1 = true;
		} else {
			ftp = new FtpUtil(Config.getValue("ftpUser"),
					Config.getValue("ftpPwd"));
			bl1 = ftp.loadFile("/export_gain/" + flname,
					PropertiesUtil.getValue("csvpath") + "/synchronization/"
							+ flname);
		}
		if (bl1) {
			// csv下载成功 入库
			List<String> list = readCsvJAVA(PropertiesUtil.getValue("csvpath")
					+ "/synchronization/" + flname);
		}
		if (ftp != null) {
			ftp.closeServer();
		}
	}

	public List<String> readCsvJAVA(String fromPath) {
		List<String> list = new ArrayList();
		OutputStreamWriter pw = null;
		try {
			File csv = new File(fromPath); // CSV文件
			InputStreamReader isr = new InputStreamReader(new FileInputStream(
					csv), "GBK");
			BufferedReader br = new BufferedReader(isr);
			// 读取直到最后一行

			String line = "";
			while ((line = br.readLine()) != null) {
				list.add(line);
			}
			br.close();
			// pw =new OutputStreamWriter(new FileOutputStream(list),"UTF-8");
			// pw.write(list);
			// System.out.println("dsfdfsaddffd======"+pw.toString());
			// pw.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return list;
	}

	public String GetNowDate() {
		String temp_str = "";
		Date dt = new Date();
		// 最后的aa表示“上午”或“下午” HH表示24小时制 如果换成hh表示12小时制
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		temp_str = sdf.format(dt);
		return temp_str;
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		// new CollectAddressService().loadCaijiData();
		new CollectAddressService().loadData();
		// AnsiToUtf8.utf();
		// UploadSample.upload();
	}

}
