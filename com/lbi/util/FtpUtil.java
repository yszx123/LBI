package com.autonavi.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketException;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;


public class FtpUtil {
	private FTPClient ftpClient;
	private String ftp_ip;
	private String ftp_user;
	private String ftp_pwd;
	
	public FtpUtil(String ftp_ip, String ftp_user, String ftp_pwd) {
		super();
		this.ftp_ip = ftp_ip;
		this.ftp_user = ftp_user;
		this.ftp_pwd = ftp_pwd;
		try {
			connectServer();
		} catch (Exception e) {
			//e.printStackTrace();
		}
	}
	
	public FtpUtil(String ftp_user, String ftp_pwd) {
		super();
		this.ftp_user = ftp_user;
		this.ftp_pwd = ftp_pwd;
		try {
			connectServer();
		} catch (Exception e) {
			//e.printStackTrace();
		}
	}
	
	private void connectServer() throws Exception {
		ftpClient = new FTPClient();
		System.out.println( "connect to ftp server..." );
		//ftpClient.setDefaultTimeout(Integer.parseInt(Config.getValue("ftpTimeout")));
		ftpClient.setControlEncoding(Config.getValue("ftpEncoding"));
		int ftpAttemptCount = Integer.parseInt(Config.getValue("ftpAttemptCount"));
		int count = 1;
		boolean isLogin = true;
		while (count <= ftpAttemptCount) {
			isLogin = connect();
			if (isLogin) {
				System.out.println(ftpClient.getLocalAddress() + "连接 " + ftpClient.getRemoteAddress() + " FTP服务器成功 ！");
				break;
			} else {
				Thread.sleep(Long.parseLong(Config.getValue("ftpAttemptInterval")));
				count++;
			}
		}
		if (isLogin == false) {
			throw new Exception(ftpClient.getLocalAddress() + "连接 " + ftpClient.getRemoteAddress() +" FTP服务器失败 尝试次数" + (count - 1));
		}
	}
	
	private boolean connect() {
		boolean isLogin = false;
		try {
				ftpClient.connect(Config.getValue("ftpServer"), Integer.parseInt(Config.getValue("ftpPort")));
			    System.out.println("Connected to "+ ftpClient.getRemoteAddress() + ".");
			    int reply = ftpClient.getReplyCode();
			if (FTPReply.isPositiveCompletion(reply)) {
					isLogin = ftpClient.login(ftp_user,ftp_pwd);
			}
			if(isLogin){
				// 设置被动模式    
		        ftpClient.enterLocalPassiveMode();   
		         // 设置以二进制方式传输    
		        ftpClient.setFileType(FTP.BINARY_FILE_TYPE);   
			}else{
				ftpClient.disconnect();   
			}
		} catch (SocketException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return isLogin;
	}
	
	public void closeServer(){
		if (ftpClient.isConnected()) {
			try {
				ftpClient.logout();
			} catch (IOException e) {
				e.printStackTrace();
			}
			try {
				ftpClient.disconnect();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	private boolean isConnected(){
		boolean isConnected = true;
		if(!ftpClient.isConnected()){
			try{
				connectServer();
			}catch(Exception e){
				isConnected =false;
			}
		}
		return isConnected;
	}
	
	public FTPFile[] queryCSV(String picname){
		FTPFile[] ftpArr = null;
		if(isConnected()){
			try {
				ftpArr = ftpClient.listFiles("*.CSV");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return ftpArr;
	}
	
	//下载文件到本地
	 public boolean loadFile(String remoteFileName,String localFileName){
		 boolean flag=false;
		 if(isConnected()){
        	//下载文件
             BufferedOutputStream buffOut=null;
            try{
                 buffOut=new BufferedOutputStream(new FileOutputStream(localFileName));
                 ftpClient.retrieveFile(remoteFileName, buffOut);
                 flag=true;
             }catch(Exception e){
                 e.printStackTrace();
             }finally{
                try{
                    if(buffOut!=null)
                         buffOut.close();
                 }catch(Exception e){
                     e.printStackTrace();
                 }
             }
         }
         return flag;
     }
	 
	 //切换文件夹
	 public void changeWorkingDirectory(String directory){
		 if(isConnected()){
			 try{
	             ftpClient.changeWorkingDirectory(directory);
	         }catch(IOException ioe){
	             ioe.printStackTrace();
	         }
		 }
		 
	 }
	 
	 //切换上一级
	 public void changeToParentDirectory(){
		 if(isConnected()){
			 try{
	             ftpClient.changeToParentDirectory();
	         }catch(IOException ioe){
	             ioe.printStackTrace();
	         }
		 }
	  }
	 
	 public void deleteFile(String filename){
		 if(isConnected()){
			 try{
	             ftpClient.deleteFile(filename);
	         }catch(IOException ioe){
	             ioe.printStackTrace();
	         }
		 }
	  }
	 
	public void renameFile(String oldFileName,String newFileName){
		if(isConnected()){
			try{
	             ftpClient.rename(oldFileName, newFileName);
	         }catch(IOException ioe){
	             ioe.printStackTrace();
	         }
		}
	  }
	
	public void uploadFile(String localFilePath,String newFileName){
       //上传文件
		if(isConnected()){
			BufferedInputStream buffIn=null;
			 try{
		            buffIn=new BufferedInputStream(new FileInputStream(localFilePath));
		            ftpClient.storeFile(newFileName, buffIn);
		        }catch(Exception e){
		            e.printStackTrace();
		        }finally{
		           try{
		               if(buffIn!=null)
		                    buffIn.close();
		            }catch(Exception e){
		                e.printStackTrace();
		            }
		        }
		}
    }
//	public String download(HttpServletRequest request,HttpServletResponse response, String filePath) {
//		String message = "";
//		InputStream is = null;
//		OutputStream os = null;
//		if(isConnected()){
//			try {
//				is = ftpClient.retrieveFileStream(filePath);
//				if(is == null){
//					message = "文件不存在";
//				} else {
//					try {
//						is.close();
//						ftpClient.completePendingCommand();
//					} catch (IOException e) {
//						e.printStackTrace();
//					}
//					os = response.getOutputStream();
//					response.addHeader("Content-Disposition",
//							"attachment;filename="
//							+ processFileName(filePath.substring(filePath.lastIndexOf("\\")+1),request.getHeader("USER-AGENT")));
//					ftpClient.retrieveFile(filePath, os);
//				}
//			} catch (IOException e) {
//				//e.printStackTrace();
//				message = "下载失败";
//			}finally {
//				if (os != null) {
//					try {
//						os.close();
//					} catch (IOException e) {
//						//e.printStackTrace();
//					}
//				}
//				closeServer();
//			}
//		}else{
//			message = "FTP连接失败";
//		}
//		return message;
//	}
//	
	private String processFileName(String fileName, String agent)throws IOException {
		String codedfilename = null;
		if (null != agent && -1 != agent.indexOf("MSIE")) {
			String prefix = fileName.lastIndexOf(".") != -1 ? fileName
					.substring(0, fileName.lastIndexOf(".")) : fileName;
			String extension = fileName.lastIndexOf(".") != -1 ? fileName
					.substring(fileName.lastIndexOf(".")) : "";
			String name = java.net.URLEncoder.encode(prefix, "UTF8");
			if (name.lastIndexOf("%0A") != -1) {
				name = name.substring(0, name.length() - 3);
			}
			int limit = 150 - extension.length();
			if (name.length() > limit) {
				name = java.net.URLEncoder.encode(prefix.substring(0, Math.min(
						prefix.length(), limit / 9)), "UTF-8");
				if (name.lastIndexOf("%0A") != -1) {
					name = name.substring(0, name.length() - 3);
				}
			}

			codedfilename = name + extension;
		} else if (null != agent && -1 != agent.indexOf("Mozilla")) {
			codedfilename = "=?UTF-8?B?"
					+ (new String(org.apache.commons.codec.binary.Base64
							.encodeBase64(fileName.getBytes("UTF-8")))) + "?=";
		} else {
			codedfilename = fileName;
		}
		return codedfilename; 
	}
	
//	static class MyFilter implements FilenameFilter{  
//        private String type;  
//        public MyFilter(String type){  
//            this.type = type;  
//        }  
//        public boolean accept(File dir,String name){  
//            return name.endsWith(type);  
//        }  
//    }
	
	public static void main(String[] args){
		FtpUtil fu=new FtpUtil("adminfrp","mapabc");
		boolean bl=fu.loadFile("wms.war", PropertiesUtil.getValue("csvpath")+"/synchronization/wms.war");
		if(bl){
			System.out.println("下载完成");
		}else{
			System.out.println("下载失败");
		}
		
	}
}
