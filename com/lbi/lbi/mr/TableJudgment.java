package com.lbi.lbi.mr;

import com.aliyun.openservices.ClientConfiguration;
import com.aliyun.openservices.odps.ODPSConnection;
import com.aliyun.openservices.odps.Project;
import com.aliyun.openservices.odps.helper.InstanceRunner;
import com.aliyun.openservices.odps.jobs.SqlTask;
import com.aliyun.openservices.odps.jobs.Task;

public class TableJudgment {
	
	protected static final String ODPS_ENDPOINT = "http://service.odps.aliyun-inc.com/api";//"http://42.121.102.18/api";//"http://dt.odps.aliyun.com";
	private static final String ODPS_ACCESS_ID = "DFTvkXWruShVfqjx";// "2w1mbUbPwhgbqLnu";
	private static final String ODPS_ACCESS_KEY = "zqeHhP3vyqTwczdVdzFulfMH6TZ9Nl";//"zpW6QAd30aDJYBuIeu1XrcCyoxllhE";
	protected static String projectName = "lbi_ods";

	public static boolean judgeDateCount(String date,String table, String prjName)
	{
		projectName = prjName;
		Project prj = new Project(getODPSConnection(), projectName);
		String sql = "count "+table+" partition(ds='"+date+"');";
		try {
			String count = getQueryResult(prj,sql);
			System.out.println(sql + "----" + count);
			count = count.replace("\"", "");
			count = count.replace("\n", "");
			if(count.equalsIgnoreCase("0")){
				return false;
			}			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	private static ODPSConnection getODPSConnection() {
        ClientConfiguration config = new ClientConfiguration();
        String endpoint = ODPS_ENDPOINT;
        return new ODPSConnection(endpoint, ODPS_ACCESS_ID, ODPS_ACCESS_KEY,
                config);
    }
	
	public static String getQueryResult(Project project,String sql)throws Exception{
	        
	    Task task = new SqlTask("SqlTask", sql);
	    InstanceRunner runner = new InstanceRunner(project, task, null, null);
	       
	    return runner.waitForCompletion();
	}
}
