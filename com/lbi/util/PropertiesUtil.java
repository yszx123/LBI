package com.lbi.util;

import java.io.InputStream;
import java.util.Properties;

public class PropertiesUtil{
	private static Properties env ;
	
	static{
		try{
			env = new Properties();										
			InputStream is = PropertiesUtil.class.getClassLoader().getResourceAsStream("cache.properties");
			env.load( is );
			is.close();
			System.out.println( "get info from config file  -->cache.properties" );
		}catch( Exception e ){
				e.printStackTrace();
				throw new ExceptionInInitializerError( e ) ;
		}
	}
	
	/**
	 * yun.properties 
	 * @param key
	 * @return
	 * @throws Exception
	 */
	public static String getValue(String key){
		return env.getProperty(key);
	}	

}