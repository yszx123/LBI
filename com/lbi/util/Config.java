package com.lbi.util;

import java.io.InputStream;
import java.util.Properties;
/**
 * 获取系统配置参数
 * @author liumk
 *
 */
public class Config {
	private static final String configFile = "system.properties";
    private static Properties properties = new Properties();
    
    public static String getConfig(String key) {
    	try {
    	    if (properties.isEmpty()) {
    		    InputStream is = Config.class.getClassLoader().getResourceAsStream(configFile);
    	        properties.load(is);
    	    }
    	}
    	catch (Exception e) {
    		e.printStackTrace();
    	}
    	return properties.getProperty(key);
    }
    
//    public static String getConfig(String key, String defaultValue) {
//    	String result = getConfig(key);
//    	if (StringUtil.isEmpty(result)) result = defaultValue;
//    	return result;
//    }
    public static String getValue(String key) {
    	String result = getConfig(key);
    	return result;
    }
}
