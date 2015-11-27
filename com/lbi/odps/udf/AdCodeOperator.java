package com.lbi.odps.udf;

import java.util.HashMap;

import com.aliyun.odps.udf.UDF;

public class AdCodeOperator extends UDF{
	private String[] adcodes = {"北京","上海","广州","深圳","杭州"};
	private HashMap<String,String> adcodeMap = null;
	public String evaluate(String desc) {
		if(adcodeMap==null){
			adcodeMap = new HashMap<String,String>();
			adcodeMap.put("北京", "1100");
			adcodeMap.put("上海", "3100");
			adcodeMap.put("广州", "4401");
			adcodeMap.put("深圳", "4403");
			adcodeMap.put("杭州", "3301");
		}
		String[] de = desc.split(" ");
		for(String prov : adcodes)
		{
			if((de.length > 0 && de[0].indexOf(prov)!=-1) || (de.length > 1 && de[1].indexOf(prov) != -1))
			{
				return adcodeMap.get(prov);
			}
		}
		return "-1";
	}
}
