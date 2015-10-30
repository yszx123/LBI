package com.autonavi.lbi;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import com.aliyun.odps.Record;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.mapreduce.ReduceContext;
import com.aliyun.odps.mapreduce.Reducer;

public class ClusterImeiReducerClass extends Reducer<Text,Text>{

private Record result = null;

@Override
protected void setup(ReduceContext<Text, Text> context) throws IOException,
		InterruptedException {
	result = context.createOutputRecord();
}
	
@Override
public void reduce(Text key, Iterable<Text> values,
		ReduceContext<Text, Text> context) throws IOException, InterruptedException {
	
	HashSet<String> hsClu = new HashSet<String>();
	HashSet<String> hsLoc = new HashSet<String>();

	
	for (Text val : values) {
		
		if(val.toString().indexOf("000:")!=-1){
			String c = val.toString().replace("000:", "");
			if(!hsClu.contains(c)){
				hsClu.add(c);
			}
		}else if(val.toString().indexOf("111:")!=-1){
			String l = val.toString().replace("111:", "");
			if(!hsLoc.contains(l)){
				hsLoc.add(l);
			}
		}
	}
	
	if(hsClu.size()>0 && hsLoc.size()>0){
		System.out.println("hsClu:"+hsClu.size()+" hsLoc:"+hsLoc.size());
		if(hsClu.size()>2){
			Iterator<String> it = hsClu.iterator();
			while(it.hasNext()){
				System.out.println("it: "+it.next());
			}
		}
		
		Object[] clu = hsClu.toArray();
		Object[] loc = hsLoc.toArray();
		for(int i=0;i<clu.length;i++){
			for(int j=0;j<loc.length;j++){
				result.set(0,new Text(clu[i].toString()));
				String[] imei_time = loc[j].toString().split("_");
				result.set(1,new Text(imei_time[0]));
				result.set(2,new Text(imei_time[1]));
				context.write(result);
			}
		}
	}
}
}

