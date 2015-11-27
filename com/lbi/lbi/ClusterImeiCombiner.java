//package com.lbi.lbi;
//
//import java.io.IOException;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.Iterator;
//
//import com.aliyun.odps.io.Text;
//import com.aliyun.odps.mapreduce.CombineContext;
//import com.aliyun.odps.mapreduce.Combiner;
//
//public class ClusterImeiCombiner extends Combiner<Text,Text>{
//	
//	protected void combine(Text key, Iterable<Text> values, CombineContext<Text, Text> context)
//            throws IOException,InterruptedException{
//		Text clu_loc_time = new Text();
//		HashSet<Text> hsLoc = new HashSet<Text>();
//		HashSet<Text> hsClu = new HashSet<Text>();
//		
//		for(Text val:values){
//			if(val.toString().indexOf("111")!=-1){
//				if(!hsLoc.contains(val)){
//					hsLoc.add(val);
//				}
//				
//			}else if(val.toString().indexOf("000")!=-1){
//				if(!hsClu.contains(val)){
//					hsClu.add(val);
//				}
//			}
//			
//		}
//		
//		if(hsClu.size()>0 && hsLoc.size()>0){
//			Iterator<Text> itClu = hsClu.iterator();
//			Iterator<Text> itLoc = hsClu.iterator();
//			while(itClu.hasNext()){
//				Text cluster = itClu.next();
//				while(itLoc.hasNext()){
//					Text location = new Text();
//					clu_loc_time.set(cluster.toString().replace("000:", "")+"_"+location.toString().replace("111:", ""));
//					context.write(key,clu_loc_time);
//				}
//		}
//		
//		}
//	
//	}
//}



//reducer
//	for(Text val : values){
//		String[] c_l_t = val.toString().split("_");
//		result.set(0,new Text(c_l_t[0]));
//		result.set(1,new Text(c_l_t[1]));
//		result.set(2,new Text(c_l_t[2]));
//		context.write(result);
//	}