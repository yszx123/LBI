package com.autonavi.lbi.mr;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.aliyun.odps.Record;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.TableInfo;
import com.aliyun.odps.io.TableInputFormat;
import com.aliyun.odps.io.TableOutputFormat;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.mapreduce.JobClient;
import com.aliyun.odps.mapreduce.JobConf;
import com.aliyun.odps.mapreduce.MapContext;
import com.aliyun.odps.mapreduce.Mapper;
import com.aliyun.odps.mapreduce.ReduceContext;
import com.aliyun.odps.mapreduce.Reducer;


public class RoadFlowDistDriver {

	public static class RoadFlowDistReducerClass extends Reducer<Text, Text> {

		private Record result = null;
		Map<String,Long> totleMap = new HashMap<String,Long>();
		String[]distArr = null;
		@Override
		protected void setup(ReduceContext<Text, Text> context) throws IOException,InterruptedException {
			result = context.createOutputRecord();
			if(context.getConfiguration().get("key.dist")!=null){
				String dists = context.getConfiguration().get("key.dist");
				distArr = dists.split(",");
			}		
		}
		
		@Override
		public void reduce(Text key, Iterable<Text> values,ReduceContext<Text, Text> context) throws IOException,InterruptedException {
			double rate = 0.0;
			String dist = "";
			Map<String,Long> map = new HashMap<String,Long>();
			for (Text val : values) {
				String[]flowSize = val.toString().split(",");
				for(int i=0;i<flowSize.length;i++){
					if(map.get(distArr[i])!=null){
						map.put(distArr[i], map.get(distArr[i])+Long.parseLong(flowSize[i]));
					}else{
						map.put(distArr[i], Long.parseLong(flowSize[i]));
					}
					if(totleMap.get(distArr[i])!=null){
						totleMap.put(distArr[i], totleMap.get(distArr[i])+Long.parseLong(flowSize[i]));
					}else{
						totleMap.put(distArr[i], Long.parseLong(flowSize[i]));
					}
				}
//				result.set(0, new LongWritable(Integer.parseInt(key.toString().split(":")[0])));
//				result.set(1, new Text(key.toString().split(":")[1]));
//				result.set(2, new LongWritable(flowSize.length));
//				result.set(3, new LongWritable(distArr.length));
//				val.set(val.toString()+"----"+distArr);
//				result.set(4,val);
//				context.write(result);
			}
			boolean flag = true;
			for(int i=1;i<distArr.length;i++){
				if(map.get(distArr[i])==0){
					continue;
				}
				double temp = ((map.get(distArr[i])-map.get(distArr[i-1]))*1.00/map.get(distArr[i]));
				if(rate>temp||flag){
					rate = temp;
					dist =distArr[i-1]+":"+ distArr[i];
					flag = false;
				}
			}
			result.set(0, new LongWritable(Integer.parseInt(key.toString().split(":")[0])));
			result.set(1, new Text(key.toString().split(":")[1]));
			result.set(4, new Text(map.toString()));
			if(dist.length()>0 && dist.split(":").length==2){
				result.set(2, new LongWritable(Integer.parseInt(dist.split(":")[0])));
				result.set(3, new LongWritable(Integer.parseInt(dist.split(":")[1])));
				
			}
			context.write(result);
		}
		@Override
		protected void cleanup(ReduceContext<Text, Text> context) throws IOException,InterruptedException {
			Record result = context.createOutputRecord();
			result.set(4, new Text(totleMap.toString()));
			context.write(result);
		}
	}

	
	public static class RoadFlowDistMapperClass extends Mapper<Text, Text> {
		int size = 0;
		String roads;
		@Override
		public void map(LongWritable recordNum, Record record,MapContext<Text, Text> context) throws IOException {
			Text k = new Text();	
			Text v = new Text();	
			long roadId = Long.parseLong(record.get("road_id").toString());
			String mesh = record.get("mesh").toString();
			String flowsize = "";
			if(size>0){
				for(int i=3;i<size+3;i++){
					flowsize+=","+record.get(i).toString();
				}
				if(roads.indexOf(mesh+":"+roadId) >=0){
					k.set(roadId+":"+mesh);
					v.set(flowsize.substring(1));
					context.write(k, v);
				}
			}
		}
		@Override
		protected void setup(MapContext<Text, Text> context) throws IOException,InterruptedException {
			roads = "J50F002019;35774|J50F001019;44490|J50F001019;44492|J50F001019;44493|J50F001019;3115|J50F001019;44465|J50F001019;46397|J50F001019;46398|J50F001019;27908|J50F001019;1611|J50F001019;1790|J50F001019;2862|J50F001019;3118|J50F001019;15173|J50F001019;42132|J50F001019;44278|J50F001019;44284|J50F001019;44285|J50F001019;44286|J50F001019;44287|J50F001019;44288|J50F001019;44289|J50F001019;48377|J50F001019;48380|J50F001019;48381|J50F001019;48384|J50F001019;48385|J50F001019;48503|J50F001019;48504|J50F002019;35785|J50F002019;35181|J50F002019;35781|J50F002019;35179|J50F002019;35182|J50F002019;35180|J50F002019;35776|J50F002019;35777|J50F001019;36971|J50F001019;44290|J50F001019;44292|J50F001019;44293|J50F001019;48373|J50F001019;48374|J50F001019;38897|J50F001019;38899|J50F001019;44296|J50F001019;44298|J50F001019;44295|J50F001019;44297|J50F001019;48158|J50F001019;48159|J50F001019;48160|J50F001019;2561|J50F001019;2947|J50F001019;2948|J50F001019;18583|J50F001019;44291|J50F001019;48369|J50F001019;48370|J50F002019;5484|J50F002019;10369|J50F002019;10370|J50F002019;35185|J50F002019;35186|J50F002019;35784|J50F002019;38452|J50F002019;38458|J50F002019;38459|J50F002019;38467|J50F002019;38468|J50F001019;1898|J50F001019;5968|J50F001019;6266|J50F001019;6267|J50F001019;11937|J50F001019;14351|J50F001019;3558|J50F001019;3560|J50F001019;3563|J50F001019;3566|J50F001019;30054|J50F001019;32870|J50F001019;44294|J50F001019;44299|J50F001019;44300|J50F001019;44301|J50F001019;44302|J50F001019;44303|J50F001019;44304|J50F001019;44305|J50F001019;44306|J50F001019;44307|J50F001019;44308|J50F001019;44309|J50F001019;47550|J50F001019;48165|J50F001019;48166|J50F001019;48167|J50F001019;48171|J50F001019;48172|J50F001019;48173|J50F001019;48175|J50F001019;48178|J50F001019;48179|J50F001019;48232|J50F001019;48233|J50F002019;1253|J50F002019;3670|J50F002019;3671|J50F002019;3673|J50F002019;3674|J50F002019;4246|J50F002019;35189|J50F002019;35190|J50F002019;35191|J50F002019;35787|J50F002019;35788|J50F002019;35789|J50F002019;38460|J50F002019;38461|J50F002019;38464|J50F002019;38465|J50F002019;38466|J50F002019;38471|J50F002019;38472|J50F001019;1478|J50F001019;5372|J50F001019;5969|J50F001019;6956|J50F001019;11946|J50F001019;29222|J50F001019;29227|J50F001019;42643|J50F001019;42644|J50F001019;44310|J50F001019;44311|J50F001019;44312|J50F001019;44313|J50F001019;44315|J50F001019;48182|J50F001019;48183|J50F001019;48184|J50F001019;48189|J50F001019;48190|J50F001019;48237|J50F001019;48238|J50F002019;3302|J50F002019;3666|J50F002019;5471|J50F002019;35196|J50F002019;35197|J50F002019;35198|J50F002019;35200|J50F002019;35794|J50F002019;35795|J50F002019;38414|J50F002019;38479|J50F002019;38480|J50F002019;38485|J50F002019;38486|J50F001019;2911|J50F001019;2912|J50F001019;19417|J50F001019;19418|J50F001019;42882|J50F001019;42887|J50F001019;44320|J50F001019;44321|J50F001019;44322|J50F001019;44324|J50F001019;3168|J50F001019;44319|J50F001019;2081|J50F001019;44314|J50F001019;44316|J50F001019;44317|J50F001019;44318|J50F001019;48241|J50F001019;48242|J50F001019;48243|J50F001019;48246|J50F001019;48247|J50F002019;5476|J50F002019;6468|J50F002019;27680|J50F002019;38405|J50F002019;38406|J50F002019;38407|J50F002019;38411|J50F002019;38417|J50F002019;38418|J50F002019;38487|J50F002019;38488|J50F002019;27681|J50F001019;1471|J50F001019;2968|J50F001019;6322|J50F001019;6996|J50F001019;7001|J50F001019;7002|J50F001019;14962|J50F001019;19439|J50F001019;19445|J50F001019;30060|J50F001019;35854|J50F001019;44323|J50F001019;44325|J50F001019;44326|J50F001019;44330|J50F001019;44331|J50F001019;44332|J50F001019;44333|J50F001019;44334|J50F001019;44335|J50F001019;44336|J50F001019;46747|J50F001019;48196|J50F001019;48197|J50F001019;48202|J50F001019;48203|J50F001019;48204|J50F001019;48207|J50F001019;48254|J50F001019;48255|J50F002019;2663|J50F002019;35810|J50F002019;35813|J50F002019;35817|J50F002019;36680|J50F002019;38397|J50F002019;38399|J50F002019;38400|J50F002019;38402|J50F002019;38403|J50F002019;38404|J50F002019;38493|J50F002019;38494|J50F002019;38495|J50F002019;38496|J50F002019;38503|J50F002019;38504|J50F001019;3457|J50F001019;20618|J50F001019;35487|J50F001019;35488|J50F001019;35495|J50F001019;35598|J50F001019;43592|J50F001019;43593|J50F001019;44337|J50F001019;44338|J50F001019;44339|J50F001019;46748|J50F001019;48208|J50F001019;48209|J50F001019;48210|J50F001019;48213|J50F001019;48214|J50F002019;13858|J50F002019;35816|J50F002019;38392|J50F002019;38393|J50F002019;38396|J50F002019;38398|J50F002019;38499|J50F002019;38500|J50F002020;2416|J50F002020;32219|J50F002020;37980|J50F002020;38540|J50F002020;45666|J50F002020;45668|J50F002020;45671|J50F002020;45672|J50F002020;47760|J50F002020;51711|J50F002020;51712|J50F002020;51778|J50F002020;51779|J50F002020;51784|J50F002020;51785|J50F001019;1753|J50F001019;3458|J50F001019;4457|J50F001019;20702|J50F001019;20703|J50F001019;35493|J50F001019;35494|J50F001019;43590|J50F001019;43591|J50F001019;48225|J50F001019;48226|J50F001019;48227|J50F001019;48228|J50F001019;48229|J50F001019;51485|J50F001019;51486|J50F001020;2150|J50F001020;3770|J50F001020;3772|J50F001020;19444|J50F001020;19448|J50F001020;19452|J50F001020;28538|J50F001020;48486|J50F001020;54970|J50F001020;54971|J50F002020;3783|J50F002020;3788|J50F002020;4491|J50F002020;7633|J50F002020;45675|J50F002020;47761|J50F002020;51718|J50F002020;51719|J50F002020;51720|J50F002020;51721|J50F002020;51722|J50F002020;51723|J50F002020;51786|J50F002020;51789|J50F002020;51790|J50F002020;51793|J50F002020;51798|J50F002020;51799|J50F002020;51833|J50F002020;51834|J50F001020;2095|J50F001020;2126|J50F001020;2330|J50F001020;2332|J50F001020;43567|J50F001020;1981|J50F001020;48493|J50F001020;1893|J50F001020;3774|J50F001020;3775|J50F001020;3977|J50F001020;5115|J50F001020;5317|J50F001020;35107|J50F001020;41109|J50F001020;48484|J50F001020;48485|J50F001020;48487|J50F001020;48488|J50F001020;48489|J50F001020;48490|J50F001020;48491|J50F001020;48492|J50F001020;48494|J50F001020;48495|J50F001020;48496|J50F001020;48497|J50F001020;48498|J50F001020;48499|J50F001020;54966|J50F001020;54967|J50F001020;54974|J50F001020;54975|J50F001020;54978|J50F001020;54979|J50F001020;57725|J50F001020;57726|J50F002020;4495|J50F002020;7121|J50F002020;8808|J50F002020;8834|J50F002020;33424|J50F002020;11676|J50F002020;13111|J50F002020;34832|J50F002020;44456|J50F002020;51726|J50F002020;51727|J50F001020;2278|J50F001020;2280|J50F001020;4025|J50F001020;4027|J50F001020;5228|J50F001020;5230|J50F001020;48717|J50F001020;49803|J50F001020;49804|J50F001020;49805|J50F001020;49806|J50F001020;49807|J50F001020;49808|J50F001020;49809|J50F001020;49810|J50F001020;49811|J50F001020;49812|J50F001020;49813|J50F001020;49814|J50F001020;49815|J50F001020;49816|J50F001020;49817|J50F001020;49878|J50F001020;49879|J50F001020;49880|J50F001020;49881|J50F001020;54980|J50F001020;54981|J50F001020;54982|J50F001020;54985|J50F001020;54986|J50F001020;54987|J50F001020;54991|J50F001020;57695|J50F001020;57697|J50F001020;57699|J50F002020;2977|J50F002020;2996|J50F002020;3804|J50F002020;16788|J50F002020;16823|J50F002020;34829|J50F002020;44453|J50F002020;51732|J50F002020;51733|J50F002020;51735|J50F002020;51736|J50F002020;51738|J50F002020;51739|J50F002020;51740|J50F002020;51741|J50F002020;51800|J50F002020;51801|J50F002020;51804|J50F002020;51805|J50F001020;3887|J50F001020;48710|J50F001020;48712|J50F001020;1901|J50F001020;3782|J50F001020;3784|J50F001020;41018|J50F001020;3778|J50F001020;3779|J50F001020;21552|J50F001020;48709|J50F001020;48711|J50F001020;48713|J50F001020;48714|J50F001020;48929|J50F001020;49797|J50F001020;49798|J50F001020;49799|J50F001020;49800|J50F001020;49801|J50F001020;49802|J50F001020;54993|J50F001020;54994|J50F001020;54999|J50F001020;55000|J50F001020;57696|J50F001020;57698|J50F001020;57700|J50F001020;57703|J50F001020;57704|J50F001020;57705|J50F001020;57706|J50F001020;57707|J50F001020;57708|J50F001020;57723|J50F001020;57724|J50F002020;2762|J50F002020;18179|J50F002020;47797|J50F002020;47796|J50F002020;2769|J50F002020;2770|J50F002020;2773|J50F002020;5982|J50F002020;5986|J50F002020;8780|J50F002020;16789|J50F002020;31768|J50F002020;51744|J50F002020;51745|J50F002020;51746|J50F002020;51751|J50F002020;51808|J50F002020;51809|J50F001020;5514|J50F001020;6718|J50F001020;7962|J50F001020;8082|J50F001020;22186|J50F001020;27771|J50F001020;27783|J50F001020;27784|J50F001020;32291|J50F001020;34448|J50F001020;34449|J50F001020;52788|J50F001020;52789|J50F001020;55001|J50F001020;55002|J50F001020;55010|J50F001020;55011|J50F002020;3388|J50F002020;4487|J50F002020;7021|J50F002020;7156|J50F002020;44532|J50F002020;44533|J50F002020;47872|J50F002020;47873|J50F002020;47874|J50F002020;51752|J50F002020;51756|J50F002020;51757|J50F002020;51760|J50F002020;51761|J50F002020;51812|J50F002020;51813|J50F002020;51831|J50F002020;51832|J50F002020;53683|J50F002020;53684|J50F001020;3791|J50F001020;3792|J50F001020;4852|J50F001020;6269|J50F001020;22187|J50F001020;49067|J50F001020;49069|J50F001020;49070|J50F001020;49071|J50F001020;49072|J50F001020;49073|J50F001020;55015|J50F001020;55021|J50F001020;55158|J50F001020;55159|J50F001020;57609|J50F001020;57610|J50F002020;3975|J50F002020;4088|J50F002020;6997|J50F002020;7161|J50F002020;18176|J50F002020;51765|J50F002020;51766|J50F002020;51767|J50F002020;51768|J50F002020;51816|J50F002020;51817|J50F002020;51820|J50F002020;51821|J50F002020;51837|J50F002020;51838|J50F001020;49113|J50F001020;49108|J50F001020;4288|J50F001020;49109|J50F001020;49110|J50F001020;49116|J50F001020;49117|J50F001020;49118|J50F001020;49119|J50F001020;49120|J50F001020;49121|J50F001020;49122|J50F001020;49124|J50F001020;31425|J50F001020;49129|J50F001020;54810|J50F001020;54812|J50F001020;7662|J50F001020;49125|J50F001020;49126|J50F001020;49128|J50F001020;55012|J50F001020;55025|J50F001020;55028|J50F001020;55029|J50F002020;2748|J50F002020;6999|J50F002020;7001|J50F002020;51772|J50F002020;51773|J50F002020;51824|J50F002020;51825|J50F002020;2745|J50F002020;46476|J50F002020;46477|J50F002020;46779|J50F001020;4293|J50F001020;6732|J50F001020;6737|J50F001020;6739|J50F001020;6741|J50F001020;6742|J50F001020;28488|J50F001020;49212|J50F001020;49216|J50F001020;49217|J50F001020;49225|J50F001020;49414|J50F001020;49415|J50F001020;35530|J50F002020;2741|J50F002020;46778|J50F002020;22291|J50F002020;3957|J50F002020;4460|J50F002020;4461|J50F002020;36504|J50F002020;36506|J50F002020;46937|J50F002020;51320|J50F002020;51321|J50F002020;51322|J50F002020;51323|J50F002020;2800|J50F002020;3715|J50F002020;5088|J50F002020;6975|J50F002020;51123|J50F002020;51309|J50F002020;51311|J50F002020;51312|J50F002020;51313|J50F002020;51314|J50F002020;51315|J50F002020;51316|J50F002020;51317|J50F002020;51318|J50F002020;51319|J50F002020;51412|J50F002020;46288|J50F002020;3377|J50F002020;46289|J50F002020;51306|J50F002020;51308|J50F002020;51310|J50F001020;35369|J50F001020;322";
			roads = roads.replaceAll(";",":");
			if(context.getConfiguration().get("key.dist")!=null) {
				size = context.getConfiguration().get("key.dist").split(",").length;
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		JobConf job = new JobConf();
		
		if(args.length>3){
			job.set("key.dist", args[3]);
			System.err.println(args[3]);
		}
		job.setMapperClass(RoadFlowDistMapperClass.class);
		job.setReducerClass(RoadFlowDistReducerClass.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(1);

		TableInputFormat.addInput(new TableInfo(args[0],"ds=" + args[2]), job);  //location
		TableOutputFormat.addOutput(new TableInfo(args[1],"ds=" + args[2]),job); //roadflow
		JobClient.runJob(job);
	}


}