package com.lbi.lbi.mr;

import java.io.IOException;
import java.util.HashSet;

import com.aliyun.odps.Record;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.TableInfo;
import com.aliyun.odps.io.TableInputFormat;
import com.aliyun.odps.io.TableOutputFormat;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.io.Tuple;
import com.aliyun.odps.io.WritableComparable;
import com.aliyun.odps.io.WritableComparator;
import com.aliyun.odps.mapreduce.JobClient;
import com.aliyun.odps.mapreduce.JobConf;
import com.aliyun.odps.mapreduce.MapContext;
import com.aliyun.odps.mapreduce.Mapper;
import com.aliyun.odps.mapreduce.Partitioner;
import com.aliyun.odps.mapreduce.ReduceContext;
import com.aliyun.odps.mapreduce.Reducer;
import com.lbi.map.LngLat;
import com.lbi.map.Point;
import com.lbi.map.TMap;
//import com.lbi.util.MortanGid;

//import com.lbi.lbi.ClusterImeiCombiner;

public class ClusterImeiSortDriver {

	public static class ClusterImeiReducerClass extends Reducer<Tuple, Text> {

		private Record result = null;

		@Override
		protected void setup(ReduceContext<Tuple, Text> context) throws IOException,
				InterruptedException {
			result = context.createOutputRecord();
		}
		
		@Override
		public void reduce(Tuple key, Iterable<Text> values,
				ReduceContext<Tuple, Text> context) throws IOException,
				InterruptedException {

			HashSet<String> hsClu = new HashSet<String>();
			HashSet<String> hsLoc = new HashSet<String>();
			String first = "";
			int i = 0;
			for (Text val : values) {
	            if(i == 0)
	            {
	            	first = val.toString();
	            }else{
	            	i++;
	            }
				if (val.toString().indexOf("0@") != -1) {
					String c = val.toString().replace("0@", "");
					if (!hsClu.contains(c)) {
						hsClu.add(c);
					}
				} else if (val.toString().indexOf("1@") != -1) {
					String l = val.toString().replace("1@", "");
					if (!hsLoc.contains(l)) {
						hsLoc.add(l);
					}
				}
			}
	        System.out.println("first key = " + first.toString() + " cls size = " + hsClu.size() + " log size = " + hsLoc.size());
	        result.set(0, new Text(first));
			result.set(1, new LongWritable(hsClu.size()));
			result.set(2, new LongWritable(hsLoc.size()));
			result.set(3, (LongWritable) key.get(0));
			context.write(result);
	        /*if (hsClu.size() > 0 && hsLoc.size() > 0) {
				System.out.println("hsClu:" + hsClu.size() + " hsLoc:"
						+ hsLoc.size());
				if (hsClu.size() > 2) {
					Iterator<String> it = hsClu.iterator();
					while (it.hasNext()) {
						System.out.println("it: " + it.next());
					}
				}

				Object[] clu = hsClu.toArray();
				Object[] loc = hsLoc.toArray();
				for (int i = 0; i < clu.length; i++) {
					for (int j = 0; j < loc.length; j++) {
						result.set(0, new Text(clu[i].toString()));
						String[] imei_time = loc[j].toString().split("_");
						result.set(1, new Text(imei_time[0]));
						result.set(2, new Text(imei_time[1]));
						context.write(result);
					}
				}
			}*/
		}
	}

	
	public static class ClusterImeiMapperClass extends Mapper<Tuple, Text> {

	    private Text grid = new Text();
	    private Tuple t = new Tuple(2);
	    private final Text result = new Text();
		private String log_table_name = "address_location_log";

		static boolean isDouble(String x, String y) {
			try {
				Double.parseDouble(x);
				Double.parseDouble(y);
				return true;
			} catch (NumberFormatException ex) {
			}
			return false;
		}

		@Override
		public void map(LongWritable recordNum, Record record,
				MapContext<Tuple, Text> context) throws IOException {
			String tableName = context.getInputTableInfo().getTableName();
			String gid = "";
			
			if (tableName.indexOf(log_table_name) != -1) {

				if (isDouble(record.get("lon").toString(), record.get("lat")
						.toString())) {
					// -----1、经纬度获取gid------
					// 经纬度点
					LngLat pt = new LngLat(Double.parseDouble(record.get("lon").toString()), 
							Double.parseDouble(record.get("lat").toString()));
					// 经纬度转墨卡托坐标
					Point point = TMap.lonLat2Mercator(pt);
					int x = (int) Math.ceil(point.X / 300);
					int y = (int) Math.ceil(point.Y / 300);
					// 点所在网格gid
					gid = x + "_" + y;
					//long m_gid = MortanGid.getMortanGid(gid);
					String imei_time = "1@" + record.get("imei") + "@"
							+ record.get("accesstime")+ "@" + gid;
					result.set(imei_time);
					t.set(0, new LongWritable(gid.hashCode()));
					t.set(1, new LongWritable(1));				
					grid.set(gid);
					context.write(t, result);
				}
			} else {
				gid = record.get("gid").toString();
				//long m_gid = MortanGid.getMortanGid(gid);
				String cluster = "0@" + record.get("cluster_id").toString()+ "@" + gid;
				result.set(cluster);
				grid.set(gid);
				t.set(0, new LongWritable(gid.hashCode()));
				t.set(1, new LongWritable(0));
				context.write(t, result);
			}
		}

		@Override
		protected void setup(MapContext<Tuple, Text> context) throws IOException,
				InterruptedException {
		}

	}
	
	@SuppressWarnings("unused")
	public static void main(String[] args) throws Exception {
		if (args.length != 5) {
			System.err.println("Usage: wordcount <in_table> <out_table>");
			System.exit(2);
		}

		JobConf job = new JobConf();

		job.setMapperClass(ClusterImeiMapperClass.class);
		// job.setCombinerClass(ClusterImeiCombiner.class);
		job.setReducerClass(ClusterImeiReducerClass.class);

		job.setOutputKeyComparatorClass(TupleComparator.class);

		// group and partition by the first int in the pair
		job.setPartitionerClass(FirstKeyPartitioner.class);
		job.setOutputValueGroupingComparator(ValueComparator.class);

		job.setMapOutputKeyClass(Tuple.class);
		job.setMapOutputValueClass(Text.class);
		// job.setNumReduceTasks(100);
		String inDate = args[3];
		/*
		 * boolean hasData =
		 * TableJudgment.judgeDateCount(inDate,args[0],"lbi_ods");
		 * while(!hasData) { inDate = TimeUtil.getPreDayTimeStamp(inDate, 7);
		 * hasData =
		 * TableJudgment.judgeDateCount(inDate,args[0],"lbi_ods");
		 * System.out.println("业务日期数据无,取前一天试一下!"); }
		 */
		TableInputFormat.addInput(new TableInfo(args[0], "ds=" + args[3]), job); // location
		TableInputFormat.addInput(new TableInfo(args[1], "ds=" + args[3]), job);// gidindex
		TableOutputFormat.addOutput(new TableInfo(args[2], "ds=" + args[4]),
				job); // cluster
		JobClient.runJob(job);
	}

	public static class FirstKeyPartitioner extends Partitioner<Tuple, Text> {
		@Override
		public int getPartition(Tuple key, Text value, int numPartitions) {
			int k = (int)((LongWritable) key.get(0)).get();
			return Math.abs(k & Integer.MAX_VALUE) % numPartitions;
		}
	}

	public static class TupleComparator extends WritableComparator {

		public TupleComparator() {
			super(Tuple.class, true);
		}

		static {
			WritableComparator.define(Tuple.class, new TupleComparator());
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable o1, WritableComparable o2) {
			Tuple t1 = (Tuple) o1;
			Tuple t2 = (Tuple) o2;
			long l0 = ((LongWritable) t1.get(0)).get();
			long r0 = ((LongWritable) t2.get(0)).get();
			long l1 = ((LongWritable) t1.get(1)).get();
			long r1 = ((LongWritable) t2.get(1)).get();

			return l0 == r0 ? (l1 == r1 ? 0 : (l1 < r1 ? -1 : 1))
					: (l0 < r0 ? -1 : 1);
		}
	}
	
	public static class ValueComparator extends WritableComparator {
		protected ValueComparator() {
			super(Tuple.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable o1, WritableComparable o2) {
			Tuple t1 = (Tuple) o1;
		    Tuple t2 = (Tuple) o2;
			long l0 = ((LongWritable) t1.get(1)).get();
			long r0 = ((LongWritable) t2.get(1)).get();
		
			return l0 == r0 ? 0 : (l0 < r0 ? -1 : 1);
		}	
	}
}
