package com.aliyun.odps.exmaples;

import java.io.IOException;

import com.aliyun.odps.Record;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.TableInfo;
import com.aliyun.odps.io.TableInputFormat;
import com.aliyun.odps.io.TableOutputFormat;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.mapreduce.CombineContext;
import com.aliyun.odps.mapreduce.Combiner;
import com.aliyun.odps.mapreduce.JobClient;
import com.aliyun.odps.mapreduce.JobConf;
import com.aliyun.odps.mapreduce.MapContext;
import com.aliyun.odps.mapreduce.Mapper;
import com.aliyun.odps.mapreduce.ReduceContext;
import com.aliyun.odps.mapreduce.Reducer;

public class WordCount {

  public static class TokenizerMapper extends Mapper<Text, LongWritable> {

    private final static LongWritable one = new LongWritable(1);
    private Text word = new Text();

    @Override
    public void map(LongWritable recordNum, Record record,
        MapContext<Text, LongWritable> context) throws IOException,
        InterruptedException {
      for (int i = 0; i < record.size(); i++) {
        word.set(record.get(i).toString());
        context.write(word, one);
      }
    }
  }

  public static class SumCombiner extends Combiner<Text, LongWritable> {
    private LongWritable result = new LongWritable();

    @Override
    protected void combine(Text key, Iterable<LongWritable> values,
        CombineContext<Text, LongWritable> context) throws IOException,
        InterruptedException {
      int count = 0;
      for (LongWritable value : values) {
        count += value.get();
      }
      result.set(count);
      context.write(key, result);
    }
  }

  public static class SumReducer extends Reducer<Text, LongWritable> {
    private LongWritable sum = new LongWritable();
    private Record result = null;

    @Override
    protected void setup(ReduceContext<Text, LongWritable> context)
        throws IOException, InterruptedException {
      result = context.createOutputRecord();
    }

    @Override
    public void reduce(Text key, Iterable<LongWritable> values,
        ReduceContext<Text, LongWritable> context) throws IOException,
        InterruptedException {
      int count = 0;
      for (LongWritable val : values) {
        count += val.get();
      }
      sum.set(count);
      result.set(0, key);
      result.set(1, sum);
      context.write(result);
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: wordcount <in_table> <out_table>");
      System.exit(2);
    }

    JobConf job = new JobConf();
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(SumCombiner.class);
    job.setReducerClass(SumReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);

    TableInputFormat.addInput(new TableInfo(args[0]), job);
    TableOutputFormat.addOutput(new TableInfo(args[1]), job);

    JobClient.runJob(job);
  }
}
 

