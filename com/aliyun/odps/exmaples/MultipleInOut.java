package com.aliyun.odps.exmaples;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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

/**
 * Multi input & output example.
 */
public class MultipleInOut {

  public static final Log LOG = LogFactory.getLog(MultipleInOut.class);

  public static class TokenizerMapper extends Mapper<Text, LongWritable> {

    private final static LongWritable one = new LongWritable(1);
    private Text word = new Text();

    @Override
    public void map(LongWritable key, Record value,
        MapContext<Text, LongWritable> context) throws IOException,
        InterruptedException {
      for (int i = 0; i < value.size(); i++) {
        word.set((Text) value.get(i));
        context.write(word, one);
      }
    }

    @Override
    protected void cleanup(MapContext<Text, LongWritable> context)
        throws IOException, InterruptedException {
      Record result = context.createOutputRecord();
      result.set(0, new Text("default"));
      result.set(1, new LongWritable(1));
      context.write(result);
      Record result1 = context.createOutputRecord("out1");
      result1.set(0, new Text("out1"));
      result1.set(1, new LongWritable(1));
      context.write(result1, "out1");
      Record result2 = context.createOutputRecord("out2");
      result2.set(0, new Text("out1"));
      result2.set(1, new LongWritable(1));
      context.write(result2, "out2");
    }
  }

  public static class SumReducer extends Reducer<Text, LongWritable> {
    private LongWritable sum = new LongWritable();
    private Record result = null;
    private Record result1 = null;
    private Record result2 = null;

    @Override
    protected void setup(ReduceContext<Text, LongWritable> context)
        throws IOException, InterruptedException {
      result = context.createOutputRecord();
      result1 = context.createOutputRecord("out1");
      result2 = context.createOutputRecord("out2");
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
      long mod = sum.get() % 3;
      if (mod == 0) {
        result.set(0, key);
        result.set(1, sum);
        context.write(result);
      } else if (mod == 1) {
        result1.set(0, key);
        result1.set(1, sum);
        context.write(result1, "out1");
      } else {
        result2.set(0, key);
        result2.set(1, sum);
        context.write(result2, "out2");
      }
    }
  }

  public static void main(String[] args) throws Exception {

    String[] inputs = null;
    String[] outputs = null;
    if (args.length == 0) {
      inputs = new String[] { "multi_in_t1", "multi_in_t2|pt=2" };
      outputs = new String[] { "multi_out_t1", "multi_out_t2|a=1/b=1|out1",
          "multi_out_t2|a=2/b=2|out2" };
    } else if (args.length == 2) {
      inputs = args[0].split(",");
      outputs = args[1].split(",");
    } else {
      System.err.println("MultipleInOut in... out...");
      System.exit(1);
    }

    JobConf job = new JobConf();

    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(SumReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);

    for (String in : inputs) {
      String[] ss = in.split("\\|");
      if (ss.length == 1) {
        TableInputFormat.addInput(new TableInfo(ss[0]), job);
      } else if (ss.length == 2) {
        TableInputFormat.addInput(new TableInfo(ss[0], ss[1]), job);
      } else {
        System.err.println("Style of input: " + in + " is not right");
        System.exit(1);
      }
    }
    for (String out : outputs) {
      String[] ss = out.split("\\|");
      if (ss.length == 1) {
        TableOutputFormat.addOutput(new TableInfo(ss[0]), job);
      } else if (ss.length == 2) {
        TableOutputFormat.addOutput(new TableInfo(ss[0], ss[1]), job);
      } else if (ss.length == 3) {
        if (ss[1].isEmpty()) {
          TableOutputFormat.addOutput(new TableInfo(ss[0]), ss[2], job);
        } else {
          TableOutputFormat.addOutput(new TableInfo(ss[0], ss[1]), ss[2], job);
        }
      } else {
        System.err.println("Style of output: " + out + " is not right");
        System.exit(1);
      }
    }

    JobClient.runJob(job);

  }
}
