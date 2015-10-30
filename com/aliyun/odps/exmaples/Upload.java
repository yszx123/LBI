package com.aliyun.odps.exmaples;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.aliyun.odps.Record;
import com.aliyun.odps.io.*;
import com.aliyun.odps.mapreduce.*;

/**
 * Example Class to add record to table from resource file.
 */
public class Upload {

  public static final Log LOG = LogFactory.getLog(Upload.class);

  public static class UploadMapper extends Mapper<Text, LongWritable> {

    @Override
    public void run(MapContext<Text, LongWritable> context) throws IOException,
        InterruptedException {
      Record record = context.createOutputRecord();
      String filename = context.getConfiguration().get("import.filename");
      byte[] importdata = context.readCacheFile(filename);
      String lines[] = new String(importdata).split("\n");
      for (int i = 0; i < lines.length; i++) {
        String[] ss = lines[i].split(",");
        record.set(0, new Text(ss[0].trim()));
        record.set(1,new Text(ss[1].trim()));
        record.set(2,new Text(ss[2].trim()));
        record.set(3,new Text(ss[3].trim()));
        record.set(4, new DoubleWritable(Double.parseDouble(ss[4].trim())));
        record.set(5, new DoubleWritable(Double.parseDouble(ss[5].trim())));
        record.set(6,new IntWritable(Integer.parseInt(ss[6].trim())));
        record.set(7,new Text(ss[7].trim()));
        context.write(record);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: Upload <import_txt> <out_table>");
      System.exit(2);
    }
    JobConf job = new JobConf();
    job.setMapperClass(UploadMapper.class);
    job.set("import.filename", args[0]);
    job.setAllowNoInput(true);
    job.setNumMapTasks(1);
    job.setNumReduceTasks(0);
    TableOutputFormat.addOutput(new TableInfo(args[1]), job);
    JobClient.runJob(job);
  }
}
