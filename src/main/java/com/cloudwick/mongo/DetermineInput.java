package com.cloudwick.mongo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;

public class DetermineInput extends Configured implements Tool {

  public static class InputMapper extends Mapper<Object, Text, Text, IntWritable> {
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      if (line != null && !line.isEmpty()) {
        String[] parts = line.split("\\t");
        int i = 0;
        for (String part: parts) {
          System.out.println(i + " -> " + part);
          i++;
        }
      }
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    Path inputDir = new Path(args[0]);
    Configuration conf = getConf();

    Job job = Job.getInstance(conf);
    job.setJobName("bulk loader: determine input");
    job.setJarByClass(DetermineInput.class);
    FileInputFormat.addInputPath(job, inputDir);
    job.setMapperClass(InputMapper.class);
    job.setOutputFormatClass(NullOutputFormat.class);

    // reducer NONE
    job.setNumReduceTasks(0);
    int ret = job.waitForCompletion(true) ? 0 : 1;

    return ret;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new MongoBulkLoadDriver(), args);
    System.exit(res);
  }
}
