package com.cloudwick.mongo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DetermineInput extends Configured implements Tool {

  public static class InputMapper extends Mapper<LongWritable, Text, Text, Text> {
    // for each input line of input file
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] tokens = value.toString().split("\\t");
      String mid;
      String day;
      String uom;
      String mrdg;
      String readingType;
      String registerFieldPrefix = "rr";

      if(tokens.length > 1) {
        mid = tokens[0];
        String readDate = tokens[1];
        String date[] = readDate.split("\\s+");
        day = date[0];
        uom = tokens[6];
        mrdg = tokens[10];

        context.write(new Text(String.format("%s#%s", mid, day)), new Text(String.format("%s#%s", uom, mrdg)));
      }
    }
  }

  public static class OutputReducer extends Reducer<Text, Text, NullWritable, NullWritable> {
    private List<String> kwhValues = new ArrayList<String>();
    private List<String> kwdValues = new ArrayList<String>();
    private List<String> kvarValues = new ArrayList<String>();
    private List<String> kvrmsValues = new ArrayList<String>();
    private List<String> vValues = new ArrayList<String>();

    @Override
    public void reduce(Text meterDayKey, Iterable<Text> meterValues, Context context) {
      for (Text meterValue: meterValues) {
        String readingType = meterValue.toString().split("#")[0];
        String readingVal  = meterValue.toString().split("#")[1];
        if (readingType.matches("(?i:.*kwh.*)")) {
          kwhValues.add(readingVal);
        } else if (readingType.matches("(?i:.*kwd.*)")) {
          kwdValues.add(readingVal);
        } else if (readingType.matches("(?i:.*kvar.*)")) {
          kvarValues.add(readingVal);
        } else if (readingType.matches("(?i:.*kvrms.*)")) {
          kvrmsValues.add(readingVal);
        } else if (readingType.matches("(?i:.*v.*)")) {
          vValues.add(readingVal);
        }
      }
      System.out.println(meterDayKey.toString() + " -> " + Arrays.asList(kwhValues));
      kwhValues.clear();
      kwdValues.clear();
      kvarValues.clear();
      kvrmsValues.clear();
      vValues.clear();

//      String meterID = meterDayKey.toString().split("#")[0];
//      String readingDate = meterDayKey.toString().split("#")[1];
//      System.out.println(String.format("mid: %s; day: %s; rr_kwh: %s, rr_kwd: %s, rr_kvar: %s, rr_kvrms: %s, rr_v: %s",
//          meterID, readingDate, Arrays.asList(kwhValues), Arrays.asList(kwdValues), Arrays.asList(kvarValues),
//          Arrays.asList(kvrmsValues), Arrays.asList(vValues)));
//      kwdValues.clear();
//      kwdValues.clear();
//      kvarValues.clear();
//      kvrmsValues.clear();
//      vValues.clear();
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
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setReducerClass(OutputReducer.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(NullWritable.class);
    job.setOutputFormatClass(NullOutputFormat.class);
    job.setNumReduceTasks(1);
    int ret = job.waitForCompletion(true) ? 0 : 1;

    return ret;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new DetermineInput(), args);
    System.exit(res);
  }
}
