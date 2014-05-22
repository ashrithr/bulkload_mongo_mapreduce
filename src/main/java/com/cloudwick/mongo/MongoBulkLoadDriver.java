package com.cloudwick.mongo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.util.Date;

/**
 * MapReduce Driver to bulk load meter register and interval data set into mongo in the following format:
 *
 * {
 *   _id: ObjectId(),
 *   mid: 'meter id',
 *   rd:  'reading recorded date',
 *   ir_kwh: 'Array of interval meter readings for a specified day in kwh',
 *   ir_kwd: 'Array of interval meter readings for a specified day in kwd',
 *   ir_kvar: 'Array of interval meter readings for a specified day in kvar',
 *   ir_kvrms: 'Array of interval meter readings for a specified day in kvrms',
 *   ir_v: 'Array of interval meter readings for a specified day in voltage',
 *   rr_kwh: 'Array of register meter readings for a specified day in kwh',
 *   rr_kwd: 'Array of register meter readings for a specified day in kwd',
 *   rr_kvar: 'Array of register meter readings for a specified day in kvar',
 *   rr_kvrms: 'Array of register meter readings for a specified day in kvrms',
 *   rr_v: 'Array of register meter readings for a specified day in voltage',
 *
 * }
 *
 * @author ashrith
 */
public class MongoBulkLoadDriver  extends Configured implements Tool {
  // private static final String MONGO_SERVER = "localhost:27017";
  private static final String MONGO_DB = "bulk";
  private static final String MONGO_COLLECTION = "ami";
  private static final String AMI_DVC_FIELD_NAME = "mid";
  private static final String DAY_FIELD_NAME = "rd";
  private static final String INTERVAL_READ_FIELD_PREFIX = "ir";
  private static final String REGISTER_READ_FIELD_PREFIX = "rr";
  private static final String MONGO_USER = "bulkDBAdmin";
  private static final String MONGO_PASSWORD = "password";

  enum BULKLOAD {
    NUM_MONGO_UPDATE_OPS,
    NUM_ERRORS,
    NUM_RECORDS,
    MALFORMED_RECORDS_INTERVAL,
    MALFORMED_RECORDS_REGISTER
  }

  public int run(String[] args) throws Exception {
    if (args.length != 3) {
      System.err.println("Usage: MongoBulkLoadDriver <in-dir> <dataset_format> <mongo_servers>");
      System.err.println("  where: <in-dir> is the input path to process the data files");
      System.err.println("         <dataset_format> is the data set type valid values: REGISTER | INTERVAL");
      System.err.println("         <mongo_servers> is a list of mongodb servers to connect to");
      System.err.println("            Format: 'localhost:27017' or 'localhost:27017,localhost:27018'");
      ToolRunner.printGenericCommandUsage(System.out);
      return 2;
    }

    if (args[1].equalsIgnoreCase("REGISTER")) {
      System.out.println("Data Format: " + args[1]);
    } else if (args[1].equalsIgnoreCase("INTERVAL")) {
      System.out.println("Data Format: " + args[1]);
    } else {
      System.err.println("invalid dataset format, only takes either REGISTER ot INTERVAL");
      System.err.println(args[1]);
      return 2;
    }

    Path inputDir = new Path(args[0]);
    Configuration conf = getConf();

    conf.set("bulkload.mongo.servers", args[2]);
    conf.set("bulkload.mongo.user", MONGO_USER);
    conf.set("bulkload.mongo.password", MONGO_PASSWORD);
    conf.set("bulkload.mongo.db", MONGO_DB);
    conf.set("bulkload.mongo.collection", MONGO_COLLECTION);
    conf.set("bulkload.mongo.dataset.type", args[1]);
    conf.set("bulkload.mongo.field.ami_dvc", AMI_DVC_FIELD_NAME);
    conf.set("bulkload.mongo.field.day", DAY_FIELD_NAME);
    conf.set("bulkload.mongo.field.interval", INTERVAL_READ_FIELD_PREFIX);
    conf.set("bulkload.mongo.field.register", REGISTER_READ_FIELD_PREFIX);

    Job job = Job.getInstance(conf);
    job.setJobName("bulk load mongo " + MONGO_COLLECTION);
    job.setJarByClass(MongoBulkLoadDriver.class);
    FileInputFormat.addInputPath(job, inputDir);
    job.setMapperClass(MongoBulkLoadMapper.class);
    job.setOutputFormatClass(NullOutputFormat.class);

    // reducer NONE
    job.setNumReduceTasks(0);

    Date startTime = new Date();
    System.out.println("Job Started: " + startTime);
    int ret = job.waitForCompletion(true) ? 0 : 1;
    Date endTime = new Date();
    System.out.println("Job ended: " + endTime);
    System.out.println("The job took " + (endTime.getTime() - startTime.getTime()) /1000 + " seconds.");

    return ret;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new MongoBulkLoadDriver(), args);
    System.exit(res);
  }
}
