package com.cloudwick.mongo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;

/**
 * Mapper to process register or interval meter records and place them in mongo
 *
 * @author ashrith
 */
public class MongoBulkLoadMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Logger logger = Logger.getLogger(MongoBulkLoadMapper.class);
    private String dataSetFormat;

    @Override
    protected void setup(Context context) {
        Configuration c = context.getConfiguration();
        dataSetFormat = c.get("bulkload.mongo.dataset.type");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] tokens = line.split("\\t");
        String mid;
        String day;
        String hour;
        String uom;
        String mrdg;

        if (tokens.length > 1) {
            if (dataSetFormat.equalsIgnoreCase("REGISTER")) {
                if (tokens.length != 13) {
                    context.getCounter(MongoBulkLoadDriver.BULKLOAD.MALFORMED_RECORDS_REGISTER).increment(1);
                    System.err.println("Malformed REGISTER record: " + line);
                } else {
                    mid = tokens[0];
                    String readDate = tokens[1];
                    String date[] = readDate.split("\\s+");
                    day = date[0];
                    hour = date[1].substring(0, 5);
                    uom = tokens[6];
                    mrdg = tokens[10];
                    context.write(new Text(mid + "#" + day), new Text(hour + "#" + uom + "#" + mrdg));
                    context.getCounter(MongoBulkLoadDriver.BULKLOAD.NUM_RECORDS).increment(1);
                }
            } else if (dataSetFormat.equalsIgnoreCase("INTERVAL")) {
                if (tokens.length != 14) {
                    context.getCounter(MongoBulkLoadDriver.BULKLOAD.MALFORMED_RECORDS_INTERVAL).increment(1);
                    System.err.println("Malformed INTERVAL record: " + line);
                } else {
                    mid = tokens[0];
                    String readDate = tokens[1];
                    String date[] = readDate.split("\\s+");
                    day = date[0];
                    hour = date[1].substring(0, 5);
                    uom = tokens[6];
                    mrdg = tokens[9];
                    context.write(new Text(mid + "#" + day), new Text(hour + "#" + uom + "#" + mrdg));
                    context.getCounter(MongoBulkLoadDriver.BULKLOAD.NUM_RECORDS).increment(1);
                }
            }
        }
    }
}
