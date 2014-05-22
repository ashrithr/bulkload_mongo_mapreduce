package com.cloudwick.mongo;

import com.mongodb.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Mapper to process register or interval meter records and place them in mongo
 *
 * @author ashrith
 */
public class MongoBulkLoadMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
  private Logger logger = Logger.getLogger(MongoBulkLoadMapper.class);
  private MongoClient mongoClient = null;
  private DBCollection collection;
  private String dataSetFormat;
  private String amiDvcFieldName;
  private String dayFieldName;
  private String intervalFieldPrefix;
  private String registerFieldPrefix;

  private static WriteResult update(DBCollection collection, BasicDBObject criteria, BasicDBObject insertDoc) {
    // db.collection.update(criteria, objNew, upsert, multi)
    return collection.update(criteria, insertDoc, true, false);
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    Configuration c = context.getConfiguration();

    String[] servers = c.getStrings("bulkload.mongo.servers");
    String databaseName = c.get("bulkload.mongo.db");
    String collectionName = c.get("bulkload.mongo.collection");
    String username = c.get("bulkload.mongo.user");
    String password = c.get("bulkload.mongo.password");
    dataSetFormat = c.get("bulkload.mongo.dataset.type");
    amiDvcFieldName = c.get("bulkload.mongo.field.ami_dvc");
    dayFieldName = c.get("bulkload.mongo.field.day");
    intervalFieldPrefix = c.get("bulkload.mongo.field.interval");
    registerFieldPrefix = c.get("bulkload.mongo.field.register");

    List<MongoCredential> creds = new ArrayList<MongoCredential>();
    creds.add(MongoCredential.createMongoCRCredential(username, databaseName, password.toCharArray()));
    List<ServerAddress> serverAddresses = new ArrayList<ServerAddress>();
    for (String server : servers) {
      serverAddresses.add(new ServerAddress(server));
    }
    mongoClient = new MongoClient(serverAddresses, creds);
    DB db = mongoClient.getDB(databaseName);
    db.setWriteConcern(WriteConcern.UNACKNOWLEDGED);
    collection = db.getCollection(collectionName);
    // check if this improves the performance or not
    // collection.createIndex(new BasicDBObject(amiDvcFieldName, 1).append(dayFieldName, 1));
  }

  @Override
  protected void map(LongWritable key, Text value, Context context) {
    String line = value.toString();
    if (line != null && !line.isEmpty()) {
      String[] parts = line.split("\\t");

      String mid;
      String day;
      String uom;
      String mrdg;
      String reading_type;

      if (dataSetFormat.equalsIgnoreCase("REGISTER")) {
        if (parts.length > 13) {
          context.getCounter(MongoBulkLoadDriver.BULKLOAD.MALFORMED_RECORDS_REGISTER);
          System.err.println("Malformed REGISTER record: " + line);
        } else {
          mid = parts[0];
          String readDate = parts[1];
          String date[] = readDate.split("\\s+");
          day = date[0];
          uom = parts[6];
          mrdg = parts[10];

          if (uom.matches("(?i:.*kwh.*)")) {
            reading_type = registerFieldPrefix + "_kwh";
          } else if (uom.matches("(?i:.*kwd.*)")) {
            reading_type = registerFieldPrefix + "_kwd";
          } else if (uom.matches("(?i:.*kvar.*)")) {
            reading_type = registerFieldPrefix + "_kvar";
          } else if (uom.matches("(?i:.*kvrms.*)") || uom.matches("(?i:.*vrms.*)")) {
            reading_type = registerFieldPrefix + "_kvrms";
          } else if (uom.matches("(?i:.*v.*)")) {
            reading_type = registerFieldPrefix + "_v";
          } else {
            reading_type = registerFieldPrefix + "_u";
          }

          // System.out.println(String.format("ami_dvc_name:%s;day:%s;%s:%s", mid, day, reading_type, mrdg));

          try {
            update(collection,
                new BasicDBObject(amiDvcFieldName, mid).append(dayFieldName, day),
                new BasicDBObject("$push", new BasicDBObject(reading_type, mrdg)));
            context.getCounter(MongoBulkLoadDriver.BULKLOAD.NUM_MONGO_UPDATE_OPS).increment(1);
          } catch (Exception e) {
            logger.debug(e);
            context.getCounter(MongoBulkLoadDriver.BULKLOAD.NUM_ERRORS).increment(1);
          }
          context.getCounter(MongoBulkLoadDriver.BULKLOAD.NUM_RECORDS).increment(1);
        }
      } else if (dataSetFormat.equalsIgnoreCase("INTERVAL")) {
        if (parts.length > 14) {
          context.getCounter(MongoBulkLoadDriver.BULKLOAD.MALFORMED_RECORDS_INTERVAL);
          System.err.println("Malformed INTERVAL found: " + line);
        } else {
          mid = parts[0];
          String readDate = parts[1];
          String date[] = readDate.split("\\s+");
          day = date[0];
          uom = parts[6];
          mrdg = parts[9];

          /*
            dataset from 2013-12-31 contains 'Voltage Sag' & 'Voltage swells' instead of voltage

          if (uom.matches("(?i:.*voltage*)")) {
            mrdg = parts[12];
          } else {
            mrdg = parts[11];
          }
          */

          if (uom.matches("(?i:.*kwh.*)")) {
            reading_type = intervalFieldPrefix + "_kwh";
          } else if (uom.matches("(?i:.*kwd.*)")) {
            reading_type = intervalFieldPrefix + "_kwd";
          } else if (uom.matches("(?i:.*kvar.*)")) {
            reading_type = intervalFieldPrefix + "_kvar";
          } else if (uom.matches("(?i:.*kvrms.*)") || uom.matches("(?i:.*vrms.*)")) {
            reading_type = intervalFieldPrefix + "_kvrms";
          } else if (uom.matches("(?i:.*v.*)")) {
            reading_type = intervalFieldPrefix + "_v";
          } else {
            reading_type = intervalFieldPrefix + "_u";
          }
          try {
            update(collection,
                new BasicDBObject(amiDvcFieldName, mid).append(dayFieldName, day),
                new BasicDBObject("$push", new BasicDBObject(reading_type, mrdg)));
            context.getCounter(MongoBulkLoadDriver.BULKLOAD.NUM_MONGO_UPDATE_OPS).increment(1);
          } catch (Exception e) {
            logger.debug(e);
            context.getCounter(MongoBulkLoadDriver.BULKLOAD.NUM_ERRORS).increment(1);
          }
          context.getCounter(MongoBulkLoadDriver.BULKLOAD.NUM_RECORDS).increment(1);
        }
      }
    }
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    /*
      Make sure we close the mongo client connection
     */
    if (mongoClient != null)
      mongoClient.close();
  }
}
