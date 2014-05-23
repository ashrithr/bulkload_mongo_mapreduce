package com.cloudwick.mongo;

import com.mongodb.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Description goes here
 *
 * @author ashrith
 */
public class MongoBulkLoadReducer  extends Reducer<Text, Text, NullWritable, NullWritable> {
  private Logger logger = Logger.getLogger(MongoBulkLoadMapper.class);
  private MongoClient mongoClient = null;
  private DBCollection collection;
  private String dataSetFormat;
  private String amiDvcFieldName;
  private String dayFieldName;
  private String intervalFieldPrefix;
  private String registerFieldPrefix;
  private long batchSize = 512;
  private long dbCounter = 0;
  private BulkWriteOperation builder;
  private BulkWriteResult result;
  private WriteConcern writeConcern;

  // interval values
  private List<String> irKwhValues = new ArrayList<String>();
  private List<String> irKwdValues = new ArrayList<String>();
  private List<String> irKvarValues = new ArrayList<String>();
  private List<String> irKvrmsValues = new ArrayList<String>();
  private List<String> irVValues = new ArrayList<String>();
  // register values
  private List<String> rrKwhValues = new ArrayList<String>();
  private List<String> rrKwdValues = new ArrayList<String>();
  private List<String> rrKvarValues = new ArrayList<String>();
  private List<String> rrKvrmsValues = new ArrayList<String>();
  private List<String> rrVValues = new ArrayList<String>();


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
    writeConcern = WriteConcern.valueOf(c.get("bulkload.mongo.write.concern"));

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
    builder = collection.initializeUnorderedBulkOperation();
  }

  @Override
  protected void reduce(Text meterDayKey, Iterable<Text> meterValues, Context context) {
    BasicDBObject document;

    for(Text meterValue: meterValues) {
      String readingType = meterValue.toString().split("#")[0];
      String readingVal  = meterValue.toString().split("#")[1];
      if (dataSetFormat.equalsIgnoreCase("REGISTER")) {
        if (readingType.matches("(?i:.*kwh.*)")) {
          rrKwhValues.add(readingVal);
        } else if (readingType.matches("(?i:.*kwd.*)")) {
          rrKwdValues.add(readingVal);
        } else if (readingType.matches("(?i:.*kvar.*)")) {
          rrKvarValues.add(readingVal);
        } else if (readingType.matches("(?i:.*kvrms.*)")) {
          rrKvrmsValues.add(readingVal);
        } else if (readingType.matches("(?i:.*v.*)")) {
          rrVValues.add(readingVal);
        }
      } else if (dataSetFormat.equalsIgnoreCase("INTERVAL")) {
        if (readingType.matches("(?i:.*kwh.*)")) {
          irKwhValues.add(readingVal);
        } else if (readingType.matches("(?i:.*kwd.*)")) {
          irKwdValues.add(readingVal);
        } else if (readingType.matches("(?i:.*kvar.*)")) {
          irKvarValues.add(readingVal);
        } else if (readingType.matches("(?i:.*kvrms.*)")) {
          irKvrmsValues.add(readingVal);
        } else if (readingType.matches("(?i:.*v.*)")) {
          irVValues.add(readingVal);
        }
      }
    }

    // Build out the mongo document
    try {
      String meterId     = meterDayKey.toString().split("#")[0];
      String recordedDay = meterDayKey.toString().split("#")[1];

      document = new BasicDBObject(amiDvcFieldName, meterId)
                    .append(dayFieldName, recordedDay);
      if (dataSetFormat.equalsIgnoreCase("REGISTER")) {
        if (!rrKwhValues.isEmpty()) {
          document.append(registerFieldPrefix + "_kwh", rrKwhValues);
        }
        if (!rrKwdValues.isEmpty()) {
          document.append(registerFieldPrefix + "_kwd", rrKwdValues);
        }
        if (!rrKvarValues.isEmpty()) {
          document.append(registerFieldPrefix + "_kvar", rrKvarValues);
        }
        if (!rrKvrmsValues.isEmpty()) {
          document.append(registerFieldPrefix + "_kvrms", rrKvrmsValues);
        }
        if (!rrVValues.isEmpty()) {
          document.append(registerFieldPrefix + "_v", rrVValues);
        }
      } else if (dataSetFormat.equalsIgnoreCase("INTERVAL")) {
        if (!irKwhValues.isEmpty()) {
          document.append(intervalFieldPrefix + "_kwh", irKwhValues);
        }
        if (!irKwdValues.isEmpty()) {
          document.append(intervalFieldPrefix + "_kwd", irKwdValues);
        }
        if (!irKvarValues.isEmpty()) {
          document.append(intervalFieldPrefix + "_kvar", irKvarValues);
        }
        if (!irKvrmsValues.isEmpty()) {
          document.append(intervalFieldPrefix + "_kvrms", irKvrmsValues);
        }
        if (!irVValues.isEmpty()) {
          document.append(intervalFieldPrefix + "_v", irVValues);
        }
      }

      builder.insert(document);
      dbCounter++;

      if (dbCounter % batchSize == 0) {
        result = builder.execute(writeConcern);
        context.getCounter(MongoBulkLoadDriver.BULKLOAD.NUM_MONGO_INSERT_OPS).increment(result.getInsertedCount());
        builder = collection.initializeUnorderedBulkOperation();
      }
    } catch (Exception ex) {
      logger.debug(ex);
      context.getCounter(MongoBulkLoadDriver.BULKLOAD.NUM_ERRORS).increment(1);
    }

    // clear all the DBList's
    if (dataSetFormat.equalsIgnoreCase("REGISTER")) {
      rrKwhValues.clear();
      rrKwdValues.clear();
      rrKvarValues.clear();
      rrKvrmsValues.clear();
      rrVValues.clear();
    } else if (dataSetFormat.equalsIgnoreCase("INTERVAL")) {
      irKwhValues.clear();
      irKwdValues.clear();
      irKvarValues.clear();
      irKvrmsValues.clear();
      irVValues.clear();
    }
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    if (builder != null) {
      try {
        result = builder.execute(writeConcern);
        context.getCounter(MongoBulkLoadDriver.BULKLOAD.NUM_MONGO_INSERT_OPS).increment(result.getInsertedCount());
      } catch (Exception ex) {
        logger.debug(ex);
        context.getCounter(MongoBulkLoadDriver.BULKLOAD.NUM_ERRORS).increment(1);
      }
    }
    /*
      Make sure we close the mongo client connection
     */
    if (mongoClient != null)
      mongoClient.close();
  }
}
