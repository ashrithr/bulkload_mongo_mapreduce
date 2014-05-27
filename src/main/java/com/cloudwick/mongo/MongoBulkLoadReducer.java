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
  //private List<DBObject> documents = new ArrayList<DBObject>();

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
    String[] keySplits  = meterDayKey.toString().split("#");
    String meterId      = keySplits[0];
    String recordedDay  = keySplits[1];
    String recordedHour = keySplits[2];

    for(Text meterValue: meterValues) {
      String[] splits = meterValue.toString().split("#");
      String readingType = splits[0];
      String readingVal  = splits[1];
      if (dataSetFormat.equalsIgnoreCase("REGISTER")) {
        if (readingType.matches("(?i:.*kwh.*)")) {
          rrKwhValues.add(recordedHour + "#" + readingVal);
        } else if (readingType.matches("(?i:.*kwd.*)")) {
          rrKwdValues.add(recordedHour + "#" + readingVal);
        } else if (readingType.matches("(?i:.*kvar.*)")) {
          rrKvarValues.add(recordedHour + "#" + readingVal);
        } else if (readingType.matches("(?i:.*kvrms.*)")) {
          rrKvrmsValues.add(recordedHour + "#" + readingVal);
        } else if (readingType.matches("(?i:.*v.*)")) {
          rrVValues.add(recordedHour + "#" + readingVal);
        }
      } else if (dataSetFormat.equalsIgnoreCase("INTERVAL")) {
        if (readingType.matches("(?i:.*kwh.*)")) {
          irKwhValues.add(recordedHour + "#" + readingVal);
        } else if (readingType.matches("(?i:.*kwd.*)")) {
          irKwdValues.add(recordedHour + "#" + readingVal);
        } else if (readingType.matches("(?i:.*kvar.*)")) {
          irKvarValues.add(recordedHour + "#" + readingVal);
        } else if (readingType.matches("(?i:.*kvrms.*)")) {
          irKvrmsValues.add(recordedHour + "#" + readingVal);
        } else if (readingType.matches("(?i:.*v.*)")) {
          irVValues.add(recordedHour + "#" + readingVal);
        }
      }
    }

    // Build out the mongo document
    try {
      document = new BasicDBObject();
      document.append(amiDvcFieldName, meterId);
      document.append(dayFieldName, recordedDay);

      if (dataSetFormat.equalsIgnoreCase("REGISTER")) {
        if (!rrKwhValues.isEmpty()) {
          // If not converted to just array, bulk insertion is acting weird
          String[] kwhValues = new String[rrKwhValues.size()];
          kwhValues = rrKwhValues.toArray(kwhValues);
          document.put(registerFieldPrefix + "_kwh", kwhValues);
        }
        if (!rrKwdValues.isEmpty()) {
          String[] kwdValues = new String[rrKwdValues.size()];
          kwdValues = rrKwdValues.toArray(kwdValues);
          document.put(registerFieldPrefix + "_kwd", kwdValues);
        }
        if (!rrKvarValues.isEmpty()) {
          String[] kvarValues = new String[rrKvarValues.size()];
          kvarValues = rrKvarValues.toArray(kvarValues);
          document.put(registerFieldPrefix + "_kvar", kvarValues);
        }
        if (!rrKvrmsValues.isEmpty()) {
          String[] kvrmsValues = new String[rrKvrmsValues.size()];
          kvrmsValues = rrKvrmsValues.toArray(kvrmsValues);
          document.put(registerFieldPrefix + "_kvrms", kvrmsValues);
        }
        if (!rrVValues.isEmpty()) {
          String[] vValues = new String[rrVValues.size()];
          vValues = rrVValues.toArray(vValues);
          document.put(registerFieldPrefix + "_v", vValues);
        }
      } else if (dataSetFormat.equalsIgnoreCase("INTERVAL")) {
        if (!irKwhValues.isEmpty()) {
          String[] kwhValues = new String[irKwhValues.size()];
          kwhValues = irKwhValues.toArray(kwhValues);
          document.put(intervalFieldPrefix + "_kwh", kwhValues);
        }
        if (!irKwdValues.isEmpty()) {
          String[] kwdValues = new String[irKwdValues.size()];
          kwdValues = irKwdValues.toArray(kwdValues);
          document.put(intervalFieldPrefix + "_kwd", kwdValues);
        }
        if (!irKvarValues.isEmpty()) {
          String[] kvarValues = new String[irKvarValues.size()];
          kvarValues = irKvarValues.toArray(kvarValues);
          document.put(intervalFieldPrefix + "_kvar", kvarValues);
        }
        if (!irKvrmsValues.isEmpty()) {
          String[] kvrmsValues = new String[irKvrmsValues.size()];
          kvrmsValues = irKvrmsValues.toArray(kvrmsValues);
          document.put(intervalFieldPrefix + "_kvrms", kvrmsValues);
        }
        if (!irVValues.isEmpty()) {
          String[] vValues = new String[irVValues.size()];
          vValues = irVValues.toArray(vValues);
          document.put(intervalFieldPrefix + "_v", vValues);
        }
      }

      builder.insert(document);
      //documents.add(document);
      //System.out.println(document.toString());
      //System.out.println(String.format("mid:%s; day:%s; _kwh:%s", meterId, recordedDay, rrKwhValues.toString()));
      dbCounter++;

      //collection.insert(document);

      if (dbCounter % batchSize == 0) {
        result = builder.execute(writeConcern);
        //collection.insert(documents, writeConcern);
        context.getCounter(MongoBulkLoadDriver.BULKLOAD.NUM_MONGO_INSERT_OPS).increment(result.getInsertedCount());
        //context.getCounter(MongoBulkLoadDriver.BULKLOAD.NUM_MONGO_INSERT_OPS).increment(documents.size());
        //documents.clear();
        builder = collection.initializeUnorderedBulkOperation();
      }
    } catch (Exception ex) {
      logger.error(ex.getMessage());
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
    //if (documents != null && documents.size() > 0) {
      try {
        result = builder.execute(writeConcern);
        context.getCounter(MongoBulkLoadDriver.BULKLOAD.NUM_MONGO_INSERT_OPS).increment(result.getInsertedCount());
        //collection.insert(documents, writeConcern);
        //context.getCounter(MongoBulkLoadDriver.BULKLOAD.NUM_MONGO_INSERT_OPS).increment(documents.size());
      } catch (Exception ex) {
        logger.error(ex.getMessage());
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
