package com.cloudwick.mongo;

import com.mongodb.*;

import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Description goes here
 *
 * @author ashrith
 */
public class QueryMongo {
  private static MongoClient mongoClient = null;
  private static DBCollection collection;

  private static String username = "bulkDBAdmin";
  private static String password = "password";
  private static String databaseName = "bulk";
  private static String registerCollectionName = "register_reads";
  private static String intervalCollectionName = "interval_reads";
  private static String registerFieldName = "rr_kwh";
  private static String intervalFieldName = "ir_kwh";

  public static void setup(String servers, String collectionFormat) throws UnknownHostException {
    List<MongoCredential> creds = new ArrayList<MongoCredential>();
    creds.add(MongoCredential.createMongoCRCredential(username, databaseName, password.toCharArray()));
    List<ServerAddress> serverAddresses = new ArrayList<ServerAddress>();

    for (String server : Arrays.asList(servers.split(","))) {
      serverAddresses.add(new ServerAddress(server));
    }

    mongoClient = new MongoClient(serverAddresses, creds);
    System.out.println("Connected to: " + mongoClient.getAddress());
    DB db = mongoClient.getDB(databaseName);
    String collectionName = null;

    if(collectionFormat.equalsIgnoreCase("REGISTER"))
      collectionName = registerCollectionName;
    else
      collectionName = intervalCollectionName;

    collection = db.getCollection(collectionName);
  }

  public static List<String> query(String collectionFormat, String mid, String start, String end) {
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm");
    String valuesFieldName = null;
    if(collectionFormat.equalsIgnoreCase("REGISTER"))
      valuesFieldName = registerFieldName;
    else
      valuesFieldName = intervalFieldName;

    df.setTimeZone(TimeZone.getTimeZone("GMT"));
    BasicDBObject query = new BasicDBObject();
    query.put("mid", mid);
    query.put("rd", new BasicDBObject("$gte", start).append("$lte", end));
    // fields to output
    BasicDBObject fields = new BasicDBObject();
    fields.put("_id", false); // do not output _id
    DBCursor cursor = null;
    String json = "";
    try {
      cursor = collection.find(query, fields);
      while(cursor.hasNext()) {
        DBObject o = cursor.next();
        System.out.println(o);
        BasicDBList values = (BasicDBList) o.get(valuesFieldName);
        boolean firstDoc = true;
        for (Object value : values) {
          String[] splits = value.toString().split("#");
          if (!firstDoc)
            json += ",";
          if (firstDoc)
            firstDoc = false;
          json += "{ \"x\" : ";
          json += df.parse(o.get("rd") + " " + splits[0]).getTime();
          json += ", \"y\" : ";
          json += splits[1];
          json += " }";
        }
      }
    } catch (ParseException e) {
      e.printStackTrace();
    } finally {
      if (cursor != null) {
        cursor.close();
      }
    }
    return new ArrayList<String>(Arrays.asList(json));
  }

  public static void findTenDocs() {
    DBCursor cursor = collection.find().limit(10);
    while(cursor.hasNext()) {
      System.out.println(cursor.next());
    }
  }

  public static void main(String[] args) throws UnknownHostException {
    if (args.length != 5) {
      System.err.println("Required number of args 5 instead got " + args.length);
      System.err.println("  Options: [servers] [collectionFormat] [meterID] [startDate] [endDate]");
      System.exit(1);
    }
    System.out.println("Args: " + Arrays.toString(args));
    String servers = args[0];
    String collectionFormat = args[1];
    String meterId = args[2];
    String startDate = args[3];
    String endData = args[4];
    setup(servers, collectionFormat);

    System.out.println(query(collectionFormat, meterId, startDate, endData));
  }
}
