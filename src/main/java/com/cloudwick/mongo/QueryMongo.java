package com.cloudwick.mongo;

import com.mongodb.*;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

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
  private static String collectionName = "ami";

  public static void setup(String servers) throws UnknownHostException {
    List<MongoCredential> creds = new ArrayList<MongoCredential>();
    creds.add(MongoCredential.createMongoCRCredential(username, databaseName, password.toCharArray()));
    List<ServerAddress> serverAddresses = new ArrayList<ServerAddress>();
    for (String server : servers.split(";")) {
      serverAddresses.add(new ServerAddress(server));
    }
    mongoClient = new MongoClient(serverAddresses, creds);
    DB db = mongoClient.getDB(databaseName);
    collection = db.getCollection(collectionName);
  }

  public static List<DBObject> query(String mid, String start, String end) {
    List<DBObject> out = new ArrayList<DBObject>();
    BasicDBObject query = new BasicDBObject();
    query.put("mid", mid);
    query.put("rd", new BasicDBObject("gte", start).append("lt", end));
    DBCursor cursor = collection.find(query);
    while(cursor.hasNext()) {
      out.add(cursor.next());
    }
    return out;
  }

  public static void main(String[] args) throws UnknownHostException {
    if (args.length != 4) {
      System.err.println("Required number of args 4 instead got " + args.length);
      System.exit(1);
    }
    String servers = args[0];
    String meterId = args[1];
    String startDate = args[2];
    String endData = args[3];
    setup(servers);
    for (DBObject o: query(meterId, startDate, endData)) {
      System.out.println(o);
    }
  }
}
