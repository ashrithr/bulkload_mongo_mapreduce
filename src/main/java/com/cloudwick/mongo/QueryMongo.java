package com.cloudwick.mongo;

import com.mongodb.*;

import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Queries mongo for premise information and gets the amc_device_name and their start-end dates and based on the
 * retrieved information queries register or interval reads collections and returns values based on hourly or daily
 * format
 *
 * @author ashrith
 */
public class QueryMongo {
    private static final String username = "bulkDBAdmin";
    private static final String password = "password";
    private static final String databaseName = "bulk";
    private static final String registerCollectionName = "register_reads";
    private static final String intervalCollectionName = "interval_reads";
    private static final String campDatabase = "amitest";
    private static final String campCollectionName = "camp_meter_new";
    private static final String registerFieldName = "rr_kwh";
    private static final String intervalFieldName = "ir_kwh";
    private static final String meterIdFieldName = "mid";
    private static final String dateRecordedFieldName = "rd";
    private static final String amiFieldName = "ami_dvc_name";
    private static final String premiseFieldName = "prem_num";
    private static final String campStartDateFieldName = "startDate";
    private static final String campEndDateFieldName = "endDate";
    private static DBCollection collection;
    private static DBCollection campCollection;
    private static double queryTime;

    public static void setup(String servers, String collectionFormat) throws UnknownHostException {
        List<MongoCredential> creds = new ArrayList<MongoCredential>();
        creds.add(MongoCredential.createMongoCRCredential(username, databaseName, password.toCharArray()));
        List<ServerAddress> serverAddresses = new ArrayList<ServerAddress>();

        List<MongoCredential> campCreds = new ArrayList<MongoCredential>();
        campCreds.add(MongoCredential.createMongoCRCredential(username, campDatabase, password.toCharArray()));

        for (String server : Arrays.asList(servers.split(","))) {
            serverAddresses.add(new ServerAddress(server));
        }

        MongoClient mongoClient = new MongoClient(serverAddresses, creds);
        MongoClient campMongoClient = new MongoClient(serverAddresses, campCreds);
        // System.out.println("Connected to: " + mongoClient.getAddress());
        DB db = mongoClient.getDB(databaseName);
        DB campDB = campMongoClient.getDB(campDatabase);
        String collectionName;

        if (collectionFormat.equalsIgnoreCase("REGISTER"))
            collectionName = registerCollectionName;
        else
            collectionName = intervalCollectionName;

        collection = db.getCollection(collectionName);
        campCollection = campDB.getCollection(campCollectionName);
    }

    public static DBCursor queryMongo(DBCollection collection, BasicDBObject query, BasicDBObject fields) {
        // long t1 = System.nanoTime();
        DBCursor cursor = collection.find(query, fields);
        // long t2 = System.nanoTime();
        // double timeTook = ((t2 - t1) * 1e-6);
        // System.out.println("Executing query (" + query + ") took: " + timeTook + " milliseconds");
        // queryTime += timeTook;

        return cursor;
    }

    public static List<String> query(String collectionFormat, String mid, String userStart, String userEnd, String duration) {
        BasicDBObject campQuery = new BasicDBObject(premiseFieldName, mid);
        BasicDBObject campFields = new BasicDBObject("_id", false)
              .append(amiFieldName, true)
              .append(campStartDateFieldName, true)
              .append(campEndDateFieldName, true);
        DBCursor campCursor = null;
        //String json = "";
        StringBuilder jsonOutput = new StringBuilder();
        Calendar calendar = Calendar.getInstance();
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        SimpleDateFormat daily = new SimpleDateFormat("yyyy-MM-dd");
        List<DBObject> amiDvcInfo = new ArrayList<DBObject>();

        try {
            // System.out.println("Getting ami_device_name and start_end dates for premise_name '" + mid + "'");
            campCursor = queryMongo(campCollection, campQuery, campFields);

            while (campCursor.hasNext()) {
                DBObject obj = campCursor.next();
                amiDvcInfo.add(obj);
            }

            // System.out.println("Executing '" + amiDvcInfo.size() + "' queries against '" + collection.getFullName() + "' collection");
            for (DBObject amiDevice : amiDvcInfo) {
                String meterId = (String) amiDevice.get(amiFieldName);
                String startDate = (String) amiDevice.get(campStartDateFieldName);
                String endDate = (String) amiDevice.get(campEndDateFieldName);
                String valuesFieldName;

                String finalStartDate = null;
                String finalEndDate = null;

                if (userStart != null) {
                    if (daily.parse(userStart).before(daily.parse(startDate)))
                        finalStartDate = startDate;
                    else
                        finalStartDate = userStart;
                } else {
                    finalStartDate = startDate;
                }

                if (userEnd != null) {
                    if (daily.parse(userEnd).after(daily.parse(endDate)))
                        finalEndDate = endDate;
                    else
                        finalEndDate = userEnd;
                } else {
                    finalEndDate = endDate;
                }

                if (collectionFormat.equalsIgnoreCase("REGISTER"))
                    valuesFieldName = registerFieldName;
                else
                    valuesFieldName = intervalFieldName;

                // df.setTimeZone(TimeZone.getTimeZone("GMT"));

                BasicDBObject query = new BasicDBObject();
                query.put(meterIdFieldName, meterId);
                query.put(dateRecordedFieldName, new BasicDBObject("$gte", finalStartDate).append("$lte", finalEndDate));
                // fields to output
                BasicDBObject fields = new BasicDBObject();
                fields.put("_id", false); // do not output _id
                DBCursor cursor = null;
                try {
                    cursor = queryMongo(collection, query, fields);
                    boolean firstDoc = true;
                    while (cursor.hasNext()) {
                        DBObject o = cursor.next();
                        BasicDBList values = (BasicDBList) o.get(valuesFieldName);
                        if (duration.equalsIgnoreCase("DAILY")) {
                            Double dailyResult = 0.0;
                            for (Object value : values) {
                                String[] splits = value.toString().split("#");
                                dailyResult += Double.parseDouble(splits[1]);
                            }
                            if (!firstDoc)
                                jsonOutput.append(",");
                            if (firstDoc)
                                firstDoc = false;
                            jsonOutput.append("{ \"x\" : ");
                            Date d = daily.parse((String) o.get("rd"));
                            jsonOutput.append(d.getTime());
                            jsonOutput.append(", \"y\" : ");
                            jsonOutput.append(String.format("%.16f", dailyResult));
                            calendar.setTime(d);
                            if (calendar.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY || calendar.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY) {
                                jsonOutput.append(", \"color\" : ");
                                jsonOutput.append("\"#476BB2\"");
                            }
                            jsonOutput.append(" }");
                        } else {
                            for (Object value : values) {
                                String[] splits = value.toString().split("#");
                                if (!firstDoc)
                                    jsonOutput.append(",");
                                if (firstDoc)
                                    firstDoc = false;
                                jsonOutput.append("{ \"x\" : ");
                                Date d = df.parse(o.get("rd") + " " + splits[0]);
                                jsonOutput.append(d.getTime());
                                jsonOutput.append(", \"y\" : ");
                                jsonOutput.append(splits[1]);
                                calendar.setTime(d);
                                if (calendar.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY || calendar.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY) {
                                    jsonOutput.append(", \"color\" : ");
                                    jsonOutput.append("\"#476BB2\"");
                                }
                                jsonOutput.append(" }");
                            }
                        }
                    }
                } catch (ParseException e) {
                    e.printStackTrace();
                } finally {
                    if (cursor != null) {
                        cursor.close();
                    }
                }
            }
        } catch (ParseException e) {
            e.printStackTrace();
        } finally {
            if (campCursor != null)
                campCursor.close();
        }

        return new ArrayList<String>(Arrays.asList(jsonOutput.toString()));
    }

    public static void findTenDocs() {
        DBCursor cursor = collection.find().limit(10);
        while (cursor.hasNext()) {
            System.out.println(cursor.next());
        }
    }

    public static void main(String[] args) throws UnknownHostException {
        if (args.length < 4) {
            System.err.println("Required number of args 3 instead got " + args.length);
            System.err.println("  Options: [servers] [collectionFormat] [premiseNum] [hourly|daily]");
            System.exit(1);
        }
        // System.out.println("Args: " + Arrays.toString(args));
        String startDate = null;
        String endDate = null;
        String servers = args[0];
        String collectionFormat = args[1];
        String premiseNum = args[2];
        String duration = args[3];
        if (args.length  > 4) {
            startDate = args[4];
            endDate = args[5];
        }
        setup(servers, collectionFormat);

        // long t1 = System.nanoTime();
        System.out.println(query(collectionFormat, premiseNum, startDate, endDate, duration));
        // long t2 = System.nanoTime();
        // System.out.println("Total mongo query time: " + queryTime + " milliseconds");

        // System.out.println("Execution time: " + ((t2 - t1) * 1e-6) + " milliseconds");
    }
}
