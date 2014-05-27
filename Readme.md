MapReduce application to process data sets in hdfs and put them to mongodb

Building a assembly jar:

```
mvn clean compile assembly:single
```

Optional: Create index on mongo collection being used to improve the performance of read operations

> Note: use the appropriate value for database, collection and field names as used in the mapper class of the application

```
use bulk
sh.enableSharding("bulk")
db.ami.ensureIndex({"_id": "hashed"})
db.ami.ensureIndex({mid: 1, rd: 1})
sh.shardCollection("bulk.ami", { "_id": "hashed" })
```


Running the mapreduce application:

```
hadoop jar bulkload-*-SNAPSHOT-jar-with-dependencies.jar \
  com.cloudwick.mongo.MongoBulkLoadDriver \
  [input_path] \
  [register|interval] \
  [mongo_servers]
```

Examples:

Running bulk loader against register dataset located at hdfs path `/register/data` and connecting to mongo servers
at "mongos1.cw.com:27017" and "mongos2.cw.com:27017"

```
hadoop jar bulkload-*-SNAPSHOT-jar-with-dependencies.jar \
  com.cloudwick.mongo.MongoBulkLoadDriver \
  -Dmapreduce.job.reduces=2 \
  -Dmapreduce.task.io.sort.factor=25 \
  -Dmapreduce.task.io.sort.mb=256 \
  /register/data \
  register \
  "mongos1.cw.com:27017,mongos2.cw.com:27017"
```

Mongo Schema For register reads:

```json
{
  _id:      ObjectId(),
  mid:      "meter id",
  rd:       "reading recorded date",
  rr_kwh:   "Array of register meter readings for a specified day in kwh",
  rr_kwd:   "Array of register meter readings for a specified day in kwd",
  rr_kvar:  "Array of register meter readings for a specified day in kvar",
  rr_kvrms: "Array of register meter readings for a specified day in kvrms",
  rr_v:     "Array of register meter readings for a specified day in voltage",
}
```

Mongo Schema for interval reads:

```json
{
  _id:      ObjectId(),
  mid:      "meter id",
  rd:       "reading recorded date",
  ir_kwh:   "Array of interval meter readings for a specified day in kwh",
  ir_kwd:   "Array of interval meter readings for a specified day in kwd",
  ir_kvar:  "Array of interval meter readings for a specified day in kvar",
  ir_kvrms: "Array of interval meter readings for a specified day in kvrms",
  ir_v:     "Array of interval meter readings for a specified day in voltage"
}
```
