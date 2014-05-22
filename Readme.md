MapReduce application to process data sets in hdfs and put them to mongodb


Building a assembly jar:

```
mvn clean compile assembly:single
```

Optional: Create index on mongo collection being used to improve the performance of update operations

> Note: use the appropriate value for database, collection and field names as used in the mapper class of the application

```
use bulk
db.ami.ensureIndex({mid: 1, rd: 1})
```


Running the mapreduce application:

```
hadoop jar bulkload-*-SNAPSHOT-jar-with-dependencies.jar com.cloudwick.mongo.MongoBulkLoadDriver [input_path] [register|interval] [mongo_servers]
```