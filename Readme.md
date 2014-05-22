MapReduce application to process data sets in hdfs and put them to mongodb


Building a assembly jar:

```
mvn clean compile assembly:single
```


Running the mapreduce application:

```
hadoop jar bulkload-*-SNAPSHOT-jar-with-dependencies.jar [input_path] [register|interval]
```