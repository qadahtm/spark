StreamSQL
===
high-level APIs in Scala, Java, and Python, and an optimized engine that
supports general computation graphs for data analysis. It also supports a
rich set of higher-level tools including Spark SQL for SQL and structured
data processing, MLlib for machine learning, GraphX for graph processing,
and Spark Streaming for stream processing.

<http://spark.apache.org/>
(You do not need to do this if you downloaded a pre-built package.)


StreamSQL is a Spark component based on [Catalyst](https://github.com/apache/spark/tree/master/sql) and [Spark Streaming](https://github.com/apache/spark/tree/master/streaming), aiming to support SQL-style queries on data streams. Our target is to advance the progress of Catalyst as well as Spark Streaming by bridging the gap between structured data queries and stream processing.

Our StreamSQL provides:

1. Full SQL support on streaming data and extended time-based aggregation and join.
2. Easy mutual operation between DStream and SQL.
3. Table and stream mutual operation with a simple query.

### An Example ###

**Creating StreamSQLContext**

`StreamSQLContext` is the entry point for all DStream related functionalities. It is the counterpart of `SQLContext` for Spark. `StreamingContext` can be created as below.
examples to a cluster. This can be a mesos:// or spark:// URL, 
"yarn-cluster" or "yarn-client" to run on YARN, and "local" to run 
locally with one thread, or "local[N]" to run locally with N threads. You 
can also use an abbreviated class name if the class is in the `examples`
package. For instance:

    val ssc: StreamingContext
    val streamSqlContext = new StreamSQLContext(ssc)

    import streamSqlContext._

**Running SQL on DStreams:**

    case class Person(name: String, age: String)

    // Create an DStream of Person objects and register it as a stream.
    val people: DStream[Person] = ssc.socketTextStream(serverIP, serverPort)
      .map(_.split(","))
      .map(p => Person(p(0), p(1).toInt))

    people.registerAsStream("people")

    val teenagers = sql("SELECT name FROM people WHERE age >= 10 && age <= 19")

    // The results of SQL queries are themselves DStreams and support all the normal operations
    teenagers.map(t => "Name: " + t(0)).print()
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

**Join Stream with Table**

    val sqlContext = streamSqlContext.sqlContext

    val historyData = sc.textFile("persons").map(_.split(",")).map(p => Person(p(0), p(1).toInt))
    val schemaData = sqlContext.createSchemaRDD(historyData)
    schemaData.registerAsTable("records")

    val result = sql("SELECT * FROM records JOIN people ON people.name = records.name")
    result.print()
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

**Writing Language Integrated Relational Queries:**

    val teenagers = people.where('age >= 10).where('age <= 19).select('name).toDstream

**Combining Hive**

    val ssc: StreamingContext
    val streamHiveContext = new StreamHiveContext(ssc)

    import streamHiveContext._

    streamHiveContext.streamHql(
      """
        |CREATE STREAM IF NOT EXISTS src (
        |     key STRING,
        |     value INT)
        |ROW FORMAT DELIMITED
        |   FIELDS TERMINATED BY ' '
        |STORED AS
        |  INPUTFORMAT 'org.apache.hadoop.mapred.TextKafkaInputFormat'
        |  OUTPUTFORMAT 'org.apache.hadoop.mapred.DummyStreamOutputFormat'
        |LOCATION 'kafka://localhost:2181/topic=test/group=aa'
      """.stripMargin)

    sql("SELECT key, value FROM src").print()
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

### How To Build and Deploy ###

StreamSQL is fully based on [Spark](http://spark.apache.org/), to build and deploy please refer to
[Spark document](http://spark.apache.org/documentation.html).

### Future List ###

1. Support time-based window slicing on data.

Details please refer to [design document](https://github.com/thunderain-project/StreamSQL/wiki).
