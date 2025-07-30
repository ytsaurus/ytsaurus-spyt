## Current SPYT development roadmap

_As new versions are released, we will update tasks in this roadmap with corresponding versions and add new tasks._
***

### Integration with the YTsaurus shuffle service

[X] The internal shuffle service of YTsaurus will be used for sorting and subsequent access to sorted portions of data. It is also planned to support saving the sorted parts in case of unexpected aborts of executors. It will not accelerate time of operations in case when no one interruption is in place, but will improve time in case when several aborts have taken, which are the norm on average. (Available from 2.7.0, but require YTsaurus 25.2 version).

### Support for dynamic allocation by changing the number of jobs in a running operation (as part of direct spark-submit)

[ ] Depending on the size of the table being processed, SPYT in direct spark-submit mode will be able to adjust the number of executors on the fly. For large tables, add them, and for small ones, on the contrary, reduce them.

### Support columnar-statistics with using Spark 3.4.x

[ ] YTsaurus metadata stores information about the generational statistics of tables, for example, for the number of values or the number of unique values in a column. Currently, this information is not used by Spark in any way when building a query plan. By adding the ability to take this information into account, we expect that the query plan will be built more efficiently.

### Support Spark SQL via Query Tracker for working with dynamic tables

[X] Currently, in a query via Query Tracker, in order to read data from a dynamic table, it is necessary to specify a suffix in the name of the table in the form of a timestamp, at the time of which you need to read the data slice. Which is generally inconvenient, because it is necessary to find and apply this timestamp to table name. And when you rerun that query in after a certain period of time, the timestamp may change, so user will need to take an actual value of timestamp . We plan to automatically find the last timestamp of the table from its "last_commit_timestamp" attribute. (Available from 2.7.0).

### Support for Java 17

[X] Java 11 are currently supported. To integrate with Spark 4.x.x. it will need support for Java 17. (Available from 2.6.0 - check the compatibility matrix).

### Support for Scala 2.13

[ ] Scala 2.12 are currently supported. To integrate with Spark 4.x.x. it will need support for Scala 2.13.

### Support for Apache Spark 3.3.x - 3.5.6
[X] All versions of Apache Spark 3 generation is supported. (Available from 2.5.0 - check the compatibility matrix).

### Support for Apache Spark 4.x.x
[ ] The new generation of Spark. 

### Spark Streaming
[X] Support Spark Structured Streaming with YTsaurus Queues for micro-batch processing. (Available from 2.6.5).

### Spark Connect
[ ] Deprecate Apache Livy server and support Spark Connect for using via Query Tracker for using Spark SQL.
