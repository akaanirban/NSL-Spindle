# Spark Sessionization

# Stateful Operations

## Operators

- `updateStateByKey`
- `mapWithState`
    - Higher performance
    - "Besides timeouts, developers can also set partitioning schemes and initial state information for bootstrapping" ([databricks][])
    - Requires a `StateSpec` function to be specified
        - Can specify a custom partitioner
            - Idea: have key include both timestamp and some shard id
               - Custom partitioner to partition on shard-id

## Getting State

`updateStateByKey` returns a `DStream` of `RDD`s containing the current state and `mapWithState` can do the same using
the `snapshotStream` method.

# Structured Streaming

Spark 2.0 supports as an "Alpha" feature Structured Streaming, where DStreams are treated as updates to an "unbounded table."
Structured Streaming is used in conjunction with the Dataset API and Spark SQL.

"Aggregations over a sliding event-time window are straightforward with Structured Streaming. The key idea to understand about window-based aggregations are very similar to grouped aggregations. In a grouped aggregation, aggregate values (e.g. counts) are maintained for each unique value in the user-specified grouping column. In case of window-based aggregations, aggregate values are maintained for each window the event-time of a row falls into" ([sparkdocs][]).

# References

[gitbooks][]

[databricks][]

[Structured Streaming Documentation][sparkdocs]

[gitbooks]: https://jaceklaskowski.gitbooks.io/mastering-apache-spark/content/spark-streaming/spark-streaming-operators-stateful.html "Stateful Spark Streaming Operators Gitbook Documentation"
[databricks]: https://databricks.com/blog/2016/02/01/faster-stateful-stream-processing-in-apache-spark-streaming.html "Fastre Stateful Stream Processing Databricks Post"
[sparkdocs]: http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#handling-event-time-and-late-data "Spark Structured Streaming Documentation"
