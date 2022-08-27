---
id: reference
title: Multi-stage query reference
sidebar_label: Reference
---

> SQL-based ingestion and the multi-stage query task engine are experimental features available starting in Druid 24.0. You can use it in place of the existing native batch and Hadoop based ingestion systems. As an experimental feature, functionality documented on this page is subject to change or removal in future releases. Review the release notes and this page to stay up to date on changes.

This topic is a reference guide for the multi-stage query architecture in Apache Druid.

## Context parameters

In addition to the Druid SQL [context parameters](../querying/sql-query-context.md), the multi-stage query task engine accepts certain context parameters that are specific to it. 

Use context parameters alongside your queries to customize the behavior of the query. If you're using the API, include the context parameters in the query context when you submit a query:

```json
{
  "query": "SELECT 1 + 1",
  "context": {
    "<key>": "<value>",
    "maxNumTasks": 3
  }
}
```

If you're using the Druid console, you can specify the context parameters through various UI options.

The following table lists the context parameters for the MSQ task engine:

|Parameter|Description|Default value|
|---------|-----------|-------------|
| maxNumTasks | SELECT, INSERT, REPLACE<br /><br />The maximum total number of tasks to launch, including the controller task. The lowest possible value for this setting is 2: one controller and one worker. All tasks must be able to launch simultaneously. If they cannot, the query returns a `TaskStartTimeout` error code after approximately 10 minutes.<br /><br />May also be provided as `numTasks`. If both are present, `maxNumTasks` takes priority.| 2 |
| taskAssignment | SELECT, INSERT, REPLACE<br /><br />Determines how many tasks to use. Possible values include: <ul><li>`max`: Use as many tasks as possible, up to the maximum `maxNumTasks`.</li><li>`auto`: Use as few tasks as possible without exceeding 10 GiB or 10,000 files per task. Review the [limitations](./msq-known-issues.md#general-query-execution) of `auto` mode before using it.</li></ui>| `max` |
| finalizeAggregations | SELECT, INSERT, REPLACE<br /><br />Determines the type of aggregation to return. If true, Druid finalizes the results of complex aggregations that directly appear in query results. If false, Druid returns the aggregation's intermediate type rather than finalized type. This parameter is useful during ingestion, where it enables storing sketches directly in Druid tables. For more information about aggregations, see [SQL aggregation functions](../querying/sql-aggregations.md). | true |
| rowsInMemory | INSERT or REPLACE<br /><br />Maximum number of rows to store in memory at once before flushing to disk during the segment generation process. Ignored for non-INSERT queries. In most cases, use the default value. You may need to override the default if you run into one of the [known issues around memory usage](./msq-known-issues.md#memory-usage)</a>. | 100,000 |
| segmentSortOrder | INSERT or REPLACE<br /><br />Normally, Druid sorts rows in individual segments using `__time` first, followed by the [CLUSTERED BY](./index.md#clustered-by) clause. When you set `segmentSortOrder`, Druid sorts rows in segments using this column list first, followed by the CLUSTERED BY order.<br /><br />You provide the column list as comma-separated values or as a JSON array in string form. If your query includes `__time`, then this list must begin with `__time`. For example, consider an INSERT query that uses `CLUSTERED BY country` and has `segmentSortOrder` set to `__time,city`. Within each time chunk, Druid assigns rows to segments based on `country`, and then within each of those segments, Druid sorts those rows by `__time` first, then `city`, then `country`. | empty list |
| maxParseExceptions| SELECT, INSERT, REPLACE<br /><br />Maximum number of parse exceptions that are ignored while executing the query before it stops with `TooManyWarningsFault`. To ignore all the parse exceptions, set the value to -1.| 0 |
| rowsPerSegment | INSERT or REPLACE<br /><br />The number of rows per segment to target. The actual number of rows per segment may be somewhat higher or lower than this number. In most cases, use the default. For general information about sizing rows per segment, see [Segment Size Optimization](../operations/segment-optimization.md). | 3,000,000 |
| durableShuffleStorage | SELECT, INSERT, REPLACE <br /><br />Whether to use durable storage for shuffle mesh. To use this feature, configure the durable storage at the server level using `druid.msq.intermediate.storage.enable=true` and its [associated properties](./msq-durable-storage.md). If these properties are not configured, any query with the context variable `durableShuffleStorage=true` fails with a configuration error. <br /><br /> | false |
| sqlTimeZone | Sets the time zone for this connection, which affects how time functions and timestamp literals behave. Use a time zone name like "America/Los_Angeles" or offset like "-08:00".| `druid.sql.planner.sqlTimeZone` on the Broker (default: UTC)|
| useApproximateCountDistinct | Whether to use an approximate cardinality algorithm for `COUNT(DISTINCT foo)`.| `druid.sql.planner.useApproximateCountDistinct` on the Broker (default: true)|

## Durable storage properties

The following table describes the properties used to configure durable storage:

| Config | Description | Required | Default |
|--------|-------------|----------|---------|
| `druid.msq.intermediate.storage.enable` | Set to `true` to enable this feature. | Yes | |
| `druid.msq.intermediate.storage.type` | Set the value to `s3`. | Yes | |
| `druid.msq.intermediate.storage.bucket` | S3 bucket to store intermediate stage results. | Yes | |
| `druid.msq.intermediate.storage.prefix` | S3 prefix to store intermediate stage results. Provide a unique value for the prefix. Don't share the same prefix between clusters. | Yes | |
| `druid.msq.intermediate.storage.tempDir`| Directory path on the local disk to temporarily store intermediate stage results. | Yes | |
| `druid.msq.intermediate.storage.maxResultsSize` | Max size of each partition file per stage. It should be between 5 MiB and 5 TiB. Supports a human-readable format. For example, if a stage has 50 partitions and each partition file is below or equal to 5 TiB, you can effectively use S3 up to 250 TiB of stage output. | No | 100 MiB |
| `druid.msq.intermediate.storage.chunkSize` | Defines the size of each chunk to temporarily store in `druid.msq.intermediate.storage.tempDir`. The chunk size must be between 5 MiB and 5 GiB. Druid computes the chunk size automatically if no value is provided.| No | |
| `druid.msq.intermediate.storage.maxTriesOnTransientErrors` | Defines the max number times to attempt S3 API calls to avoid failures due to transient errors. | No | 10 |

## Error codes

Error codes have corresponding human-readable messages that explain the error. For more information about the error codes, see [Error codes](./msq-concepts.md#error-codes).

## SQL syntax

The MSQ task engine has three primary SQL functions: 

- EXTERN
- INSERT
- REPLACE

For information about using these functions and their corresponding examples, see [MSQ task engine query syntax](./index.md#msq-task-engine-query-syntax). For information about adjusting the shape of your data, see [Adjust query behavior](./index.md#adjust-query-behavior).

### EXTERN

Use the EXTERN function to read external data.

Function format:

```sql
SELECT
 <column>
FROM TABLE(
  EXTERN(
    '<Druid input source>',
    '<Druid input format>',
    '<row signature>'
  )
)
```
/native-batch.html#input-sources
EXTERN consists of the following parts:

1.  Any [Druid input source](../ingestion/native-batch-input-source.md) as a JSON-encoded string.
2.  Any [Druid input format](../ingestion/data-formats.md) as a JSON-encoded string.
3.  A row signature, as a JSON-encoded array of column descriptors. Each column descriptor must have a `name` and a `type`. The type can be `string`, `long`, `double`, or `float`. This row signature is used to map the external data into the SQL layer.

### INSERT

Use the INSERT function to insert data.

Function format:

```sql
INSERT INTO <table name>
SELECT
  <column>
FROM <table>
PARTITIONED BY <time frame>
```

INSERT consists of the following parts:

1. Optional [context parameters](./msq-reference.md#context-parameters).
2. An `INSERT INTO <dataSource>` clause at the start of your query, such as `INSERT INTO w000`.
3. A clause for the data you want to insert, such as `SELECT...FROM TABLE...`. You can use EXTERN to reference external tables using the following format: ``TABLE(EXTERN(...))`.
4. A [PARTITIONED BY](./index.md#partitioned-by) clause for your INSERT statement. For example, use `PARTITIONED BY DAY` for daily partitioning or `PARTITIONED BY ALL TIME` to skip time partitioning completely.
5. An optional [CLUSTERED BY](./index.md#clustered-by) clause.

### REPLACE

You can use the REPLACE function to replace all or some of the data.

#### REPLACE all data

Function format to replace all data:

```sql
REPLACE INTO <target table>
OVERWRITE ALL
SELECT
  TIME_PARSE("timestamp") AS __time,
  <column>
FROM <source table>

PARTITIONED BY <time>
```

#### REPLACE specific data

Function format to replace specific data:

```sql
REPLACE INTO <target table>
OVERWRITE WHERE __time >= TIMESTAMP '<lower bound>' AND __time < TIMESTAMP '<upper bound>'
SELECT
  TIME_PARSE("timestamp") AS __time,
  <column>
FROM <source table>

PARTITIONED BY <time>
```

REPLACE consists of the following parts:

1. Optional [context parameters](./msq-reference.md#context-parameters).
2. A `REPLACE INTO <dataSource>` clause at the start of your query, such as `REPLACE INTO w000`.
3. An OVERWRITE clause after the datasource, either OVERWRITE ALL or OVERWRITE WHERE:
  - OVERWRITE ALL replaces the entire existing datasource with the results of the query.
  - OVERWRITE WHERE drops the time segments that match the condition you set. Conditions are based on the `__time` column and use the format `__time [< > = <= >=] TIMESTAMP`. Use them with AND, OR, and NOT between them, inclusive of the timestamps specified. For example, see [REPLACE INTO ... OVERWRITE WHERE ... SELECT](./index.md#replace-some-data).
4. A clause for the actual data you want to use for the replacement.
5. A [PARTITIONED BY](./index.md#partitioned-by) clause to your REPLACE statement. For example, use PARTITIONED BY DAY for daily partitioning, or PARTITIONED BY ALL TIME to skip time partitioning completely.
6. An optional [CLUSTERED BY](./index.md#clustered-by) clause.
 