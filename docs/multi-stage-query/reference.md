---
id: reference
title: SQL-based ingestion reference
sidebar_label: Reference
---

<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->

> This page describes SQL-based batch ingestion using the [`druid-multi-stage-query`](../multi-stage-query/index.md)
> extension, new in Druid 24.0. Refer to the [ingestion methods](../ingestion/index.md#batch) table to determine which
> ingestion method is right for you.

## SQL reference

This topic is a reference guide for the multi-stage query architecture in Apache Druid. For examples of real-world
usage, refer to the [Examples](examples.md) page.

### `EXTERN`

Use the `EXTERN` function to read external data.

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

`EXTERN` consists of the following parts:

1. Any [Druid input source](../ingestion/native-batch-input-source.md) as a JSON-encoded string.
2. Any [Druid input format](../ingestion/data-formats.md) as a JSON-encoded string.
3. A row signature, as a JSON-encoded array of column descriptors. Each column descriptor must have a `name` and a `type`. The type can be `string`, `long`, `double`, or `float`. This row signature is used to map the external data into the SQL layer.

For more information, see [Read external data with EXTERN](concepts.md#extern).

### `HTTP`, `INLINE` and `LOCALFILES`

While `EXTERN` allows you to specify an external table using JSON, other table functions allow you
describe the external table using SQL syntax. Each function works for one specific kind of input
source. You provide properties using SQL named arguments. The row signature is given using the
Druid SQL `EXTEND` keyword using SQL syntax and types. Function format:

```sql
SELECT
 <column>
FROM TABLE(
  http(
    userName => 'bob',
    password => 'secret',
    uris => 'http:foo.com/bar.csv',
    format => 'csv'
    )
  ) EXTEND (x VARCHAR, y VARCHAR, z BIGINT)
```

Note that the `EXTEND` keyword is optional. The following is equally valid (and perhaps
more convenient):

```sql
SELECT
 <column>
FROM TABLE(
  http(
    userName => 'bob',
    password => 'secret',
    uris => 'http:foo.com/bar.csv',
    format => 'csv'
    )
  ) (x VARCHAR, y VARCHAR, z BIGINT)
```


The set of table functions and formats is preliminary in this release.

#### `HTTP`

The `HTTP` table function represents the `HttpInputSource` class in Druid which allows you to
read from an HTTP server. The function accepts the following arguments:

| Name | Description | JSON equivalent | Required |
| ---- | ----------- | --------------- | -------- |
| `userName` | Basic authentication user name | `httpAuthenticationUsername` | No |
| `password` | Basic authentication password | `httpAuthenticationPassword` | No |
| `passwordEnvVar` | Environment variable that contains the basic authentication password| `httpAuthenticationPassword` | No |
| `uris` | Comma-separated list of URIs to read. | `uris` | Yes |

#### `INLINE`

The `INLINE` table function represents the `InlineInputSource` class in Druid which provides
data directly in the table function. The function accepts the following arguments:

| Name | Description | JSON equivalent | Required |
| ---- | ----------- | --------------- | -------- |
| `data` | Text lines of inline data. Separate lines with a newline. | `data` | Yes |

#### `LOCALFILES`

The `LOCALFILES` table function represents the `LocalInputSource` class in Druid which reads
files from the file system of the node running Druid. This is most useful for single-node
installations. The function accepts the following arguments:

| Name | Description | JSON equivalent | Required |
| ---- | ----------- | --------------- | -------- |
| `baseDir` | Directory to read from. | `baseDir` | No |
| `filter` | Filter pattern to read. Example: `*.csv`. | `filter` | No |
| `files` | Comma-separated list of files to read. | `files` | No |

You must either provide the `baseDir` or the list of `files`. You can provide both, in which case
the files are assumed relative to the `baseDir`. If you provide a `filter`, you must provide the
`baseDir`.

Note that, due to [Issue #13359](https://github.com/apache/druid/issues/13359), the functionality
described above is broken. Until that issue is resolved, you must provide one or more absolute
file paths in the `files` property and the other two properties are unavailable.

#### Table Function Format

Each of the table functions above requires that you specify a format.

| Name | Description | JSON equivalent | Required |
| ---- | ----------- | --------------- | -------- |
| `format` | The input format, using the same names as for `EXTERN`. | `inputFormat.type` | Yes |

#### CSV Format

Use the `csv` format to read from CSV. This choice selects the Druid `CsvInputFormat` class.

| Name | Description | JSON equivalent | Required |
| ---- | ----------- | --------------- | -------- |
| `listDelimiter` | The delimiter to use for fields that represent a list of strings. | `listDelimiter` | No |
| `skipRows` | The number of rows to skip at the start of the file. Default is 0. | `skipHeaderRows` | No |

MSQ does not have the ability to infer schema from a CSV, file, so the `findColumnsFromHeader` property
is unavailable. Instead, Columns are given using the `EXTEND` syntax described above.

#### Delimited Text Format

Use the `tsv` format to read from an arbitrary delimited (CSV-like) file such as tab-delimited,
pipe-delimited, etc. This choice selects the Druid `DelimitedInputFormat` class.

| Name | Description | JSON equivalent | Required |
| ---- | ----------- | --------------- | -------- |
| `delimiter` | The delimiter which separates fields. | `delimiter` | Yes |
| `listDelimiter` | The delimiter to use for fields that represent a list of strings. | `listDelimiter` | No |
| `skipRows` | The number of rows to skip at the start of the file. Default is 0. | `skipHeaderRows` | No |

As noted above, MSQ cannot infer schema using headers. Use `EXTEND` instead.

#### JSON Format

Use the `json` format to read from a JSON input source. This choice selects the Druid `JsonInputFormat` class.

| Name | Description | JSON equivalent | Required |
| ---- | ----------- | --------------- | -------- |
| `keepNulls` | Whether to keep null values. Defaults to `false`. | `keepNullColumns` | No |


### `INSERT`

Use the `INSERT` statement to insert data.

Unlike standard SQL, `INSERT` loads data into the target table according to column name, not positionally. If necessary,
use `AS` in your `SELECT` column list to assign the correct names. Do not rely on their positions within the SELECT
clause.

Statement format:

```sql
INSERT INTO <table name>
< SELECT query >
PARTITIONED BY <time frame>
[ CLUSTERED BY <column list> ]
```

INSERT consists of the following parts:

1. Optional [context parameters](./reference.md#context-parameters).
2. An `INSERT INTO <dataSource>` clause at the start of your query, such as `INSERT INTO your-table`.
3. A clause for the data you want to insert, such as `SELECT ... FROM ...`. You can use [EXTERN](#extern) to reference external tables using `FROM TABLE(EXTERN(...))`.
4. A [PARTITIONED BY](#partitioned-by) clause, such as `PARTITIONED BY DAY`.
5. An optional [CLUSTERED BY](#clustered-by) clause.

For more information, see [Load data with INSERT](concepts.md#insert).

### `REPLACE`

You can use the `REPLACE` function to replace all or some of the data.

Unlike standard SQL, `REPLACE` loads data into the target table according to column name, not positionally. If necessary,
use `AS` in your `SELECT` column list to assign the correct names. Do not rely on their positions within the SELECT
clause.

#### `REPLACE` all data

Function format to replace all data:

```sql
REPLACE INTO <target table>
OVERWRITE ALL
< SELECT query >
PARTITIONED BY <time granularity>
[ CLUSTERED BY <column list> ]
```

#### `REPLACE` specific time ranges

Function format to replace specific time ranges:

```sql
REPLACE INTO <target table>
OVERWRITE WHERE __time >= TIMESTAMP '<lower bound>' AND __time < TIMESTAMP '<upper bound>'
< SELECT query >
PARTITIONED BY <time granularity>
[ CLUSTERED BY <column list> ]
```

`REPLACE` consists of the following parts:

1. Optional [context parameters](./reference.md#context-parameters).
2. A `REPLACE INTO <dataSource>` clause at the start of your query, such as `REPLACE INTO "your-table".`
3. An OVERWRITE clause after the datasource, either OVERWRITE ALL or OVERWRITE WHERE:
    - OVERWRITE ALL replaces the entire existing datasource with the results of the query.
    - OVERWRITE WHERE drops the time segments that match the condition you set. Conditions are based on the `__time`
        column and use the format `__time [< > = <= >=] TIMESTAMP`. Use them with AND, OR, and NOT between them, inclusive
        of the timestamps specified. No other expressions or functions are valid in OVERWRITE.
4. A clause for the actual data you want to use for the replacement.
5. A [PARTITIONED BY](#partitioned-by) clause, such as `PARTITIONED BY DAY`.
6. An optional [CLUSTERED BY](#clustered-by) clause.

For more information, see [Overwrite data with REPLACE](concepts.md#replace).

### `PARTITIONED BY`

The `PARTITIONED BY <time granularity>` clause is required for [INSERT](#insert) and [REPLACE](#replace). See
[Partitioning](concepts.md#partitioning) for details.

The following granularity arguments are accepted:

- Time unit: `HOUR`, `DAY`, `MONTH`, or `YEAR`. Equivalent to `FLOOR(__time TO TimeUnit)`.
- `TIME_FLOOR(__time, 'granularity_string')`, where granularity_string is one of the ISO 8601 periods listed below. The
  first argument must be `__time`.
- `FLOOR(__time TO TimeUnit)`, where `TimeUnit` is any unit supported by the [FLOOR function](../querying/sql-scalar.md#date-and-time-functions). The first argument must be `__time`.
- `ALL` or `ALL TIME`, which effectively disables time partitioning by placing all data in a single time chunk. To use
  LIMIT or OFFSET at the outer level of your INSERT or REPLACE query, you must set PARTITIONED BY to ALL or ALL TIME.

The following ISO 8601 periods are supported for `TIME_FLOOR`:

- PT1S
- PT1M
- PT5M
- PT10M
- PT15M
- PT30M
- PT1H
- PT6H
- P1D
- P1W
- P1M
- P3M
- P1Y

For more information about partitioning, see [Partitioning](concepts.md#partitioning).

### `CLUSTERED BY`

The `CLUSTERED BY <column list>` clause is optional for [INSERT](#insert) and [REPLACE](#replace). It accepts a list of
column names or expressions.

For more information about clustering, see [Clustering](concepts.md#clustering).

<a name="context"></a>

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

If you're using the web console, you can specify the context parameters through various UI options.

The following table lists the context parameters for the MSQ task engine:

| Parameter | Description | Default value |
|---|---|---|
| `maxNumTasks` | SELECT, INSERT, REPLACE<br /><br />The maximum total number of tasks to launch, including the controller task. The lowest possible value for this setting is 2: one controller and one worker. All tasks must be able to launch simultaneously. If they cannot, the query returns a `TaskStartTimeout` error code after approximately 10 minutes.<br /><br />May also be provided as `numTasks`. If both are present, `maxNumTasks` takes priority.| 2 |
| `taskAssignment` | SELECT, INSERT, REPLACE<br /><br />Determines how many tasks to use. Possible values include: <ul><li>`max`: Uses as many tasks as possible, up to `maxNumTasks`.</li><li>`auto`: When file sizes can be determined through directory listing (for example: local files, S3, GCS, HDFS) uses as few tasks as possible without exceeding 10 GiB or 10,000 files per task, unless exceeding these limits is necessary to stay within `maxNumTasks`. When file sizes cannot be determined through directory listing (for example: http), behaves the same as `max`.</li></ul> | `max` |
| `finalizeAggregations` | SELECT, INSERT, REPLACE<br /><br />Determines the type of aggregation to return. If true, Druid finalizes the results of complex aggregations that directly appear in query results. If false, Druid returns the aggregation's intermediate type rather than finalized type. This parameter is useful during ingestion, where it enables storing sketches directly in Druid tables. For more information about aggregations, see [SQL aggregation functions](../querying/sql-aggregations.md). | true |
| `rowsInMemory` | INSERT or REPLACE<br /><br />Maximum number of rows to store in memory at once before flushing to disk during the segment generation process. Ignored for non-INSERT queries. In most cases, use the default value. You may need to override the default if you run into one of the [known issues](./known-issues.md) around memory usage. | 100,000 |
| `segmentSortOrder` | INSERT or REPLACE<br /><br />Normally, Druid sorts rows in individual segments using `__time` first, followed by the [CLUSTERED BY](#clustered-by) clause. When you set `segmentSortOrder`, Druid sorts rows in segments using this column list first, followed by the CLUSTERED BY order.<br /><br />You provide the column list as comma-separated values or as a JSON array in string form. If your query includes `__time`, then this list must begin with `__time`. For example, consider an INSERT query that uses `CLUSTERED BY country` and has `segmentSortOrder` set to `__time,city`. Within each time chunk, Druid assigns rows to segments based on `country`, and then within each of those segments, Druid sorts those rows by `__time` first, then `city`, then `country`. | empty list |
| `maxParseExceptions`| SELECT, INSERT, REPLACE<br /><br />Maximum number of parse exceptions that are ignored while executing the query before it stops with `TooManyWarningsFault`. To ignore all the parse exceptions, set the value to -1.| 0 |
| `rowsPerSegment` | INSERT or REPLACE<br /><br />The number of rows per segment to target. The actual number of rows per segment may be somewhat higher or lower than this number. In most cases, use the default. For general information about sizing rows per segment, see [Segment Size Optimization](../operations/segment-optimization.md). | 3,000,000 |
| `indexSpec` | INSERT or REPLACE<br /><br />An [`indexSpec`](../ingestion/ingestion-spec.md#indexspec) to use when generating segments. May be a JSON string or object. See [Front coding](../ingestion/ingestion-spec.md#front-coding) for details on configuring an `indexSpec` with front coding. | See [`indexSpec`](../ingestion/ingestion-spec.md#indexspec). |
| `clusterStatisticsMergeMode` | Whether to use parallel or sequential mode for merging of the worker sketches. Can be `PARALLEL`, `SEQUENTIAL` or `AUTO`. See [Sketch Merging Mode](#sketch-merging-mode) for more information. | `AUTO` |

## Sketch Merging Mode
This section details the advantages and performance of various Cluster By Statistics Merge Modes.

If a query requires key statistics to generate partition boundaries, key statistics are gathered by the workers while
reading rows from the datasource. These statistics must be transferred to the controller to be merged together.
`clusterStatisticsMergeMode` configures the way in which this happens.

`PARALLEL` mode fetches the key statistics for all time chunks from all workers together and the controller then downsamples
the sketch if it does not fit in memory. This is faster than `SEQUENTIAL` mode as there is less over head in fetching sketches
for all time chunks together. This is good for small sketches which won't be downsampled even if merged together or if
accuracy in segment sizing for the ingestion is not very important.

`SEQUENTIAL` mode fetches the sketches in ascending order of time and generates the partition boundaries for one time
chunk at a time. This gives more working memory to the controller for merging sketches, which results in less
downsampling and thus, more accuracy. There is, however, a time overhead on fetching sketches in sequential order. This is
good for cases where accuracy is important.

`AUTO` mode tries to find the best approach based on number of workers and size of input rows. If there are more
than 100 workers or if the combined sketch size among all workers is more than 1GB, `SEQUENTIAL` is chosen, otherwise,
`PARALLEL` is chosen.

## Durable Storage

This section enumerates the advantages and performance implications of enabling durable storage while executing MSQ tasks.

To prevent durable storage from getting filled up with temporary files in case the tasks fail to clean them up, a periodic
cleaner can be scheduled to clean the directories corresponding to which there isn't a controller task running. It utilizes
the storage connector to work upon the durable storage. The durable storage location should only be utilized to store the output
for cluster's MSQ tasks. If the location contains other files or directories, then they will get cleaned up as well.
Following table lists the properties that can be set to control the behavior of the durable storage of the cluster.

|Parameter          |Default                                 | Description          |
|-------------------|----------------------------------------|----------------------|
|`druid.msq.intermediate.storage.enable` | true | Whether to enable durable storage for the cluster |
|`druid.msq.intermediate.storage.cleaner.enabled`| false | Whether durable storage cleaner should be enabled for the cluster. This should be set on the overlord|
|`druid.msq.intermediate.storage.cleaner.delaySeconds`| 86400 | The delay (in seconds) after the last run post which the durable storage cleaner would clean the outputs. This should be set on the overlord |

## Limits

Knowing the limits for the MSQ task engine can help you troubleshoot any [errors](#error-codes) that you encounter. Many of the errors occur as a result of reaching a limit.

The following table lists query limits:

| Limit | Value | Error if exceeded |
|---|---|---|
| Size of an individual row written to a frame. Row size when written to a frame may differ from the original row size. | 1 MB | [`RowTooLarge`](#error_RowTooLarge) |
| Number of segment-granular time chunks encountered during ingestion. | 5,000 | [`TooManyBuckets`](#error_TooManyBuckets) |
| Number of input files/segments per worker. | 10,000 | [`TooManyInputFiles`](#error_TooManyInputFiles) |
| Number of output partitions for any one stage. Number of segments generated during ingestion. |25,000 | [`TooManyPartitions`](#error_TooManyPartitions) |
| Number of output columns for any one stage. | 2,000 | [`TooManyColumns`](#error_TooManyColumns) |
| Number of cluster by columns that can appear in a stage | 1,500 | [`TooManyClusteredByColumns`](#error_TooManyClusteredByColumns) |
| Number of workers for any one stage. | Hard limit is 1,000. Memory-dependent soft limit may be lower. | [`TooManyWorkers`](#error_TooManyWorkers) |
| Maximum memory occupied by broadcasted tables. | 30% of each [processor memory bundle](concepts.md#memory-usage). | [`BroadcastTablesTooLarge`](#error_BroadcastTablesTooLarge) |

<a name="errors"></a>

## Error codes

The following table describes error codes you may encounter in the `multiStageQuery.payload.status.errorReport.error.errorCode` field:

| Code | Meaning | Additional fields |
|---|---|---|
| <a name="error_BroadcastTablesTooLarge">`BroadcastTablesTooLarge`</a> | The size of the broadcast tables used in the right hand side of the join exceeded the memory reserved for them in a worker task.<br /><br />Try increasing the peon memory or reducing the size of the broadcast tables. | `maxBroadcastTablesSize`: Memory reserved for the broadcast tables, measured in bytes. |
| <a name="error_Canceled">`Canceled`</a> | The query was canceled. Common reasons for cancellation:<br /><br /><ul><li>User-initiated shutdown of the controller task via the `/druid/indexer/v1/task/{taskId}/shutdown` API.</li><li>Restart or failure of the server process that was running the controller task.</li></ul>| |
| <a name="error_CannotParseExternalData">`CannotParseExternalData`</a> | A worker task could not parse data from an external datasource. | `errorMessage`: More details on why parsing failed. |
| <a name="error_ColumnNameRestricted">`ColumnNameRestricted`</a> | The query uses a restricted column name. | `columnName`: The restricted column name. |
| <a name="error_ColumnTypeNotSupported">`ColumnTypeNotSupported`</a> | The column type is not supported. This can be because:<br /> <br /><ul><li>Support for writing or reading from a particular column type is not supported.</li><li>The query attempted to use a column type that is not supported by the frame format. This occurs with ARRAY types, which are not yet implemented for frames.</li></ul> | `columnName`: The column name with an unsupported type.<br /> <br />`columnType`: The unknown column type. |
| <a name="error_InsertCannotAllocateSegment">`InsertCannotAllocateSegment`</a> | The controller task could not allocate a new segment ID due to conflict with existing segments or pending segments. Common reasons for such conflicts:<br /> <br /><ul><li>Attempting to mix different granularities in the same intervals of the same datasource.</li><li>Prior ingestions that used non-extendable shard specs.</li></ul>| `dataSource`<br /> <br />`interval`: The interval for the attempted new segment allocation. |
| <a name="error_InsertCannotBeEmpty">`InsertCannotBeEmpty`</a> | An INSERT or REPLACE query did not generate any output rows in a situation where output rows are required for success. This can happen for INSERT or REPLACE queries with `PARTITIONED BY` set to something other than `ALL` or `ALL TIME`. | `dataSource` |
| <a name="error_InsertCannotOrderByDescending">`InsertCannotOrderByDescending`</a> | An INSERT query contained a `CLUSTERED BY` expression in descending order. Druid's segment generation code only supports ascending order. | `columnName` |
| <a name="error_InsertCannotReplaceExistingSegment">`InsertCannotReplaceExistingSegment`</a> | A REPLACE query cannot proceed because an existing segment partially overlaps those bounds, and the portion within the bounds is not fully overshadowed by query results. <br /> <br />There are two ways to address this without modifying your query:<ul><li>Shrink the OVERLAP filter to match the query results.</li><li>Expand the OVERLAP filter to fully contain the existing segment.</li></ul>| `segmentId`: The existing segment <br />
| <a name="error_InsertLockPreempted">`InsertLockPreempted`</a> | An INSERT or REPLACE query was canceled by a higher-priority ingestion job, such as a real-time ingestion task. | |
| <a name="error_InsertTimeNull">`InsertTimeNull`</a> | An INSERT or REPLACE query encountered a null timestamp in the `__time` field.<br /><br />This can happen due to using an expression like `TIME_PARSE(timestamp) AS __time` with a timestamp that cannot be parsed. (TIME_PARSE returns null when it cannot parse a timestamp.) In this case, try parsing your timestamps using a different function or pattern.<br /><br />If your timestamps may genuinely be null, consider using COALESCE to provide a default value. One option is CURRENT_TIMESTAMP, which represents the start time of the job. |
| <a name="error_InsertTimeOutOfBounds">`InsertTimeOutOfBounds`</a> | A REPLACE query generated a timestamp outside the bounds of the TIMESTAMP parameter for your OVERWRITE WHERE clause.<br /> <br />To avoid this error, verify that the you specified is valid. | `interval`: time chunk interval corresponding to the out-of-bounds timestamp |
| <a name="error_InvalidNullByte">`InvalidNullByte`</a> | A string column included a null byte. Null bytes in strings are not permitted. | `column`: The column that included the null byte |
| <a name="error_QueryNotSupported">`QueryNotSupported`</a> | QueryKit could not translate the provided native query to a multi-stage query.<br /> <br />This can happen if the query uses features that aren't supported, like GROUPING SETS. | |
| <a name="error_RowTooLarge">`RowTooLarge`</a> | The query tried to process a row that was too large to write to a single frame. See the [Limits](#limits) table for specific limits on frame size. Note that the effective maximum row size is smaller than the maximum frame size due to alignment considerations during frame writing. | `maxFrameSize`: The limit on the frame size. |
| <a name="error_TaskStartTimeout">`TaskStartTimeout`</a> | Unable to launch all the worker tasks in time. <br /> <br />There might be insufficient available slots to start all the worker tasks simultaneously.<br /> <br /> Try splitting up the query into smaller chunks with lesser `maxNumTasks` number. Another option is to increase capacity. | `numTasks`: The number of tasks attempted to launch. |
| <a name="error_TooManyBuckets">`TooManyBuckets`</a> | Exceeded the maximum number of partition buckets for a stage (5,000 partition buckets).<br />< br />Partition buckets are created for each [`PARTITIONED BY`](#partitioned-by) time chunk for INSERT and REPLACE queries. The most common reason for this error is that your `PARTITIONED BY` is too narrow relative to your data. | `maxBuckets`: The limit on partition buckets. |
| <a name="error_TooManyInputFiles">`TooManyInputFiles`</a> | Exceeded the maximum number of input files or segments per worker (10,000 files or segments).<br /><br />If you encounter this limit, consider adding more workers, or breaking up your query into smaller queries that process fewer files or segments per query. | `numInputFiles`: The total number of input files/segments for the stage.<br /><br />`maxInputFiles`: The maximum number of input files/segments per worker per stage.<br /><br />`minNumWorker`: The minimum number of workers required for a successful run. |
| <a name="error_TooManyPartitions">`TooManyPartitions`</a> | Exceeded the maximum number of partitions for a stage (25,000 partitions).<br /><br />This can occur with INSERT or REPLACE statements that generate large numbers of segments, since each segment is associated with a partition. If you encounter this limit, consider breaking up your INSERT or REPLACE statement into smaller statements that process less data per statement. | `maxPartitions`: The limit on partitions which was exceeded |
| <a name="error_TooManyClusteredByColumns">`TooManyClusteredByColumns`</a>  | Exceeded the maximum number of clustering columns for a stage (1,500 columns).<br /><br />This can occur with `CLUSTERED BY`, `ORDER BY`, or `GROUP BY` with a large number of columns. | `numColumns`: The number of columns requested.<br /><br />`maxColumns`: The limit on columns which was exceeded.`stage`: The stage number exceeding the limit<br /><br /> |
| <a name="error_TooManyColumns">`TooManyColumns`</a> | Exceeded the maximum number of columns for a stage (2,000 columns). | `numColumns`: The number of columns requested.<br /><br />`maxColumns`: The limit on columns which was exceeded. |
| <a name="error_TooManyWarnings">`TooManyWarnings`</a> | Exceeded the maximum allowed number of warnings of a particular type. | `rootErrorCode`: The error code corresponding to the exception that exceeded the required limit. <br /><br />`maxWarnings`: Maximum number of warnings that are allowed for the corresponding `rootErrorCode`. |
| <a name="error_TooManyWorkers">`TooManyWorkers`</a> | Exceeded the maximum number of simultaneously-running workers. See the [Limits](#limits) table for more details. | `workers`: The number of simultaneously running workers that exceeded a hard or soft limit. This may be larger than the number of workers in any one stage if multiple stages are running simultaneously. <br /><br />`maxWorkers`: The hard or soft limit on workers that was exceeded. If this is lower than the hard limit (1,000 workers), then you can increase the limit by adding more memory to each task. |
| <a name="error_NotEnoughMemory">`NotEnoughMemory`</a> | Insufficient memory to launch a stage. | `serverMemory`: The amount of memory available to a single process.<br /><br />`serverWorkers`: The number of workers running in a single process.<br /><br />`serverThreads`: The number of threads in a single process. |
| <a name="error_WorkerFailed">`WorkerFailed`</a> | A worker task failed unexpectedly. | `errorMsg`<br /><br />`workerTaskId`: The ID of the worker task. |
| <a name="error_WorkerRpcFailed">`WorkerRpcFailed`</a> | A remote procedure call to a worker task failed and could not recover. | `workerTaskId`: the id of the worker task |
| <a name="error_UnknownError">`UnknownError`</a> | All other errors. | `message` |
