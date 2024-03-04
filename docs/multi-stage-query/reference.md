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

:::info
 This page describes SQL-based batch ingestion using the [`druid-multi-stage-query`](../multi-stage-query/index.md)
 extension, new in Druid 24.0. Refer to the [ingestion methods](../ingestion/index.md#batch) table to determine which
 ingestion method is right for you.
:::

## SQL reference

This topic is a reference guide for the multi-stage query architecture in Apache Druid. For examples of real-world
usage, refer to the [Examples](examples.md) page.

`INSERT` and `REPLACE` load data into a Druid datasource from either an external input source, or from another
datasource. When loading from an external datasource, you typically must provide the kind of input source,
the data format, and the schema (signature) of the input file. Druid provides *table functions* to allow you to
specify the external file. There are two kinds. `EXTERN` works with the JSON-serialized specs for the three
items, using the same JSON you would use in native ingest. A set of other, input-source-specific functions
use SQL syntax to specify the format and the input schema. There is one function for each input source. The
input-source-specific functions allow you to use SQL query parameters to specify the set of files (or URIs),
making it easy to reuse the same SQL statement for each ingest: just specify the set of files to use each time.

### `EXTERN` Function

Use the `EXTERN` function to read external data or write to an external location.

#### `EXTERN` as an input source

The function has two variations.
Function variation 1, with the input schema expressed as JSON:

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

1. Any [Druid input source](../ingestion/input-sources.md) as a JSON-encoded string.
2. Any [Druid input format](../ingestion/data-formats.md) as a JSON-encoded string.
3. A row signature, as a JSON-encoded array of column descriptors. Each column descriptor must have a
   `name` and a `type`. The type can be `string`, `long`, `double`, or `float`. This row signature is
   used to map the external data into the SQL layer.

Variation 2, with the input schema expressed in SQL using an `EXTEND` clause. (See the next
section for more detail on `EXTEND`). This format also uses named arguments to make the
SQL a bit easier to read:

```sql
SELECT
 <column>
FROM TABLE(
  EXTERN(
    inputSource => '<Druid input source>',
    inputFormat => '<Druid input format>'
  )) (<columns>)

```

The input source and format are as above. The columns are expressed as in a SQL `CREATE TABLE`.
Example: `(timestamp VARCHAR, metricType VARCHAR, value BIGINT)`. The optional `EXTEND` keyword
can precede the column list: `EXTEND (timestamp VARCHAR...)`.

For more information, see [Read external data with EXTERN](concepts.md#read-external-data-with-extern).

#### `EXTERN` to export to a destination

`EXTERN` can be used to specify a destination where you want to export data to.
This variation of EXTERN requires one argument, the details of the destination as specified below.
This variation additionally requires an `AS` clause to specify the format of the exported rows.

Keep the following in mind when using EXTERN to export rows:
- Only INSERT statements are supported.
- Only `CSV` format is supported as an export format.
- Partitioning (`PARTITIONED BY`) and clustering (`CLUSTERED BY`) aren't supported with export statements.
- You can export to Amazon S3 or local storage.
- The destination provided should contain no other files or directories.

When you export data, use the `rowsPerPage` context parameter to control how many rows get exported. The default is 100,000.

```sql
INSERT INTO
  EXTERN(<destination function>)
AS CSV
SELECT
  <column>
FROM <table>
```

##### S3

Export results to S3 by passing the function `S3()` as an argument to the `EXTERN` function. Note that this requires the `druid-s3-extensions`.
The `S3()` function is a Druid function that configures the connection. Arguments for `S3()` should be passed as named parameters with the value in single quotes like the following example:

```sql
INSERT INTO
  EXTERN(
    S3(bucket => 'your_bucket', prefix => 'prefix/to/files')
  )
AS CSV
SELECT
  <column>
FROM <table>
```

Supported arguments for the function:

| Parameter   | Required | Description                                                                                                                                                                                                                                                                    | Default |
|-------------|----------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|
| `bucket`    | Yes      | The S3 bucket to which the files are exported to. The bucket and prefix combination should be whitelisted in `druid.export.storage.s3.allowedExportPaths`.                                                                                                                     | n/a     |
| `prefix`    | Yes      | Path where the exported files would be created. The export query expects the destination to be empty. If the location includes other files, then the query will fail. The bucket and prefix combination should be whitelisted in `druid.export.storage.s3.allowedExportPaths`. | n/a     |

The following runtime parameters must be configured to export into an S3 destination:

| Runtime Parameter                            | Required | Description                                                                                                                                                                                                                          | Default |
|----------------------------------------------|----------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----|
| `druid.export.storage.s3.tempLocalDir`       | Yes      | Directory used on the local storage of the worker to store temporary files required while uploading the data.                                                                                                                        | n/a |
| `druid.export.storage.s3.allowedExportPaths` | Yes      | An array of S3 prefixes that are whitelisted as export destinations. Export queries fail if the export destination does not match any of the configured prefixes. Example: `[\"s3://bucket1/export/\", \"s3://bucket2/export/\"]`    | n/a |
| `druid.export.storage.s3.maxRetry`           | No       | Defines the max number times to attempt S3 API calls to avoid failures due to transient errors.                                                                                                                                      | 10  |
| `druid.export.storage.s3.chunkSize`          | No       | Defines the size of each chunk to temporarily store in `tempDir`. The chunk size must be between 5 MiB and 5 GiB. A large chunk size reduces the API calls to S3, however it requires more disk space to store the temporary chunks. | 100MiB |

##### LOCAL

You can export to the local storage, which exports the results to the filesystem of the MSQ worker.
This is useful in a single node setup or for testing but is not suitable for production use cases.

Export results to local storage by passing the function `LOCAL()` as an argument for the `EXTERN FUNCTION`.
To use local storage as an export destination, the runtime property `druid.export.storage.baseDir` must be configured on the Indexer/Middle Manager.
This value must be set to an absolute path on the local machine. Exporting data will be allowed to paths which match the prefix set by this value.
Arguments to `LOCAL()` should be passed as named parameters with the value in single quotes in the following example:

```sql
INSERT INTO
  EXTERN(
    local(exportPath => 'exportLocation/query1')
  )
AS CSV
SELECT
  <column>
FROM <table>
```

Supported arguments to the function:

| Parameter   | Required | Description                                                                                                                                                                                                                                              | Default |
|-------------|--------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------| --|
| `exportPath`  | Yes | Absolute path to a subdirectory of `druid.export.storage.baseDir` used as the destination to export the results to. The export query expects the destination to be empty. If the location includes other files or directories, then the query will fail. | n/a |

For more information, see [Read external data with EXTERN](concepts.md#write-to-an-external-destination-with-extern).

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
3. A clause for the data you want to insert, such as `SELECT ... FROM ...`. You can use [`EXTERN`](#extern-function)
   to reference external tables using `FROM TABLE(EXTERN(...))`.
4. A [PARTITIONED BY](#partitioned-by) clause, such as `PARTITIONED BY DAY`.
5. An optional [CLUSTERED BY](#clustered-by) clause.

For more information, see [Load data with INSERT](concepts.md#load-data-with-insert).

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
[Partitioning](concepts.md#partitioning-by-time) for details.

The following granularity arguments are accepted:

- Time unit keywords: `HOUR`, `DAY`, `MONTH`, or `YEAR`. Equivalent to `FLOOR(__time TO TimeUnit)`.
- Time units as ISO 8601 period strings: `'PT1H'`, `'P1D'`, etc. (Druid 26.0 and later.)
- `TIME_FLOOR(__time, 'granularity_string')`, where granularity_string is one of the ISO 8601 periods listed below. The
  first argument must be `__time`.
- `FLOOR(__time TO TimeUnit)`, where `TimeUnit` is any unit supported by the [FLOOR function](../querying/sql-scalar.md#date-and-time-functions). The first argument must be `__time`.
- `ALL` or `ALL TIME`, which effectively disables time partitioning by placing all data in a single time chunk. To use
  LIMIT or OFFSET at the outer level of your `INSERT` or `REPLACE` query, you must set `PARTITIONED BY` to `ALL` or `ALL TIME`.

Earlier versions required the `TIME_FLOOR` notation to specify a granularity other than the keywords.
In the current version, the string constant provides a simpler equivalent solution.

The following ISO 8601 periods are supported for `TIME_FLOOR` and the string constant:

- PT1S
- PT1M
- PT5M
- PT10M
- PT15M
- PT30M
- PT1H
- PT6H
- P1D
- P1W*
- P1M
- P3M
- P1Y

The string constant can also include any of the keywords mentioned above:

- `HOUR` - Same as `'PT1H'`
- `DAY` - Same as `'P1D'`
- `MONTH` - Same as `'P1M'`
- `YEAR` - Same as `'P1Y'`
- `ALL TIME`
- `ALL` - Alias for `ALL TIME`

Examples:

```SQL
-- Keyword
PARTITIONED BY HOUR

-- String literal
PARTITIONED BY 'HOUR'

-- ISO 8601 period
PARTITIONED BY 'PT1H'

-- TIME_FLOOR function
PARTITIONED BY TIME_FLOOR(__time, 'PT1H')
```

For more information about partitioning, see [Partitioning](concepts.md#partitioning-by-time). <br /><br />
*Avoid  partitioning by week, `P1W`, because weeks don't align neatly with months and years, making it difficult to partition by coarser granularities later.

### `CLUSTERED BY`

The `CLUSTERED BY <column list>` clause is optional for [INSERT](#insert) and [REPLACE](#replace). It accepts a list of
column names or expressions. Druid's segment generation only supports ascending order, so an `INSERT` or `REPLACE` query with
`CLUSTERED BY` columns in `DESC` ordering is not allowed.

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
| `maxNumTasks` | SELECT, INSERT, REPLACE<br /><br />The maximum total number of tasks to launch, including the controller task. The lowest possible value for this setting is 2: one controller and one worker. All tasks must be able to launch simultaneously. If they cannot, the query returns a `TaskStartTimeout` error code after approximately 10 minutes.<br /><br />May also be provided as `numTasks`. If both are present, `maxNumTasks` takes priority. | 2 |
| `taskAssignment` | SELECT, INSERT, REPLACE<br /><br />Determines how many tasks to use. Possible values include: <ul><li>`max`: Uses as many tasks as possible, up to `maxNumTasks`.</li><li>`auto`: When file sizes can be determined through directory listing (for example: local files, S3, GCS, HDFS) uses as few tasks as possible without exceeding 512 MiB or 10,000 files per task, unless exceeding these limits is necessary to stay within `maxNumTasks`. When calculating the size of files, the weighted size is used, which considers the file format and compression format used if any. When file sizes cannot be determined through directory listing (for example: http), behaves the same as `max`.</li></ul> | `max` |
| `finalizeAggregations` | SELECT, INSERT, REPLACE<br /><br />Determines the type of aggregation to return. If true, Druid finalizes the results of complex aggregations that directly appear in query results. If false, Druid returns the aggregation's intermediate type rather than finalized type. This parameter is useful during ingestion, where it enables storing sketches directly in Druid tables. For more information about aggregations, see [SQL aggregation functions](../querying/sql-aggregations.md). | `true` |
| `arrayIngestMode` | INSERT, REPLACE<br /><br /> Controls how ARRAY type values are stored in Druid segments. When set to `array` (recommended for SQL compliance), Druid will store all ARRAY typed values in [ARRAY typed columns](../querying/arrays.md), and supports storing both VARCHAR and numeric typed arrays. When set to `mvd` (the default, for backwards compatibility), Druid only supports VARCHAR typed arrays, and will store them as [multi-value string columns](../querying/multi-value-dimensions.md). When set to `none`, Druid will throw an exception when trying to store any type of arrays. `none` is most useful when set in the system default query context with (`druid.query.default.context.arrayIngestMode=none`) to be used to help migrate operators from `mvd` mode to `array` mode and force query writers to make an explicit choice between ARRAY and multi-value VARCHAR typed columns. | `mvd` (for backwards compatibility, recommended to use `array` for SQL compliance)|
| `sqlJoinAlgorithm` | SELECT, INSERT, REPLACE<br /><br />Algorithm to use for JOIN. Use `broadcast` (the default) for broadcast hash join or `sortMerge` for sort-merge join. Affects all JOIN operations in the query. This is a hint to the MSQ engine and the actual joins in the query may proceed in a different way than specified. See [Joins](#joins) for more details. | `broadcast` |
| `rowsInMemory` | INSERT or REPLACE<br /><br />Maximum number of rows to store in memory at once before flushing to disk during the segment generation process. Ignored for non-INSERT queries. In most cases, use the default value. You may need to override the default if you run into one of the [known issues](./known-issues.md) around memory usage. | 100,000 |
| `segmentSortOrder` | INSERT or REPLACE<br /><br />Normally, Druid sorts rows in individual segments using `__time` first, followed by the [CLUSTERED BY](#clustered-by) clause. When you set `segmentSortOrder`, Druid sorts rows in segments using this column list first, followed by the CLUSTERED BY order.<br /><br />You provide the column list as comma-separated values or as a JSON array in string form. If your query includes `__time`, then this list must begin with `__time`. For example, consider an INSERT query that uses `CLUSTERED BY country` and has `segmentSortOrder` set to `__time,city`. Within each time chunk, Druid assigns rows to segments based on `country`, and then within each of those segments, Druid sorts those rows by `__time` first, then `city`, then `country`. | empty list |
| `maxParseExceptions`| SELECT, INSERT, REPLACE<br /><br />Maximum number of parse exceptions that are ignored while executing the query before it stops with `TooManyWarningsFault`. To ignore all the parse exceptions, set the value to -1. | 0 |
| `rowsPerSegment` | INSERT or REPLACE<br /><br />The number of rows per segment to target. The actual number of rows per segment may be somewhat higher or lower than this number. In most cases, use the default. For general information about sizing rows per segment, see [Segment Size Optimization](../operations/segment-optimization.md). | 3,000,000 |
| `indexSpec` | INSERT or REPLACE<br /><br />An [`indexSpec`](../ingestion/ingestion-spec.md#indexspec) to use when generating segments. May be a JSON string or object. See [Front coding](../ingestion/ingestion-spec.md#front-coding) for details on configuring an `indexSpec` with front coding. | See [`indexSpec`](../ingestion/ingestion-spec.md#indexspec). |
| `durableShuffleStorage` | SELECT, INSERT, REPLACE <br /><br />Whether to use durable storage for shuffle mesh. To use this feature, configure the durable storage at the server level using `druid.msq.intermediate.storage.enable=true`). If these properties are not configured, any query with the context variable `durableShuffleStorage=true` fails with a configuration error. <br /><br /> | `false` |
| `faultTolerance` | SELECT, INSERT, REPLACE<br /><br /> Whether to turn on fault tolerance mode or not. Failed workers are retried based on [Limits](#limits). Cannot be used when `durableShuffleStorage` is explicitly set to false. | `false` |
| `selectDestination` | SELECT<br /><br /> Controls where the final result of the select query is written. <br />Use `taskReport`(the default) to write select results to the task report. <b> This is not scalable since task reports size explodes for large results </b> <br/>Use `durableStorage` to write results to durable storage location. <b>For large results sets, its recommended to use `durableStorage` </b>. To configure durable storage see [`this`](#durable-storage) section. | `taskReport` |
| `waitUntilSegmentsLoad` | INSERT, REPLACE<br /><br /> If set, the ingest query waits for the generated segment to be loaded before exiting, else the ingest query exits without waiting. The task and live reports contain the information about the status of loading segments if this flag is set. This will ensure that any future queries made after the ingestion exits will include results from the ingestion. The drawback is that the controller task will stall till the segments are loaded. | `false` |
| `includeSegmentSource` | SELECT, INSERT, REPLACE<br /><br /> Controls the sources, which will be queried for results in addition to the segments present on deep storage. Can be `NONE` or `REALTIME`. If this value is `NONE`, only non-realtime (published and used) segments will be downloaded from deep storage. If this value is `REALTIME`, results will also be included from realtime tasks. | `NONE` |
| `rowsPerPage` | SELECT<br /><br />The number of rows per page to target. The actual number of rows per page may be somewhat higher or lower than this number. In most cases, use the default.<br /> This property comes into effect only when `selectDestination` is set to `durableStorage` | 100000 |
| `failOnEmptyInsert` | INSERT or REPLACE<br /><br /> When set to false (the default), an INSERT query generating no output rows will be no-op, and a REPLACE query generating no output rows will delete all data that matches the OVERWRITE clause.  When set to true, an ingest query generating no output rows will throw an `InsertCannotBeEmpty` fault. | `false` |

## Joins

Joins in multi-stage queries use one of two algorithms based on what you set the [context parameter](#context-parameters) `sqlJoinAlgorithm` to: 

- [`broadcast`](#broadcast) (default) 
- [`sortMerge`](#sort-merge).

If you omit this context parameter, the MSQ task engine uses broadcast since it's the default join algorithm. The context parameter applies to the entire SQL statement, so you can't mix different
join algorithms in the same query.

`sqlJoinAlgorithm` is a hint to the planner to execute the join in the specified manner. The planner can decide to ignore
the hint if it deduces that the specified algorithm can be detrimental to the performance of the join beforehand. This intelligence
is very limited as of now, and the `sqlJoinAlgorithm` set would be respected in most cases, therefore the user should set it
appropriately. See the advantages and the drawbacks for the [broadcast](#broadcast) and the [sort-merge](#sort-merge) join to 
determine which join to use beforehand.

### Broadcast

The default join algorithm for multi-stage queries is a broadcast hash join, which is similar to how
[joins are executed with native queries](../querying/query-execution.md#join). 

To use broadcast joins, either omit the  `sqlJoinAlgorithm` or set it to `broadcast`.

For a broadcast join, any adjacent joins are flattened
into a structure with a "base" input (the bottom-leftmost one) and other leaf inputs (the rest). Next, any subqueries
that are inputs the join (either base or other leafs) are planned into independent stages. Then, the non-base leaf
inputs are all connected as broadcast inputs to the "base" stage.

Together, all of these non-base leaf inputs must not exceed the [limit on broadcast table footprint](#limits). There
is no limit on the size of the base (leftmost) input.

Only LEFT JOIN, INNER JOIN, and CROSS JOIN are supported with `broadcast`.

Join conditions, if present, must be equalities. It is not necessary to include a join condition; for example,
`CROSS JOIN` and comma join do not require join conditions.

The following example has a single join chain where `orders` is the base input while `products` and
`customers` are non-base leaf inputs. The broadcast inputs (`products` and `customers`) must fall under the limit on broadcast table footprint, but the base `orders` input
can be unlimited in size.

The query reads `products` and `customers` and then broadcasts both to
the stage that reads `orders`. That stage loads the broadcast inputs (`products` and `customers`) in memory and walks
through `orders` row by row. The results are aggregated and written to the table `orders_enriched`. 

```sql
REPLACE INTO orders_enriched
OVERWRITE ALL
SELECT
  orders.__time,
  products.name AS product_name,
  customers.name AS customer_name,
  SUM(orders.amount) AS amount
FROM orders
LEFT JOIN products ON orders.product_id = products.id
LEFT JOIN customers ON orders.customer_id = customers.id
GROUP BY 1, 2
PARTITIONED BY HOUR
CLUSTERED BY product_name
```

### Sort-merge

You can use the sort-merge join algorithm to make queries more scalable at the cost of performance. If your goal is performance, consider [broadcast joins](#broadcast).  There are various scenarios where broadcast join would return a [`BroadcastTablesTooLarge`](#error-codes) error, but a sort-merge join would succeed.

To use the sort-merge join algorithm, set the context parameter `sqlJoinAlgorithm` to `sortMerge`.

In a sort-merge join, each pairwise join is planned into its own stage with two inputs. The two inputs are partitioned and sorted using a hash partitioning on the same key. 

When using the sort-merge algorithm, keep the following in mind:

- There is no limit on the overall size of either input, so sort-merge is a good choice for performing a join of two large inputs or for performing a self-join of a large input with itself.

- There is a limit on the amount of data associated with each individual key. If _both_ sides of the join exceed this limit, the query returns a [`TooManyRowsWithSameKey`](#error-codes) error. If only one side exceeds the limit, the query does not return this error.

- Join conditions are optional but must be equalities if they are present. For example, `CROSS JOIN` and comma join do not require join conditions.

- All join types are supported with `sortMerge`: LEFT, RIGHT, INNER, FULL, and CROSS.

The following example  runs using a single sort-merge join stage that receives `eventstream`
(partitioned on `user_id`) and `users` (partitioned on `id`) as inputs. There is no limit on the size of either input.

```sql
REPLACE INTO eventstream_enriched
OVERWRITE ALL
SELECT
  eventstream.__time,
  eventstream.user_id,
  eventstream.event_type,
  eventstream.event_details,
  users.signup_date AS user_signup_date
FROM eventstream
LEFT JOIN users ON eventstream.user_id = users.id
PARTITIONED BY HOUR
CLUSTERED BY user
```

The context parameter that sets `sqlJoinAlgorithm` to `sortMerge` is not shown in the above example.

## Durable storage

SQL-based ingestion supports using durable storage to store intermediate files temporarily. Enabling it can improve reliability. For more information, see [Durable storage](../operations/durable-storage.md).

### Durable storage configurations

Durable storage is supported on Amazon S3 storage, Microsoft's Azure Blob Storage and Google Cloud Storage. 
There are common configurations that control the behavior regardless of which storage service you use. Apart from these common configurations, there are a few properties specific to S3 and to Azure.

Common properties to configure the behavior of durable storage

|Parameter          | Required | Description          | Default | 
|--|--|--|
|`druid.msq.intermediate.storage.enable`  | Yes |  Whether to enable durable storage for the cluster. Set it to true to enable durable storage. For more information about enabling durable storage, see [Durable storage](../operations/durable-storage.md). | false | 
|`druid.msq.intermediate.storage.type` |  Yes | The type of storage to use. Set it to `s3` for S3, `azure` for Azure and `google` for Google | n/a |
|`druid.msq.intermediate.storage.tempDir`| Yes |  Directory path on the local disk to store temporary files required while uploading and downloading the data  | n/a |
|`druid.msq.intermediate.storage.maxRetry` |  No | Defines the max number times to attempt S3 API calls to avoid failures due to transient errors. | 10 |
|`druid.msq.intermediate.storage.chunkSize` | No | Defines the size of each chunk to temporarily store in `druid.msq.intermediate.storage.tempDir`. The chunk size must be between 5 MiB and 5 GiB. A large chunk size reduces the API calls made to the durable storage, however it requires more disk space to store the temporary chunks. Druid uses a default of 100MiB if the value is not provided.| 100MiB | 

To use S3 or Google for durable storage, you also need to configure the following properties:

|Parameter          | Required | Description  | Default |
|-------------------|----------------------------------------|----------------------| --|
|`druid.msq.intermediate.storage.bucket` | Yes | The S3 or Google bucket where the files are uploaded to and download from | n/a |
|`druid.msq.intermediate.storage.prefix` | Yes | Path prepended to all the paths uploaded to the bucket to namespace the connector's files. Provide a unique value for the prefix and do not share the same prefix between different clusters. If the location includes other files or directories, then they might get cleaned up as well.  | n/a | 

To use Azure for durable storage, you also need to configure the following properties:

|Parameter          | Required  | Description          | Default |
|-------------------|----------------------------------------|----------------------| - |
|`druid.msq.intermediate.storage.container` | Yes | The Azure container where the files are uploaded to and downloaded from.  | n/a |
|`druid.msq.intermediate.storage.prefix` | Yes | Path prepended to all the paths uploaded to the container to namespace the connector's files. Provide a unique value for the prefix and do not share the same prefix between different clusters. If the location includes other files or directories, then they might get cleaned up as well. | n/a |

### Durable storage cleaner configurations

Durable storage creates files on the remote storage, and these files get cleaned up once a job no longer requires those files. However, due to failures causing abrupt exits of tasks, these files might not get cleaned up.
You can configure the Overlord to periodically clean up these intermediate files after a task completes and the files are no longer need. The files that get cleaned up are determined by the storage prefix you configure. Any files that match the path for the storage prefix may get cleaned up, not just intermediate files that are no longer needed.

Use the following configurations to control the cleaner:

|Parameter  | Required  | Description | Default | 
|--|--|--|--|
|`druid.msq.intermediate.storage.cleaner.enabled`|  No | Whether durable storage cleaner should be enabled for the cluster.  | false |
|`druid.msq.intermediate.storage.cleaner.delaySeconds`| No | The delay (in seconds) after the latest run post which the durable storage cleaner cleans the up files.  | 86400 | 


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
| Maximum memory occupied by buffered data during sort-merge join. Only relevant when `sqlJoinAlgorithm` is `sortMerge`. | 10 MB | `TooManyRowsWithSameKey` |
| Maximum relaunch attempts per worker. Initial run is not a relaunch. The worker will be spawned 1 + `workerRelaunchLimit` times before the job fails. | 2 | `TooManyAttemptsForWorker` |
| Maximum relaunch attempts for a job across all workers. | 100 | `TooManyAttemptsForJob` |
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
| <a name="error_InsertCannotAllocateSegment">`InsertCannotAllocateSegment`</a> | The controller task could not allocate a new segment ID due to conflict with existing segments or pending segments. Common reasons for such conflicts:<br /> <br /><ul><li>Attempting to mix different granularities in the same intervals of the same datasource.</li><li>Prior ingestions that used non-extendable shard specs.</li></ul> <br /> <br /> Use REPLACE to overwrite the existing data or if the error contains the `allocatedInterval` then alternatively rerun the INSERT job with the mentioned granularity to append to existing data. Note that it might not always be possible to append to the existing data using INSERT and can only be done if `allocatedInterval` is present. | `dataSource`<br /> <br />`interval`: The interval for the attempted new segment allocation. <br /> <br /> `allocatedInterval`: The incorrect interval allocated by the overlord. It can be null |
| <a name="error_InsertCannotBeEmpty">`InsertCannotBeEmpty`</a> | An INSERT or REPLACE query did not generate any output rows when `failOnEmptyInsert` query context is set to true. `failOnEmptyInsert` defaults to false, so an INSERT query generating no output rows will be no-op, and a REPLACE query generating no output rows will delete all data that matches the OVERWRITE clause. | `dataSource` |
| <a name="error_InsertLockPreempted">`InsertLockPreempted`</a> | An INSERT or REPLACE query was canceled by a higher-priority ingestion job, such as a real-time ingestion task. | |
| <a name="error_InsertTimeNull">`InsertTimeNull`</a> | An INSERT or REPLACE query encountered a null timestamp in the `__time` field.<br /><br />This can happen due to using an expression like `TIME_PARSE(timestamp) AS __time` with a timestamp that cannot be parsed. ([`TIME_PARSE`](../querying/sql-scalar.md#date-and-time-functions) returns null when it cannot parse a timestamp.) In this case, try parsing your timestamps using a different function or pattern. Or, if your timestamps may genuinely be null, consider using [`COALESCE`](../querying/sql-scalar.md#other-scalar-functions) to provide a default value. One option is [`CURRENT_TIMESTAMP`](../querying/sql-scalar.md#date-and-time-functions), which represents the start time of the job.|
| <a name="error_InsertTimeOutOfBounds">`InsertTimeOutOfBounds`</a> | A REPLACE query generated a timestamp outside the bounds of the TIMESTAMP parameter for your OVERWRITE WHERE clause.<br /> <br />To avoid this error, verify that the you specified is valid. | `interval`: time chunk interval corresponding to the out-of-bounds timestamp |
| <a name="error_InvalidNullByte">`InvalidNullByte`</a> | A string column included a null byte. Null bytes in strings are not permitted. |`source`: The source that included the null byte <br /><br /> `rowNumber`: The row number (1-indexed) that included the null byte <br /><br /> `column`: The column that included the null byte <br /><br /> `value`: Actual string containing the null byte <br /><br /> `position`: Position (1-indexed) of occurrence of null byte|
| <a name="error_QueryNotSupported">`QueryNotSupported`</a> | QueryKit could not translate the provided native query to a multi-stage query.<br /> <br />This can happen if the query uses features that aren't supported, like GROUPING SETS. | |
| <a name="error_QueryRuntimeError">`QueryRuntimeError`</a> | MSQ uses the native query engine to run the leaf stages. This error tells MSQ that error is in native query runtime.<br /> <br /> Since this is a generic error, the user needs to look at logs for the error message and stack trace to figure out the next course of action. If the user is stuck, consider raising a `github` issue for assistance. |  `baseErrorMessage` error message from the native query runtime. |
| <a name="error_RowTooLarge">`RowTooLarge`</a> | The query tried to process a row that was too large to write to a single frame. See the [Limits](#limits) table for specific limits on frame size. Note that the effective maximum row size is smaller than the maximum frame size due to alignment considerations during frame writing. | `maxFrameSize`: The limit on the frame size. |
| <a name="error_TaskStartTimeout">`TaskStartTimeout`</a> | Unable to launch `pendingTasks` worker out of total `totalTasks` workers tasks within `timeout` seconds of the last successful worker launch.<br /><br />There may be insufficient available slots to start all the worker tasks simultaneously. Try splitting up your query into smaller chunks using a smaller value of [`maxNumTasks`](#context-parameters). Another option is to increase capacity. | `pendingTasks`: Number of tasks not yet started.<br /><br />`totalTasks`: The number of tasks attempted to launch.<br /><br />`timeout`: Timeout, in milliseconds, that was exceeded. |
| <a name="error_TooManyAttemptsForJob">`TooManyAttemptsForJob`</a> | Total relaunch attempt count across all workers exceeded max relaunch attempt limit. See the [Limits](#limits) table for the specific limit. | `maxRelaunchCount`: Max number of relaunches across all the workers defined in the [Limits](#limits) section. <br /><br /> `currentRelaunchCount`: current relaunch counter for the job across all workers. <br /><br /> `taskId`: Latest task id which failed <br /> <br /> `rootErrorMessage`: Error message of the latest failed task.|
| <a name="error_TooManyAttemptsForWorker">`TooManyAttemptsForWorker`</a> | Worker exceeded maximum relaunch attempt count as defined in the [Limits](#limits) section. |`maxPerWorkerRelaunchCount`: Max number of relaunches allowed per worker as defined in the [Limits](#limits) section. <br /><br /> `workerNumber`: the worker number for which the task failed <br /><br /> `taskId`: Latest task id which failed <br /> <br /> `rootErrorMessage`: Error message of the latest failed task.|
| <a name="error_TooManyBuckets">`TooManyBuckets`</a> | Exceeded the maximum number of partition buckets for a stage (5,000 partition buckets).<br />< br />Partition buckets are created for each [`PARTITIONED BY`](#partitioned-by) time chunk for INSERT and REPLACE queries. The most common reason for this error is that your `PARTITIONED BY` is too narrow relative to your data. | `maxBuckets`: The limit on partition buckets. |
| <a name="error_TooManyInputFiles">`TooManyInputFiles`</a> | Exceeded the maximum number of input files or segments per worker (10,000 files or segments).<br /><br />If you encounter this limit, consider adding more workers, or breaking up your query into smaller queries that process fewer files or segments per query. | `numInputFiles`: The total number of input files/segments for the stage.<br /><br />`maxInputFiles`: The maximum number of input files/segments per worker per stage.<br /><br />`minNumWorker`: The minimum number of workers required for a successful run. |
| <a name="error_TooManyPartitions">`TooManyPartitions`</a> | Exceeded the maximum number of partitions for a stage (25,000 partitions).<br /><br />This can occur with INSERT or REPLACE statements that generate large numbers of segments, since each segment is associated with a partition. If you encounter this limit, consider breaking up your INSERT or REPLACE statement into smaller statements that process less data per statement. | `maxPartitions`: The limit on partitions which was exceeded |
| <a name="error_TooManyClusteredByColumns">`TooManyClusteredByColumns`</a>  | Exceeded the maximum number of clustering columns for a stage (1,500 columns).<br /><br />This can occur with `CLUSTERED BY`, `ORDER BY`, or `GROUP BY` with a large number of columns. | `numColumns`: The number of columns requested.<br /><br />`maxColumns`: The limit on columns which was exceeded.`stage`: The stage number exceeding the limit<br /><br /> |
| <a name="error_TooManyRowsWithSameKey">`TooManyRowsWithSameKey`</a> | The number of rows for a given key exceeded the maximum number of buffered bytes on both sides of a join. See the [Limits](#limits) table for the specific limit. Only occurs when join is executed via the sort-merge join algorithm. | `key`: The key that had a large number of rows.<br /><br />`numBytes`: Number of bytes buffered, which may include other keys.<br /><br />`maxBytes`: Maximum number of bytes buffered. |
| <a name="error_TooManyColumns">`TooManyColumns`</a> | Exceeded the maximum number of columns for a stage (2,000 columns). | `numColumns`: The number of columns requested.<br /><br />`maxColumns`: The limit on columns which was exceeded. |
| <a name="error_TooManyWarnings">`TooManyWarnings`</a> | Exceeded the maximum allowed number of warnings of a particular type. | `rootErrorCode`: The error code corresponding to the exception that exceeded the required limit. <br /><br />`maxWarnings`: Maximum number of warnings that are allowed for the corresponding `rootErrorCode`. |
| <a name="error_TooManyWorkers">`TooManyWorkers`</a> | Exceeded the maximum number of simultaneously-running workers. See the [Limits](#limits) table for more details. | `workers`: The number of simultaneously running workers that exceeded a hard or soft limit. This may be larger than the number of workers in any one stage if multiple stages are running simultaneously. <br /><br />`maxWorkers`: The hard or soft limit on workers that was exceeded. If this is lower than the hard limit (1,000 workers), then you can increase the limit by adding more memory to each task. |
| <a name="error_NotEnoughMemory">`NotEnoughMemory`</a> | Insufficient memory to launch a stage. | `suggestedServerMemory`: Suggested number of bytes of memory to allocate to a given process. <br /><br />`serverMemory`: The number of bytes of memory available to a single process.<br /><br />`usableMemory`: The number of usable bytes of memory for a single process.<br /><br />`serverWorkers`: The number of workers running in a single process.<br /><br />`serverThreads`: The number of threads in a single process. |
| <a name="error_NotEnoughTemporaryStorage">`NotEnoughTemporaryStorage`</a> | Insufficient temporary storage configured to launch a stage. This limit is set by the property `druid.indexer.task.tmpStorageBytesPerTask`. This property should be increased to the minimum suggested limit to resolve this.| `suggestedMinimumStorage`: Suggested number of bytes of temporary storage space to allocate to a given process. <br /><br />`configuredTemporaryStorage`: The number of bytes of storage currently configured. |
| <a name="error_WorkerFailed">`WorkerFailed`</a> | A worker task failed unexpectedly. | `errorMsg`<br /><br />`workerTaskId`: The ID of the worker task. |
| <a name="error_WorkerRpcFailed">`WorkerRpcFailed`</a> | A remote procedure call to a worker task failed and could not recover. | `workerTaskId`: the id of the worker task |
| <a name="error_UnknownError">`UnknownError`</a> | All other errors. | `message` |
