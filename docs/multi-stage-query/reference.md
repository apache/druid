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

## SQL reference

This topic is a reference guide for the multi-stage query architecture in Apache&circledR; Druid. For examples of real-world
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

Variation 2, with the input schema expressed in SQL using an `EXTEND` clause. See the next
section for more detail on `EXTEND`. This format also uses named arguments to make the SQL easier to read:

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

You can use `EXTERN` to specify a destination to export data.
This variation of `EXTERN` accepts the details of the destination as the only argument and requires an `AS` clause to specify the format of the exported rows.

When you export data, Druid creates metadata files in a subdirectory named `_symlink_format_manifest`.
Within the `_symlink_format_manifest/manifest` directory, the `manifest` file lists absolute paths to exported files using the symlink manifest format. For example:

```text
s3://export-bucket/export/query-6564a32f-2194-423a-912e-eead470a37c4-worker2-partition2.csv
s3://export-bucket/export/query-6564a32f-2194-423a-912e-eead470a37c4-worker1-partition1.csv
s3://export-bucket/export/query-6564a32f-2194-423a-912e-eead470a37c4-worker0-partition0.csv
...
s3://export-bucket/export/query-6564a32f-2194-423a-912e-eead470a37c4-worker0-partition24.csv
```

Keep the following in mind when using EXTERN to export rows:
- Only INSERT statements are supported.
- Only `CSV` format is supported as an export format.
- Partitioning (PARTITIONED BY) and clustering (CLUSTERED BY) aren't supported with EXTERN statements.
- You can export to Amazon S3, Google GCS, or local storage.
- The destination provided should contain no other files or directories.

When you export data, use the `rowsPerPage` context parameter to restrict the size of exported files.
When the number of rows in the result set exceeds the value of the parameter, Druid splits the output into multiple files.
The following statement shows the format of a SQL query using EXTERN to export rows:

```sql
SET rowsPerPage=<number_of_rows>;
INSERT INTO
  EXTERN(<destination function>)
AS CSV
SELECT
  <column>
FROM <table>
```

For details on applying context parameters using SET, see [SET](../querying/sql.md#set).


##### S3 - Amazon S3

To export results to S3, pass the `s3()` function as an argument to the `EXTERN` function.
Export to S3 requires the `druid-s3-extensions` extension.
For a list of S3 permissions the MSQ task engine requires to perform export, see [Permissions for durable storage](./security.md#s3).

The `s3()` function configures the connection to AWS.
Pass all arguments for `s3()` as named parameters with their values enclosed in single quotes. For example:

```sql
INSERT INTO
  EXTERN(
    s3(bucket => 'your_bucket', prefix => 'prefix/to/files', assumeRoleArn => 'arn:aws:iam::some-role')
  )
AS CSV
SELECT
  <column>
FROM <table>
```

Supported arguments for the function:

| Parameter | Required | Description | Default |
|---|---|---|---|
| `bucket` | Yes  | S3 bucket destination for exported files. You must add the bucket and prefix combination to the `druid.export.storage.s3.allowedExportPaths` allow list. | n/a |
| `prefix` | Yes  | Destination path in the bucket to create exported files. The export query expects the destination path to be empty. If the location includes other files, the query will fail. You must add the bucket and prefix combination to the `druid.export.storage.s3.allowedExportPaths` allow list. | n/a |
| `assumeRoleArn` | No | ARN of the role to assume before exporting data. If not provided, the default credentials configured for the Druid process are used. | n/a |
| `assumeRoleExternalId` | No | External ID to use when assuming the role specified in assumeRoleArn. This provides an additional layer of security for role assumption. Only used when assumeRoleArn is set. | n/a |

Configure the following runtime parameters to export to an S3 destination:

| Runtime parameter | Required | Description | Default |
|---|---|---|---|
| `druid.export.storage.s3.allowedExportPaths` | Yes | Array of S3 prefixes allowed as export destinations. Export queries fail if the export destination does not match any of the configured prefixes. For example: `[\"s3://bucket1/export/\", \"s3://bucket2/export/\"]` | n/a |
| `druid.export.storage.s3.tempLocalDir` | No | Directory for local storage where the worker stores temporary files before uploading the data to S3. | n/a |
| `druid.export.storage.s3.maxRetry` | No | Maximum number of  attempts for S3 API calls to avoid failures due to transient errors. | 10 |
| `druid.export.storage.s3.chunkSize` | No | Individual chunk size to store temporarily in `tempDir`. Large chunk sizes reduce the number of API calls to S3, but require more disk space to store temporary chunks. | 100MiB |

##### GOOGLE - Google Cloud Storage

To export query results to Google Cloud Storage (GCS), pass the `google()` function as an argument to the `EXTERN` function.
Export to GCS requires the `druid-google-extensions` extension.

The `google()` function configures the connection to GCS. Pass the arguments for `google()` as named parameters with their values enclosed in single quotes. For example:

```sql
INSERT INTO
  EXTERN(
    google(bucket => 'your_bucket', prefix => 'prefix/to/files')
  )
AS CSV
SELECT
  <column>
FROM <table>
```

Supported arguments for the function:

| Parameter   | Required | Description | Default |
|---|---|---|---|
| `bucket`    | Yes | GCS bucket destination for exported files. You must add the bucket and prefix combination to the `druid.export.storage.google.allowedExportPaths` allow list. | n/a |
| `prefix` | Yes  | Destination path in the bucket to create exported files. The export query expects the destination path to be empty. If the location includes other files, the query will fail. You must add the bucket and prefix combination to the `druid.export.storage.google.allowedExportPaths` allow list. | n/a |

Configure the following runtime parameters to export query results to a GCS destination:

| Runtime parameter | Required | Description | Default |
|---|---|---|---|
| `druid.export.storage.google.allowedExportPaths` | Yes | Array of GCS prefixes allowed as export destinations. Export queries fail if the export destination does not match any of the configured prefixes. For example: `[\"gs://bucket1/export/\", \"gs://bucket2/export/\"]` | n/a     |
| `druid.export.storage.google.tempLocalDir` | No | Directory for local storage where the worker stores temporary files before uploading the data to GCS. | n/a |
| `druid.export.storage.google.maxRetry` | No |  Maximum number of attempts for GCS API calls to avoid failures due to transient errors. | 10 |
| `druid.export.storage.google.chunkSize` | No | Individual chunk size to store temporarily in `tempDir`. Large chunk sizes reduce the number of API calls to GS, but require more disk space to store temporary chunks. | 4 MiB |


##### LOCAL - local file storage

You can export queries to local storage. This process writes the results to the filesystem on the MSQ worker.
This is useful in a single node setup or for testing but is not suitable for production use cases.

To export results to local storage, pass the `LOCAL()` function as an argument to the EXTERN function.
You must configure the runtime property `druid.export.storage.baseDir` as an absolute path on the Indexer or Middle Manager to use local storage as an export destination.
You can export data to paths that match this value as a prefix.
Pass all arguments to `LOCAL()` as named parameters with values enclosed in single quotes. For example:

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

Supported arguments for the function:

| Parameter | Required | Description | Default |
|---|---|---|---|
| `exportPath`  | Yes | Absolute path to a subdirectory of `druid.export.storage.baseDir` where Druid exports the query results. The destination must be empty. If the location includes other files or directories, the query will fail. | n/a |

For more information, see [Export external data with EXTERN](concepts.md#write-to-an-external-destination-with-extern).

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

For more information, see [Overwrite data with REPLACE](concepts.md#overwrite-data-with-replace).

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
column names or expressions.

This column list is used for [secondary partitioning](../ingestion/partitioning.md#secondary-partitioning) of segments
within a time chunk, and [sorting](../ingestion/partitioning.md#sorting) of rows within a segment. For sorting purposes,
Druid implicitly prepends `__time` to the `CLUSTERED BY` column list, unless
[`forceSegmentSortByTime`](#context-parameters) is set to `false`
(an experimental feature; see [Sorting](../ingestion/partitioning.md#sorting) for details).

For more information about clustering, see [Clustering](concepts.md#clustering).

<a name="context"></a>

## Query context parameters {#context-parameters}

The multi-stage query task engine supports the [SQL context parameters](../querying/sql-query-context.md), as well as its own context parameters described in this section. Use these parameters to tailor how Druid executes your query.

You can specify the context parameters in SELECT, INSERT, or REPLACE statements.

For detailed instructions on configuring query context parameters, refer to [Set query context](../querying/query-context.md).

The following context parameters apply to the MSQ task engine. See the [query context reference](../querying/query-context-reference.md#sql-based-ingestion-parameters) for details.

<!-- BEGIN GENERATED QUERY CONTEXT PARAMETER TABLE: engines~MSQ engines!~NATIVE -->

|Parameter|Statements|Description|Default|
|---|---|---|---|
|[`arrayIngestMode`](../querying/query-context-reference.md#arrayingestmode)|INSERT, REPLACE|Controls how ARRAY type values are stored in Druid segments.|`array`|
|[`backgroundFetchExternalFiles`](../querying/query-context-reference.md#backgroundfetchexternalfiles)|SELECT, INSERT, REPLACE|Controls how workers read cloud storage files (e.g. `s3`, `gs`, `azure`) referenced by [`EXTERN`](../multi-stage-query/reference.md#extern-function).|`true`|
|[`catalogValidationEnabled`](../querying/query-context-reference.md#catalogvalidationenabled)||If false, Druid skips the catalog-specific validation of `INSERT` and `REPLACE` statements that write to tables defined in the Druid catalog.|`true`|
|[`durableShuffleStorage`](../querying/query-context-reference.md#durableshufflestorage)|SELECT, INSERT, REPLACE|Whether to use durable storage for shuffle mesh.|`false`|
|[`executionMode`](../querying/query-context-reference.md#executionmode)||Determines how the [SQL statements API](../api-reference/sql-api.md#query-from-deep-storage) fetches query results.|N/A|
|[`failOnEmptyInsert`](../querying/query-context-reference.md#failonemptyinsert)|INSERT, REPLACE|When set to false (the default), an INSERT query generating no output rows will be no-op, and a REPLACE query generating no output rows will delete all data that matches the OVERWRITE clause.|`false`|
|[`faultTolerance`](../querying/query-context-reference.md#faulttolerance)|SELECT, INSERT, REPLACE|Whether to turn on fault tolerance mode or not.|`false`|
|[`finalizeAggregations`](../querying/query-context-reference.md#finalizeaggregations)|SELECT, INSERT, REPLACE|Determines the type of aggregation to return.|`true`|
|[`forceSegmentSortByTime`](../querying/query-context-reference.md#forcesegmentsortbytime)|INSERT, REPLACE|When set to `true` (the default), Druid prepends `__time` to [CLUSTERED BY](../multi-stage-query/reference.md#clustered-by) when determining the sort order for individual segments.|`true`|
|[`includeAllCounters`](../querying/query-context-reference.md#includeallcounters)|SELECT, INSERT, REPLACE|Whether to include counters that were added in Druid 31 or later.|`true`|
|[`includeSegmentSource`](../querying/query-context-reference.md#includesegmentsource)|SELECT, INSERT, REPLACE|Controls the sources Druid queries for results in addition to the segments present on deep storage.|`NONE` for the MSQ task engine, `REALTIME` for Dart|
|[`indexSpec`](../querying/query-context-reference.md#indexspec)|INSERT, REPLACE|An [`indexSpec`](../ingestion/ingestion-spec.md#indexspec) to use when generating segments.|See [`indexSpec`](../ingestion/ingestion-spec.md#indexspec).|
|[`maxConcurrentStages`](../querying/query-context-reference.md#maxconcurrentstages)|SELECT, INSERT, REPLACE|Number of stages that can run concurrently for a query.|`2` for Dart, `1` for the MSQ task engine|
|[`maxFrameSize`](../querying/query-context-reference.md#maxframesize)|SELECT, INSERT, REPLACE|Size of frames used for data transfer within the MSQ engine.|`1000000` (1 MB)|
|[`maxInputFilesPerWorker`](../querying/query-context-reference.md#maxinputfilesperworker)|SELECT, INSERT, REPLACE|Maximum number of input files or segments per worker.|`10000`|
|[`maxNumSegments`](../querying/query-context-reference.md#maxnumsegments)|INSERT, REPLACE|Maximum number of segments that an ingestion query can generate in a single time chunk.|No limit|
|[`maxNumTasks`](../querying/query-context-reference.md#maxnumtasks)|SELECT, INSERT, REPLACE|The maximum total number of tasks to launch, including the controller task.|`2`|
|[`maxParseExceptions`](../querying/query-context-reference.md#maxparseexceptions)|SELECT, INSERT, REPLACE|Maximum number of parse exceptions that are ignored while executing the query before it stops with `TooManyWarningsFault`.|`0`|
|[`maxPartitions`](../querying/query-context-reference.md#maxpartitions)|SELECT, INSERT, REPLACE|Maximum number of output partitions for any single stage.|`25000`|
|[`maxRowsInMemory`](../querying/query-context-reference.md#maxrowsinmemory)|INSERT, REPLACE|Maximum number of rows to store in memory at once before flushing to disk during the segment generation process.|`100000`|
|[`maxRowsMaterializedInWindow`](../querying/query-context-reference.md#maxrowsmaterializedinwindow)|SELECT, INSERT, REPLACE|Maximum number of rows that window functions can materialize in a single window partition.|`100000`|
|[`maxThreads`](../querying/query-context-reference.md#maxthreads)|SELECT, INSERT, REPLACE|Maximum number of threads to use for processing.|Not set (use default thread count)|
|[`removeNullBytes`](../querying/query-context-reference.md#removenullbytes)|SELECT, INSERT, REPLACE|The MSQ engine can't process null bytes in strings and throws `InvalidNullByteFault` if it encounters them in the source data.|`false`|
|[`rowsInMemory`](../querying/query-context-reference.md#rowsinmemory)|INSERT, REPLACE|Alternate spelling of `maxRowsInMemory`.|`100000`|
|[`rowsPerPage`](../querying/query-context-reference.md#rowsperpage)|SELECT|The number of rows per page to target.|`100000`|
|[`rowsPerSegment`](../querying/query-context-reference.md#rowspersegment)|INSERT, REPLACE|The number of rows per segment to target.|`3000000`|
|[`segmentSortOrder`](../querying/query-context-reference.md#segmentsortorder)|INSERT, REPLACE|Normally, Druid sorts rows in individual segments using `__time` first, followed by the [CLUSTERED BY](../multi-stage-query/reference.md#clustered-by) clause.|empty list|
|[`selectDestination`](../querying/query-context-reference.md#selectdestination)|SELECT|Controls where the final result of the select query is written.|`taskReport`|
|[`skipTypeVerification`](../querying/query-context-reference.md#skiptypeverification)|INSERT, REPLACE|During query validation, Druid validates that [string arrays](../querying/arrays.md) and [multi-value dimensions](../querying/multi-value-dimensions.md) are not mixed in the same column.|empty list|
|[`sqlJoinAlgorithm`](../querying/query-context-reference.md#sqljoinalgorithm)|SELECT, INSERT, REPLACE|Algorithm to use for `JOIN`: `broadcast` for broadcast hash join or `sortMerge` for sort-merge join.|`broadcast`|
|[`storeCompactionState`](../querying/query-context-reference.md#storecompactionstate)|REPLACE|When set to true, a REPLACE query stores as part of each segment's metadata a `lastCompactionState` field that captures the various specs used to create the segment.|`false`|
|[`targetPartitionsPerWorker`](../querying/query-context-reference.md#targetpartitionsperworker)|SELECT, INSERT, REPLACE|Number of partitions Druid generates for each worker.|`1` for Dart, the number of processing threads for the MSQ task engine|
|[`taskAssignment`](../querying/query-context-reference.md#taskassignment)|SELECT, INSERT, REPLACE|Determines how many tasks to use.|`max`|
|[`taskLockType`](../querying/query-context-reference.md#tasklocktype)|INSERT, REPLACE|Type of time chunk lock that an INSERT or REPLACE query acquires: `EXCLUSIVE`, `SHARED`, `APPEND` or `REPLACE`.|`EXCLUSIVE` for REPLACE, `SHARED` for INSERT|
|[`useConcurrentLocks`](../querying/query-context-reference.md#useconcurrentlocks)|INSERT, REPLACE|Whether an INSERT or REPLACE query uses concurrent locks: `APPEND` locks for INSERT and `REPLACE` locks for REPLACE.|`false`|
|[`waitUntilSegmentsLoad`](../querying/query-context-reference.md#waituntilsegmentsload)|INSERT, REPLACE|If set, the ingest query waits for the generated segments to be loaded before exiting, else the ingest query exits without waiting.|`false`|

<!-- END GENERATED QUERY CONTEXT PARAMETER TABLE -->

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

The following query runs a single sort-merge join stage that takes the following inputs:
* `eventstream` partitioned on `user_id`
* `users` partitioned on `id`

There is no limit on the size of either input.
The SET command assigns the `sqlJoinAlgorithm` context parameter so that Druid uses the sort-merge join algorithm for the query.

```sql
SET sqlJoinAlgorithm='sortMerge';
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

## Durable storage

SQL-based ingestion supports using durable storage to store intermediate files temporarily. Enabling it can improve reliability. For more information, see [Durable storage](../operations/durable-storage.md).

### Durable storage configurations

Durable storage is supported on Amazon S3 storage, Microsoft&circledR; Azure Blob Storage and Google Cloud Storage. 
There are common configurations that control the behavior regardless of which storage service you use. Apart from these common configurations, there are a few properties specific to S3 and to Azure.

Common properties to configure the behavior of durable storage

|Parameter          | Required | Description          | Default | 
|--|--|--|--|
|`druid.msq.intermediate.storage.enable`  | Yes |  Whether to enable durable storage for the cluster. Set it to true to enable durable storage. For more information about enabling durable storage, see [Durable storage](../operations/durable-storage.md). | false | 
|`druid.msq.intermediate.storage.type` |  Yes | The type of storage to use. Set it to `s3` for S3, `azure` for Azure and `google` for Google | n/a |
|`druid.msq.intermediate.storage.tempDir`| Yes |  Directory path on the local disk to store temporary files required while uploading and downloading the data. If the property is not configured on the indexer or middle manager, it defaults to using the task temporary directory. | n/a |
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
| Size of an individual row written to a frame. Row size when written to a frame may differ from the original row size. Configurable with [`maxFrameSize`](../querying/query-context-reference.md#maxframesize). | 1 MB | `RowTooLarge` |
| Number of segment-granular time chunks encountered during ingestion. | 5,000 | `TooManyBuckets`|
| Number of input files/segments per worker. Configurable with [`maxInputFilesPerWorker`](../querying/query-context-reference.md#maxinputfilesperworker). | 10,000 | `TooManyInputFiles`|
| Number of output partitions for any one stage. Number of segments generated during ingestion. Configurable with [`maxPartitions`](../querying/query-context-reference.md#maxpartitions). | 25,000 | `TooManyPartitions`|
| Number of output columns for any one stage. | 2,000 | `TooManyColumns`|
| Number of cluster by columns that can appear in a stage | 1,500 | `TooManyClusteredByColumns` |
| Number of workers for any one stage. | Hard limit is 1,000. Memory-dependent soft limit may be lower. | `TooManyWorkers`|
| Maximum memory occupied by broadcasted tables. | 30% of each [processor memory bundle](concepts.md#memory-usage). | `BroadcastTablesTooLarge` |
| Maximum memory occupied by buffered data during sort-merge join. Only relevant when `sqlJoinAlgorithm` is `sortMerge`. | 10 MB | `TooManyRowsWithSameKey` |
| Number of rows materialized in a single window partition. Configurable with `maxRowsMaterializedInWindow`. | 100,000 | `TooManyRowsInAWindow` |
| Maximum relaunch attempts per worker. Initial run is not a relaunch. The worker will be spawned 1 + `workerRelaunchLimit` times before the job fails. | 2 | `TooManyAttemptsForWorker` |
| Maximum relaunch attempts for a job across all workers. | 100 | `TooManyAttemptsForJob` |
<a name="errors"></a>

## Error codes

The following table describes error codes you may encounter in the `multiStageQuery.payload.status.errorReport.error.errorCode` field:

| Code | Meaning | Additional fields |
|---|---|---|
| `BroadcastTablesTooLarge` | The size of the broadcast tables used in the right hand side of the join exceeded the memory reserved for them in a worker task.<br /><br />Try increasing the peon memory or reducing the size of the broadcast tables. | `maxBroadcastTablesSize`: Memory reserved for the broadcast tables, measured in bytes. |
| `Canceled`| The query was canceled. Common reasons for cancellation:<br /><br /><ul><li>User-initiated shutdown of the controller task via the `/druid/indexer/v1/task/{taskId}/shutdown` API.</li><li>Restart or failure of the server process that was running the controller task.</li></ul>| |
| `CannotParseExternalData`| A worker task could not parse data from an external datasource. | `errorMessage`: More details on why parsing failed. |
| `ColumnNameRestricted`| The query uses a restricted column name. | `columnName`: The restricted column name. |
| `ColumnTypeNotSupported` | The column type is not supported. This can be because:<br /> <br /><ul><li>Support for writing or reading from a particular column type is not supported.</li><li>The query attempted to use a column type that is not supported by the frame format. This occurs with ARRAY types, which are not yet implemented for frames.</li></ul> | `columnName`: The column name with an unsupported type.<br /> <br />`columnType`: The unknown column type. |
| `DruidException` | An error occurred with structured information such as code, persona, and category. It does not fall into one of the other categories. | `druidErrorCode`: More specific error code.<br /><br />`persona`: Target persona.<br /><br />`category`: Error category.<br /><br />`context`: Additional context. |
| `DurableStorageConfiguration` | Durable storage mode was enabled but is not configured correctly. Check the documentation on how to enable durable storage mode. | `message`: Details on the configuration error. |
|`InsertCannotAllocateSegment`| The controller task could not allocate a new segment ID due to conflict with existing segments or pending segments. Common reasons for such conflicts:<br /> <br /><ul><li>Attempting to mix different granularities in the same intervals of the same datasource.</li><li>Prior ingestions that used non-extendable shard specs.</li></ul> <br /> <br /> Use REPLACE to overwrite the existing data or if the error contains the `allocatedInterval` then alternatively rerun the INSERT job with the mentioned granularity to append to existing data. Note that it might not always be possible to append to the existing data using INSERT and can only be done if `allocatedInterval` is present. | `dataSource`<br /> <br />`interval`: The interval for the attempted new segment allocation. <br /> <br /> `allocatedInterval`: The incorrect interval allocated by the overlord. It can be null |
|`InsertCannotBeEmpty`| An INSERT or REPLACE query did not generate any output rows when `failOnEmptyInsert` query context is set to true. `failOnEmptyInsert` defaults to false, so an INSERT query generating no output rows will be no-op, and a REPLACE query generating no output rows will delete all data that matches the OVERWRITE clause. | `dataSource` |
| `InsertLockPreempted` | An INSERT or REPLACE query was canceled by a higher-priority ingestion job, such as a real-time ingestion task. | |
| `InsertTimeNull` | An INSERT or REPLACE query encountered a null timestamp in the `__time` field.<br /><br />This can happen due to using an expression like `TIME_PARSE(timestamp) AS __time` with a timestamp that cannot be parsed. ([`TIME_PARSE`](../querying/sql-scalar.md#date-and-time-functions) returns null when it cannot parse a timestamp.) In this case, try parsing your timestamps using a different function or pattern. Or, if your timestamps may genuinely be null, consider using [`COALESCE`](../querying/sql-scalar.md#other-scalar-functions) to provide a default value. One option is [`CURRENT_TIMESTAMP`](../querying/sql-scalar.md#date-and-time-functions), which represents the start time of the job.|
| `InsertTimeOutOfBounds`| A REPLACE query generated a timestamp outside the bounds of the TIMESTAMP parameter for your OVERWRITE WHERE clause.<br /> <br />To avoid this error, verify that the you specified is valid. | `interval`: time chunk interval corresponding to the out-of-bounds timestamp |
| `InvalidField`| An error was encountered while writing a field. | `error`: Encountered error. <br /><br /> `source`: Source for the error. <br /><br /> `rowNumber`: Row number (1-indexed) for the error. <br /><br /> `column`: Column for the error. |
| `InvalidNullByte`| A string column included a null byte. Null bytes in strings are not permitted. |`source`: The source that included the null byte <br /><br /> `rowNumber`: The row number (1-indexed) that included the null byte <br /><br /> `column`: The column that included the null byte <br /><br /> `value`: Actual string containing the null byte <br /><br /> `position`: Position (1-indexed) of occurrence of null byte|
| `QueryNotSupported`| QueryKit could not translate the provided native query to a multi-stage query.<br /> <br />This can happen if the query uses features that aren't supported, like GROUPING SETS. | |
| `RowTooLarge`| The query tried to process a row that was too large to write to a single frame. See the [Limits](#limits) table for specific limits on frame size. Note that the effective maximum row size is smaller than the maximum frame size due to alignment considerations during frame writing. | `maxFrameSize`: The limit on the frame size. |
| `TaskStartTimeout` | Unable to launch `pendingTasks` worker out of total `totalTasks` workers tasks within `timeout` seconds of the last successful worker launch.<br /><br />There may be insufficient available slots to start all the worker tasks simultaneously. Try splitting up your query into smaller chunks using a smaller value of [`maxNumTasks`](#context-parameters). Another option is to increase capacity. | `pendingTasks`: Number of tasks not yet started.<br /><br />`totalTasks`: The number of tasks attempted to launch.<br /><br />`timeout`: Timeout, in milliseconds, that was exceeded. |
| `TooManyAttemptsForJob` | Total relaunch attempt count across all workers exceeded max relaunch attempt limit. See the [Limits](#limits) table for the specific limit. | `maxRelaunchCount`: Max number of relaunches across all the workers defined in the [Limits](#limits) section. <br /><br /> `currentRelaunchCount`: current relaunch counter for the job across all workers. <br /><br /> `taskId`: Latest task id which failed <br /> <br /> `rootErrorMessage`: Error message of the latest failed task.|
| `TooManyAttemptsForWorker`| Worker exceeded maximum relaunch attempt count as defined in the [Limits](#limits) section. |`maxPerWorkerRelaunchCount`: Max number of relaunches allowed per worker as defined in the [Limits](#limits) section. <br /><br /> `workerNumber`: the worker number for which the task failed <br /><br /> `taskId`: Latest task id which failed <br /> <br /> `rootErrorMessage`: Error message of the latest failed task.|
| `TooManyBuckets` | Exceeded the maximum number of partition buckets for a stage (5,000 partition buckets).<br />< br />Partition buckets are created for each [`PARTITIONED BY`](#partitioned-by) time chunk for INSERT and REPLACE queries. The most common reason for this error is that your `PARTITIONED BY` is too narrow relative to your data. | `maxBuckets`: The limit on partition buckets. |
| `TooManyInputFiles` | Exceeded the maximum number of input files or segments per worker (10,000 files or segments).<br /><br />If you encounter this limit, consider adding more workers, or breaking up your query into smaller queries that process fewer files or segments per query. | `numInputFiles`: The total number of input files/segments for the stage.<br /><br />`maxInputFiles`: The maximum number of input files/segments per worker per stage.<br /><br />`minNumWorker`: The minimum number of workers required for a successful run. |
| `TooManyPartitions`| Exceeded the maximum number of partitions for a stage (25,000 partitions).<br /><br />This can occur with INSERT or REPLACE statements that generate large numbers of segments, since each segment is associated with a partition. If you encounter this limit, consider breaking up your INSERT or REPLACE statement into smaller statements that process less data per statement. | `maxPartitions`: The limit on partitions which was exceeded |
| `TooManyClusteredByColumns` | Exceeded the maximum number of clustering columns for a stage (1,500 columns).<br /><br />This can occur with `CLUSTERED BY`, `ORDER BY`, or `GROUP BY` with a large number of columns. | `numColumns`: The number of columns requested.<br /><br />`maxColumns`: The limit on columns which was exceeded.`stage`: The stage number exceeding the limit<br /><br /> |
| `TooManyRowsWithSameKey` | The number of rows for a given key exceeded the maximum number of buffered bytes on both sides of a join. See the [Limits](#limits) table for the specific limit. Only occurs when join is executed via the sort-merge join algorithm. | `key`: The key that had a large number of rows.<br /><br />`numBytes`: Number of bytes buffered, which may include other keys.<br /><br />`maxBytes`: Maximum number of bytes buffered. |
| `TooManyRowsInAWindow` | Too many rows materialized in a single window partition. Try using a window with a higher cardinality `PARTITION BY` column, changing the query shape, or increasing the limit with the `maxRowsMaterializedInWindow` query context parameter. | `numRows`: Number of rows encountered.<br /><br />`maxRows`: Maximum number of rows allowed. |
| `TooManySegmentsInTimeChunk` | Too many segments requested to be generated in a single time chunk. Try breaking up the query or increase the limit using the `maxNumSegments` query context parameter. | `timeChunk`: The time chunk that exceeded the limit.<br /><br />`numSegments`: Number of segments requested.<br /><br />`maxNumSegments`: Maximum number of segments allowed.<br /><br />`segmentGranularity`: The segment granularity. |
| `TooManyColumns` | Exceeded the maximum number of columns for a stage (2,000 columns). | `numColumns`: The number of columns requested.<br /><br />`maxColumns`: The limit on columns which was exceeded. |
| `TooManyWarnings` | Exceeded the maximum allowed number of warnings of a particular type. | `rootErrorCode`: The error code corresponding to the exception that exceeded the required limit. <br /><br />`maxWarnings`: Maximum number of warnings that are allowed for the corresponding `rootErrorCode`. |
| `TooManyWorkers`| Exceeded the maximum number of simultaneously-running workers. See the [Limits](#limits) table for more details. | `workers`: The number of simultaneously running workers that exceeded a hard or soft limit. This may be larger than the number of workers in any one stage if multiple stages are running simultaneously. <br /><br />`maxWorkers`: The hard or soft limit on workers that was exceeded. If this is lower than the hard limit (1,000 workers), then you can increase the limit by adding more memory to each task. |
| `NotEnoughMemory` | Insufficient memory to launch a stage. | `suggestedServerMemory`: Suggested number of bytes of memory to allocate to a given process. <br /><br />`serverMemory`: The number of bytes of memory available to a single process.<br /><br />`usableMemory`: The number of usable bytes of memory for a single process.<br /><br />`serverWorkers`: The number of workers running in a single process.<br /><br />`serverThreads`: The number of threads in a single process. |
| `NotEnoughTemporaryStorage` | Insufficient temporary storage configured to launch a stage. This limit is set by the property `druid.indexer.task.tmpStorageBytesPerTask`. This property should be increased to the minimum suggested limit to resolve this.| `suggestedMinimumStorage`: Suggested number of bytes of temporary storage space to allocate to a given process. <br /><br />`configuredTemporaryStorage`: The number of bytes of storage currently configured. |
| `WorkerFailed` | A worker task failed unexpectedly. | `errorMsg`<br /><br />`workerTaskId`: The ID of the worker task. |
| `WorkerRpcFailed` | A remote procedure call to a worker task failed and could not recover. | `workerTaskId`: the id of the worker task |
| `UnknownError` | All other errors. | `message` |
