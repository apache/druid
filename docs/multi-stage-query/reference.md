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

`INSERT` and `REPLACE` load data into a Druid datasource from either an external input source, or from another
datasource. When loading from an external datasource, you typically must provide the kind of input source,
the data format, and the schema (signature) of the input file. Druid provides *table functions* to allow you to
specify the external file. There are two kinds. `EXTERN` works with the JSON-serialized specs for the three
items, using the same JSON you would use in native ingest. A set of other, input-source-specific functions
use SQL syntax to specify the format and the input schema. There is one function for each input source. The
input-source-specific functions allow you to use SQL query parameters to specify the set of files (or URIs),
making it easy to reuse the same SQL statement for each ingest: just specify the set of files to use each time.

### `EXTERN` Function

Use the `EXTERN` function to read external data. The function has two variations.

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

1. Any [Druid input source](../ingestion/native-batch-input-source.md) as a JSON-encoded string.
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

For more information, see [Read external data with EXTERN](concepts.md#extern).

### `HTTP`, `INLINE`, `LOCALFILES` and `S3` Functions

While `EXTERN` allows you to specify an external table using JSON, other table functions allow you
describe the external table using SQL syntax. Each function works for one specific kind of input
source. You provide properties using SQL named arguments. The row signature is given using the
Druid SQL `EXTEND` keyword using SQL syntax and types.

The set of table functions and formats is preliminary in this release.

Function format:

```sql
SELECT
 <column>
FROM TABLE(
  http(
    userName => 'bob',
    password => 'secret',
    uris => ARRAY['http://example.com/foo.csv', 'http://example.com/bar.csv'],
    format => 'csv'
    )
  ) EXTEND (x VARCHAR, y VARCHAR, z BIGINT)
```

For each function, you provide:

* The function name indicates the kind of input source: `http`, `inline` or `localfiles`.
* The function arguments correspond to a subset of the JSON fields for that input source.
* A `format` argument to indicate the desired input format.
* Additional arguments required for the selected format type.

Note that the `EXTEND` keyword is optional. The following is equally valid (and perhaps
more convenient):

```sql
SELECT
 <column>
FROM TABLE(
  http(
    userName => 'bob',
    password => 'secret',
    uris => ARRAY['http://example.com/foo.csv', 'http://example.com/bar.csv'],
    format => 'csv'
    )
  ) (x VARCHAR, y VARCHAR, z BIGINT)
```

#### Function Arguments

These table functions are intended for use with the SQL by-name argument syntax
as shown above. Because the functions include all parameters for all formats,
using positional calls is both cumbersome and error-prone.

Function argument names are generally the same as the JSON field names, except
as noted below. Each argument has a SQL type which matches the JSON type. For
arguments that take a string list in JSON, use the SQL `ARRAY[...]` syntax in
SQL as shown in the above example.

Array parameters are good candidates for use in parameterized queries. That is:

```sql
SELECT
 <column>
FROM TABLE(
  http(
    userName => 'bob',
    password => 'secret',
    uris => ?,
    format => 'csv'
    )
  ) (x VARCHAR, y VARCHAR, z BIGINT)
```

Provide the list of URIs (in this case) as a query parameter in each ingest. Doing
so is simpler than writing a script to insert the array into the SQL text.

#### `HTTP` Function

The `HTTP` table function represents the
[HTTP input source](../ingestion/native-batch-input-source.md#http-input-source)
to read from an HTTP server. The function accepts the following arguments:

* `userName` (`VARCHAR`) -  Same as JSON `httpAuthenticationUsername`.
* `password`  (`VARCHAR`) - Same as`httpAuthenticationPassword` when used with the default option.
* `passwordEnvVar` (`VARCHAR`) - Same as the HTTP `httpAuthenticationPassword` when used with
  the `"type": "environment"` option.
* `uris` (`ARRAY` of `VARCHAR`)

#### `INLINE` Function

The `INLINE` table function represents the
[Inline input source](../ingestion/native-batch-input-source.md#inline-input-source)
which provides data directly in the table function. Parameter:

* `data` (`ARRAY` of `VARCHAR`) - Data lines, without a trailing newline, as an array.

Example:

```sql
SELECT ...
FROM TABLE(
  inline(
    data => ARRAY[
    	'a,b',
    	'c,d'],
    format => 'csv'
    )
  ) (x VARCHAR, y VARCHAR)
```

#### `LOCALFILES` Function

The `LOCALFILES` table function represents the
[Local input source](../ingestion/native-batch-input-source.md#local-input-source) which reads
files from the file system of the node running Druid. This is most useful for single-node
installations. The function accepts the following parameters:

* `baseDir`
* `filter`
* `files`

When the local files input source is used directly in an `extern` function, or ingestion spec, you
can provide either `baseDir` and `filter` or `files` but not both. This function, however, allows
you to provide any of the following combinations:

* `baseDir` - Matches all files in the given directory. (Assumes the filter is `*`.)
* `baseDir` and `filter` - Match files in the given directory using the filter.
* `baseDir` and `files` - A set of files relative to `baseDir`.
* `files` - The files should be absolute paths, else they will be computed relative to Druid's
  working directory (usually the Druid install directory.)

Examples:

To read All files in /tmp, which must be CSV files:

```sql
SELECT ...
FROM TABLE(
  localfiles(
      baseDir => '/tmp',
      format => 'csv')
  ) (x VARCHAR, y VARCHAR)
```

Some additional variations (omitting the common bits):

```sql
  -- CSV files in /tmp
  localfiles(baseDir => '/tmp',
             filter => '*.csv',
             format => 'csv')

  -- /tmp/a.csv and /tmp/b.csv
  localfiles(baseDir => '/tmp',
             files => ARRAY['a.csv', 'b.csv'],
             format => 'csv')

  -- /tmp/a.csv and /tmp/b.csv
  localfiles(files => ARRAY['/tmp/a.csv', '/tmp/b.csv'],
             format => 'csv')
```

#### `S3` Function

The `S3` table function represents the
[S3 input source](../ingestion/native-batch-input-source.md#s3-input-source) which reads
files from an S3 bucket. The function accepts the following parameters to specify the
objects to read:

* `uris` (`ARRAY` of `VARCHAR`)
* `prefix` (`VARCHAR`) - Corresponds to the JSON `prefixes` property, but allows a single
  prefix.
* `bucket` (`VARCHAR`) - Corresponds to the `bucket` field of the `objects` JSON field. SQL
  does not have syntax for an array of objects. Instead, this function takes a single bucket,
  and one or more objects within that bucket.
* `paths` (`ARRAY` of `VARCHAR`) - Corresponds to the `path` fields of the `object` JSON field.
  All paths are within the single `bucket` parameter.

The S3 input source accepts one of the following patterns:

* `uris` - A list of fully-qualified object URIs.
* `prefixes` - A list of fully-qualified "folder" prefixes.
* `bucket` and `paths` - A list of objects relative to the given bucket path.

The `S3` function also accepts the following security parameters:

* `accessKeyId` (`VARCHAR`)
* `secretAccessKey` (`VARCHAR`)
* `assumeRoleArn` (`VARCHAR`)

The `S3` table function does not support either the `clientConfig` or `proxyConfig`
JSON properties.

If you need the full power of the S3 input source, then consider the use of the `extern`
function, which accepts the full S3 input source serialized as JSON. Alternatively,
create a catalog external table that has the full set of properties, leaving just the
`uris` or `paths` to be provided at query time.

Examples, each of which correspond to an example on the
[S3 input source](../ingestion/native-batch-input-source.md#s3-input-source) page.
The examples omit the format and schema; however you must remember to provide those
in an actual query.

```sql
SELECT ...
FROM TABLE(S3(
    uris => ARRAY['s3://foo/bar/file.json', 's3://bar/foo/file2.json'],
    format => 'csv'))
  ) (x VARCHAR, y VARCHAR)
```

Additional variations, omitting the common bits:

```sql
  S3(prefixes => ARRAY['s3://foo/bar/', 's3://bar/foo/']))
```

```sql
  -- Not an exact match for the JSON example: the S3 function allows
  -- only one bucket.
  S3(bucket => 's3://foo`,
           paths => ARRAY['bar/file1.json', 'foo/file2.json'])
```

```sql
  S3(uris => ARRAY['s3://foo/bar/file.json', 's3://bar/foo/file2.json'],
           accessKeyId => 'KLJ78979SDFdS2',
           secretAccessKey => 'KLS89s98sKJHKJKJH8721lljkd')
```

```sql
  S3(uris => ARRAY['s3://foo/bar/file.json', 's3://bar/foo/file2.json'],
           accessKeyId => 'KLJ78979SDFdS2',
           secretAccessKey => 'KLS89s98sKJHKJKJH8721lljkd',
           assumeRoleArn => 'arn:aws:iam::2981002874992:role/role-s3')
```

#### Input Format

Each of the table functions above requires that you specify a format using the `format`
parameter which accepts a value the same as the format names used for `EXTERN` and described
for [each input source](../ingestion/native-batch-input-source.md).

#### CSV Format

The `csv` format selects the [CSV input format](../ingestion/data-formats.md#csv).
Parameters:

* `listDelimiter` (`VARCHAR`)
* `skipHeaderRows` (`BOOLEAN`)

Example for a CSV format with a list delimiter and where we want to skip the first
input row:

```sql
SELECT ...
FROM TABLE(
  inline(
    data => ARRAY[
        'skip me',
    	'a;foo,b',
    	'c;bar,d'],
    format => 'csv',
    listDelimiter => ';',
    skipHeaderRows => 1
    )
  ) (x VARCHAR, y VARCHAR)
```

#### Delimited Text Format

The `tsv` format selects the [TSV (Delimited) input format](../ingestion/data-formats.md#tsv-delimited).
Parameters:

* `delimiter` (`VARCHAR`)
* `listDelimiter` (`VARCHAR`)
* `skipHeaderRows` (`BOOLEAN`)

Example for a pipe-separated format with a list delimiter and where we want to skip the first
input row:

```sql
SELECT ...
FROM TABLE(
  inline(
    data => ARRAY[
        'skip me',
    	'a;foo|b',
    	'c;bar|d'],
    format => 'tsv',
    listDelimiter => ';',
    skipHeaderRows => 1,
    delimiter => '|'
    )
  ) (x VARCHAR, y VARCHAR)
```

#### JSON Format

The `json` format selects the
[JSON input format](../ingestion/data-formats.html#json).
The JSON format accepts no additional parameters.

Example:

```sql
SELECT ...
FROM TABLE(
  inline(
    data => ARRAY['{"x": "foo", "y": "bar"}'],
    format => 'json')
  ) (x VARCHAR, y VARCHAR)
```

The JSON function allows columns to be of type `TYPE('COMPLEX<json>')` which indicates that the column contains
some form of complex JSON: a JSON object, a JSON array, or an array of JSON objects or arrays.
Note that the case must exactly match that given: upper case `COMPLEX`, lower case `json`.
The SQL type simply names a native Druid type. However, the actual
segment column produced may be of some other type if Druid infers that it can use a simpler type
instead.

### Parameters

Starting with the Druid 26.0 release, you can use query parameters with MSQ queries. You may find
that you periodically ingest a new set of files into Druid. Often, the bulk of the query is identical
for each ingestion: only the list of files (or URIs or objects) changes. For example, for the `S3`
input source, you will likely ingest from the same bucket and security setup in
each query; only the specific objects will change. Consider using a query parameter
to pass the object names:

```sql
INSERT INTO ...
SELECT ...
FROM TABLE(S3(bucket => 's3://foo`,
              accessKeyId => ?,
              paths => ?,
              format => JSON))
     (a VARCHAR, b BIGINT, ...)
```

This same technique can be used with the `uris` or `prefixes` parameters instead.

Function arguments that take an array parameter require an array function in your JSON request.
For example:

```json
{
  "query" : "INSERT INTO ...
SELECT ...
FROM TABLE(S3(bucket => 's3://foo`,
              accessKeyId => ?,
              paths => ?,
              format => JSON))
     (a VARCHAR, b BIGINT, ...)",
  "parameters": [
    { "type": "VARCHAR", "value": "ABCD-EF01"},
    { "type": "VARCHAR", "value": [
    	"foo.csv", "bar.csv"
    ] }
  ]
}
```

The type in the above example is the type of each element. It must be `VARCHAR` for all the array
parameters for functions described on this page.

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

- Time unit keywords: `HOUR`, `DAY`, `MONTH`, or `YEAR`. Equivalent to `FLOOR(__time TO TimeUnit)`.
- Time units as ISO 8601 period strings: :`'PT1H'`, '`P1D`, etc. (Druid 26.0 and later.)
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
| `sqlJoinAlgorithm` | SELECT, INSERT, REPLACE<br /><br />Algorithm to use for JOIN. Use `broadcast` (the default) for broadcast hash join or `sortMerge` for sort-merge join. Affects all JOIN operations in the query. See [Joins](#joins) for more details. | `broadcast` |
| `rowsInMemory` | INSERT or REPLACE<br /><br />Maximum number of rows to store in memory at once before flushing to disk during the segment generation process. Ignored for non-INSERT queries. In most cases, use the default value. You may need to override the default if you run into one of the [known issues](./known-issues.md) around memory usage. | 100,000 |
| `segmentSortOrder` | INSERT or REPLACE<br /><br />Normally, Druid sorts rows in individual segments using `__time` first, followed by the [CLUSTERED BY](#clustered-by) clause. When you set `segmentSortOrder`, Druid sorts rows in segments using this column list first, followed by the CLUSTERED BY order.<br /><br />You provide the column list as comma-separated values or as a JSON array in string form. If your query includes `__time`, then this list must begin with `__time`. For example, consider an INSERT query that uses `CLUSTERED BY country` and has `segmentSortOrder` set to `__time,city`. Within each time chunk, Druid assigns rows to segments based on `country`, and then within each of those segments, Druid sorts those rows by `__time` first, then `city`, then `country`. | empty list |
| `maxParseExceptions`| SELECT, INSERT, REPLACE<br /><br />Maximum number of parse exceptions that are ignored while executing the query before it stops with `TooManyWarningsFault`. To ignore all the parse exceptions, set the value to -1.| 0 |
| `rowsPerSegment` | INSERT or REPLACE<br /><br />The number of rows per segment to target. The actual number of rows per segment may be somewhat higher or lower than this number. In most cases, use the default. For general information about sizing rows per segment, see [Segment Size Optimization](../operations/segment-optimization.md). | 3,000,000 |
| `indexSpec` | INSERT or REPLACE<br /><br />An [`indexSpec`](../ingestion/ingestion-spec.md#indexspec) to use when generating segments. May be a JSON string or object. See [Front coding](../ingestion/ingestion-spec.md#front-coding) for details on configuring an `indexSpec` with front coding. | See [`indexSpec`](../ingestion/ingestion-spec.md#indexspec). |
| `durableShuffleStorage` | SELECT, INSERT, REPLACE <br /><br />Whether to use durable storage for shuffle mesh. To use this feature, configure the durable storage at the server level using `druid.msq.intermediate.storage.enable=true`). If these properties are not configured, any query with the context variable `durableShuffleStorage=true` fails with a configuration error. <br /><br /> | `false` |
| `faultTolerance` | SELECT, INSERT, REPLACE<br /><br /> Whether to turn on fault tolerance mode or not. Failed workers are retried based on [Limits](#limits). Cannot be used when `durableShuffleStorage` is explicitly set to false.  | `false` |

## Joins

Joins in multi-stage queries use one of two algorithms, based on the [context parameter](#context-parameters)
`sqlJoinAlgorithm`. This context parameter applies to the entire SQL statement, so it is not possible to mix different
join algorithms in the same query.

### Broadcast

Set `sqlJoinAlgorithm` to `broadcast`.

The default join algorithm for multi-stage queries is a broadcast hash join, which is similar to how
[joins are executed with native queries](../querying/query-execution.md#join). First, any adjacent joins are flattened
into a structure with a "base" input (the bottom-leftmost one) and other leaf inputs (the rest). Next, any subqueries
that are inputs the join (either base or other leafs) are planned into independent stages. Then, the non-base leaf
inputs are all connected as broadcast inputs to the "base" stage.

Together, all of these non-base leaf inputs must not exceed the [limit on broadcast table footprint](#limits). There
is no limit on the size of the base (leftmost) input.

Only LEFT JOIN, INNER JOIN, and CROSS JOIN are supported with with `broadcast`.

Join conditions, if present, must be equalities. It is not necessary to include a join condition; for example,
`CROSS JOIN` and comma join do not require join conditions.

As an example, the following statement has a single join chain where `orders` is the base input, and `products` and
`customers` are non-base leaf inputs. The query will first read `products` and `customers`, then broadcast both to
the stage that reads `orders`. That stage loads the broadcast inputs (`products` and `customers`) in memory, and walks
through `orders` row by row. The results are then aggregated and written to the table `orders_enriched`. The broadcast
inputs (`products` and `customers`) must fall under the limit on broadcast table footprint, but the base `orders` input
can be unlimited in size.

```
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

Set `sqlJoinAlgorithm` to `sortMerge`.

Multi-stage queries can use a sort-merge join algorithm. With this algorithm, each pairwise join is planned into its own
stage with two inputs. The two inputs are partitioned and sorted using a hash partitioning on the same key. This
approach is generally less performant, but more scalable, than `broadcast`. There are various scenarios where broadcast
join would return a [`BroadcastTablesTooLarge`](#errors) error, but a sort-merge join would succeed.

There is no limit on the overall size of either input, so sort-merge is a good choice for performing a join of two large
inputs, or for performing a self-join of a large input with itself.

There is a limit on the amount of data associated with each individual key. If _both_ sides of the join exceed this
limit, the query returns a [`TooManyRowsWithSameKey`](#errors) error. If only one side exceeds the limit, the query
does not return this error.

Join conditions, if present, must be equalities. It is not necessary to include a join condition; for example,
`CROSS JOIN` and comma join do not require join conditions.

All join types are supported with `sortMerge`: LEFT, RIGHT, INNER, FULL, and CROSS.

As an example, the following statement runs using a single sort-merge join stage that receives `eventstream`
(partitioned on `user_id`) and `users` (partitioned on `id`) as inputs. There is no limit on the size of either input.

```
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

## Durable Storage

Using durable storage with your SQL-based ingestions can improve their reliability by writing intermediate files to a storage location temporarily. 

To prevent durable storage from getting filled up with temporary files in case the tasks fail to clean them up, a periodic
cleaner can be scheduled to clean the directories corresponding to which there isn't a controller task running. It utilizes
the storage connector to work upon the durable storage. The durable storage location should only be utilized to store the output
for cluster's MSQ tasks. If the location contains other files or directories, then they will get cleaned up as well.

### Enable durable storage

To enable durable storage, you need to set the following common service properties:

```
druid.msq.intermediate.storage.enable=true
druid.msq.intermediate.storage.type=s3
druid.msq.intermediate.storage.bucket=YOUR_BUCKET
druid.msq.intermediate.storage.prefix=YOUR_PREFIX
druid.msq.intermediate.storage.tempDir=/path/to/your/temp/dir
```

For detailed information about the settings related to durable storage, see [Durable storage configurations](#durable-storage-configurations).


### Use durable storage for queries

When you run a query,  include the context parameter `durableShuffleStorage` and set it to `true`. 

For queries where you want to use fault tolerance for workers,  set `faultTolerance` to `true`, which automatically sets `durableShuffleStorage` to `true`.

## Durable storage configurations

The following common service properties control how durable storage behaves:

|Parameter          |Default                                 | Description          |
|-------------------|----------------------------------------|----------------------|
|`druid.msq.intermediate.storage.bucket` | n/a | The bucket in S3 where you want to store intermediate files.  |
| `druid.msq.intermediate.storage.chunkSize` | n/a | Optional. Defines the size of each chunk to temporarily store in `druid.msq.intermediate.storage.tempDir`. The chunk size must be between 5 MiB and 5 GiB. Druid computes the chunk size automatically if no value is provided.| 
|`druid.msq.intermediate.storage.enable` | true | Required. Whether to enable durable storage for the cluster.|
| `druid.msq.intermediate.storage.maxTriesOnTransientErrors` | 10 | Optional. Defines the max number times to attempt S3 API calls to avoid failures due to transient errors. | 
|`druid.msq.intermediate.storage.type` | `s3` if your deep storage is S3 | Required. The type of storage to use. You can either set this to `local` or `s3`.  |
|`druid.msq.intermediate.storage.prefix` | n/a | S3 prefix to store intermediate stage results. Provide a unique value for the prefix. Don't share the same prefix between clusters. If the location  includes other files or directories, then they will get cleaned up as well.  |
| `druid.msq.intermediate.storage.tempDir`|  | Required. Directory path on the local disk to temporarily store intermediate stage results.  |

In addition to the common service properties, there are certain properties that you configure on the Overlord specifically to clean up intermediate files:

|Parameter          |Default                                 | Description          |
|-------------------|----------------------------------------|----------------------|
|`druid.msq.intermediate.storage.cleaner.enabled`| false | Optional. Whether durable storage cleaner should be enabled for the cluster.  |
|`druid.msq.intermediate.storage.cleaner.delaySeconds`| 86400 | Optional. The delay (in seconds) after the last run post which the durable storage cleaner would clean the outputs.  |


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
| <a name="error_InsertCannotAllocateSegment">`InsertCannotAllocateSegment`</a> | The controller task could not allocate a new segment ID due to conflict with existing segments or pending segments. Common reasons for such conflicts:<br /> <br /><ul><li>Attempting to mix different granularities in the same intervals of the same datasource.</li><li>Prior ingestions that used non-extendable shard specs.</li></ul>| `dataSource`<br /> <br />`interval`: The interval for the attempted new segment allocation. |
| <a name="error_InsertCannotBeEmpty">`InsertCannotBeEmpty`</a> | An INSERT or REPLACE query did not generate any output rows in a situation where output rows are required for success. This can happen for INSERT or REPLACE queries with `PARTITIONED BY` set to something other than `ALL` or `ALL TIME`. | `dataSource` |
| <a name="error_InsertCannotOrderByDescending">`InsertCannotOrderByDescending`</a> | An INSERT query contained a `CLUSTERED BY` expression in descending order. Druid's segment generation code only supports ascending order. | `columnName` |
| <a name="error_InsertLockPreempted">`InsertLockPreempted`</a> | An INSERT or REPLACE query was canceled by a higher-priority ingestion job, such as a real-time ingestion task. | |
| <a name="error_InsertTimeNull">`InsertTimeNull`</a> | An INSERT or REPLACE query encountered a null timestamp in the `__time` field.<br /><br />This can happen due to using an expression like `TIME_PARSE(timestamp) AS __time` with a timestamp that cannot be parsed. (TIME_PARSE returns null when it cannot parse a timestamp.) In this case, try parsing your timestamps using a different function or pattern.<br /><br />If your timestamps may genuinely be null, consider using COALESCE to provide a default value. One option is CURRENT_TIMESTAMP, which represents the start time of the job. |
| <a name="error_InsertTimeOutOfBounds">`InsertTimeOutOfBounds`</a> | A REPLACE query generated a timestamp outside the bounds of the TIMESTAMP parameter for your OVERWRITE WHERE clause.<br /> <br />To avoid this error, verify that the you specified is valid. | `interval`: time chunk interval corresponding to the out-of-bounds timestamp |
| <a name="error_InvalidNullByte">`InvalidNullByte`</a> | A string column included a null byte. Null bytes in strings are not permitted. | `column`: The column that included the null byte |
| <a name="error_QueryNotSupported">`QueryNotSupported`</a> | QueryKit could not translate the provided native query to a multi-stage query.<br /> <br />This can happen if the query uses features that aren't supported, like GROUPING SETS. | |
| <a name="error_RowTooLarge">`RowTooLarge`</a> | The query tried to process a row that was too large to write to a single frame. See the [Limits](#limits) table for specific limits on frame size. Note that the effective maximum row size is smaller than the maximum frame size due to alignment considerations during frame writing. | `maxFrameSize`: The limit on the frame size. |
| <a name="error_TaskStartTimeout">`TaskStartTimeout`</a> | Unable to launch `numTasks` tasks within `timeout` milliseconds.<br /><br />There may be insufficient available slots to start all the worker tasks simultaneously. Try splitting up your query into smaller chunks using a smaller value of [`maxNumTasks`](#context-parameters). Another option is to increase capacity. | `numTasks`: The number of tasks attempted to launch.<br /><br />`timeout`: Timeout, in milliseconds, that was exceeded. |
| <a name="error_TooManyAttemptsForJob">`TooManyAttemptsForJob`</a> | Total relaunch attempt count across all workers exceeded max relaunch attempt limit. See the [Limits](#limits) table for the specific limit. | `maxRelaunchCount`: Max number of relaunches across all the workers defined in the [Limits](#limits) section. <br /><br /> `currentRelaunchCount`: current relaunch counter for the job across all workers. <br /><br /> `taskId`: Latest task id which failed <br /> <br /> `rootErrorMessage`: Error message of the latest failed task.|
| <a name="error_TooManyAttemptsForWorker">`TooManyAttemptsForWorker`</a> | Worker exceeded maximum relaunch attempt count as defined in the [Limits](#limits) section. |`maxPerWorkerRelaunchCount`: Max number of relaunches allowed per worker as defined in the [Limits](#limits) section. <br /><br /> `workerNumber`: the worker number for which the task failed <br /><br /> `taskId`: Latest task id which failed <br /> <br /> `rootErrorMessage`: Error message of the latest failed task.|
| <a name="error_TooManyBuckets">`TooManyBuckets`</a> | Exceeded the maximum number of partition buckets for a stage (5,000 partition buckets).<br />< br />Partition buckets are created for each [`PARTITIONED BY`](#partitioned-by) time chunk for INSERT and REPLACE queries. The most common reason for this error is that your `PARTITIONED BY` is too narrow relative to your data. | `maxBuckets`: The limit on partition buckets. |
| <a name="error_TooManyInputFiles">`TooManyInputFiles`</a> | Exceeded the maximum number of input files or segments per worker (10,000 files or segments).<br /><br />If you encounter this limit, consider adding more workers, or breaking up your query into smaller queries that process fewer files or segments per query. | `numInputFiles`: The total number of input files/segments for the stage.<br /><br />`maxInputFiles`: The maximum number of input files/segments per worker per stage.<br /><br />`minNumWorker`: The minimum number of workers required for a successful run. |
| <a name="error_TooManyPartitions">`TooManyPartitions`</a> | Exceeded the maximum number of partitions for a stage (25,000 partitions).<br /><br />This can occur with INSERT or REPLACE statements that generate large numbers of segments, since each segment is associated with a partition. If you encounter this limit, consider breaking up your INSERT or REPLACE statement into smaller statements that process less data per statement. | `maxPartitions`: The limit on partitions which was exceeded |
| <a name="error_TooManyClusteredByColumns">`TooManyClusteredByColumns`</a>  | Exceeded the maximum number of clustering columns for a stage (1,500 columns).<br /><br />This can occur with `CLUSTERED BY`, `ORDER BY`, or `GROUP BY` with a large number of columns. | `numColumns`: The number of columns requested.<br /><br />`maxColumns`: The limit on columns which was exceeded.`stage`: The stage number exceeding the limit<br /><br /> |
| <a name="error_TooManyRowsWithSameKey">`TooManyRowsWithSameKey`</a> | The number of rows for a given key exceeded the maximum number of buffered bytes on both sides of a join. See the [Limits](#limits) table for the specific limit. Only occurs when `sqlJoinAlgorithm` is `sortMerge`. | `key`: The key that had a large number of rows.<br /><br />`numBytes`: Number of bytes buffered, which may include other keys.<br /><br />`maxBytes`: Maximum number of bytes buffered. |
| <a name="error_TooManyColumns">`TooManyColumns`</a> | Exceeded the maximum number of columns for a stage (2,000 columns). | `numColumns`: The number of columns requested.<br /><br />`maxColumns`: The limit on columns which was exceeded. |
| <a name="error_TooManyWarnings">`TooManyWarnings`</a> | Exceeded the maximum allowed number of warnings of a particular type. | `rootErrorCode`: The error code corresponding to the exception that exceeded the required limit. <br /><br />`maxWarnings`: Maximum number of warnings that are allowed for the corresponding `rootErrorCode`. |
| <a name="error_TooManyWorkers">`TooManyWorkers`</a> | Exceeded the maximum number of simultaneously-running workers. See the [Limits](#limits) table for more details. | `workers`: The number of simultaneously running workers that exceeded a hard or soft limit. This may be larger than the number of workers in any one stage if multiple stages are running simultaneously. <br /><br />`maxWorkers`: The hard or soft limit on workers that was exceeded. If this is lower than the hard limit (1,000 workers), then you can increase the limit by adding more memory to each task. |
| <a name="error_NotEnoughMemory">`NotEnoughMemory`</a> | Insufficient memory to launch a stage. | `suggestedServerMemory`: Suggested number of bytes of memory to allocate to a given process. <br /><br />`serverMemory`: The number of bytes of memory available to a single process.<br /><br />`usableMemory`: The number of usable bytes of memory for a single process.<br /><br />`serverWorkers`: The number of workers running in a single process.<br /><br />`serverThreads`: The number of threads in a single process. |
| <a name="error_WorkerFailed">`WorkerFailed`</a> | A worker task failed unexpectedly. | `errorMsg`<br /><br />`workerTaskId`: The ID of the worker task. |
| <a name="error_WorkerRpcFailed">`WorkerRpcFailed`</a> | A remote procedure call to a worker task failed and could not recover. | `workerTaskId`: the id of the worker task |
| <a name="error_UnknownError">`UnknownError`</a> | All other errors. | `message` |
