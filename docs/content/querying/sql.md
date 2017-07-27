---
layout: doc_page
---
# SQL

<div class="note caution">
Built-in SQL is an <a href="../development/experimental.html">experimental</a> feature. The API described here is
subject to change.
</div>

Druid SQL is a built-in SQL layer and an alternative to Druid's native JSON-based query language, and is powered by a
parser and planner based on [Apache Calcite](https://calcite.apache.org/). Druid SQL translates SQL into native Druid
queries on the query broker (the first node you query), which are then passed down to data nodes as native Druid
queries. Other than the (slight) overhead of translating SQL on the broker, there isn't an additional performance
penalty versus native queries.

To enable Druid SQL, make sure you have set `druid.sql.enable = true` either in your common.runtime.properties or your
broker's runtime.properties.

## Query syntax

Each Druid datasource appears as a table in the "druid" schema. This is also the default schema, so Druid datasources
can be referenced as either `druid.dataSourceName` or simply `dataSourceName`.

Identifiers like datasource and column names can optionally be quoted using double quotes. To escape a double quote
inside an identifier, use another double quote, like `"My ""very own"" identifier"`. All identifiers are case-sensitive
and no implicit case conversions are performed.

Literal strings should be quoted with single quotes, like `'foo'`. Literal strings with Unicode escapes can be written
like `U&'fo\00F6'`, where character codes in hex are prefixed by a backslash. Literal numbers can be written in forms
like `100` (denoting an integer), `100.0` (denoting a floating point value), or `1.0e5` (scientific notation). Literal
timestamps can be written like `TIMESTAMP '2000-01-01 00:00:00'`. Literal intervals, used for time arithmetic, can be
written like `INTERVAL '1' HOUR`, `INTERVAL '1 02:03' DAY TO MINUTE`, `INTERVAL '1-2' YEAR TO MONTH`, and so on.

Druid SQL supports SELECT queries with the following structure:

```
[ EXPLAIN PLAN FOR ]
[ WITH tableName [ ( column1, column2, ... ) ] AS ( query ) ]
SELECT [ ALL | DISTINCT ] { * | exprs }
FROM table
[ WHERE expr ]
[ GROUP BY exprs ]
[ HAVING expr ]
[ ORDER BY expr [ ASC | DESC ], expr [ ASC | DESC ], ... ]
[ LIMIT limit ]
```

The FROM clause refers to either a Druid datasource, like `druid.foo`, an [INFORMATION_SCHEMA table](#retrieving-metadata), a
subquery, or a common-table-expression provided in the WITH clause. If the FROM clause references a subquery or a
common-table-expression, and both levels of queries are aggregations and they cannot be combined into a single level of
aggregation, the overall query will be executed as a [nested GroupBy](groupbyquery.html#nested-groupbys).

The WHERE clause refers to columns in the FROM table, and will be translated to [native filters](filters.html). The
WHERE clause can also reference a subquery, like `WHERE col1 IN (SELECT foo FROM ...)`. Queries like this are executed
as [semi-joins](#query-execution), described below.

The GROUP BY clause refers to columns in the FROM table. Using GROUP BY, DISTINCT, or any aggregation functions will
trigger an aggregation query using one of Druid's [three native aggregation query types](#query-execution).

The HAVING clause refers to columns that are present after execution of GROUP BY. It can be used to filter on either
grouping expressions or aggregated values. It can only be used together with GROUP BY.

The ORDER BY clause refers to columns that are present after execution of GROUP BY. It can be used to order the results
based on either grouping expressions or aggregated values. The ORDER BY expression can be a column name, alias, or
ordinal position (like `ORDER BY 2` to order by the second column). ORDER BY can only be used together with GROUP BY.

The LIMIT clause can be used to limit the number of rows returned. It can be used with any query type. It is pushed down
to data nodes for queries that run with the native TopN query type, but not the native GroupBy query type. Future
versions of Druid will support pushing down limits using the native GroupBy query type as well. If you notice that
adding a limit doesn't change performance very much, then it's likely that Druid didn't push down the limit for your
query.

Add "EXPLAIN PLAN FOR" to the beginning of any query to see how it would be run as a native Druid query. In this case,
the query will not actually be executed.

### Aggregation functions

Aggregation functions can appear in the SELECT clause of any query. Any aggregator can be filtered using syntax like
`AGG(expr) FILTER(WHERE whereExpr)`. Filtered aggregators will only aggregate rows that match their filter. It's
possible for two aggregators in the same SQL query to have different filters.

Only the COUNT aggregation can accept DISTINCT.

|Function|Notes|
|--------|-----|
|`COUNT(*)`|Counts the number of rows.|
|`COUNT(DISTINCT expr)`|Counts distinct values of expr, which can be string, numeric, or hyperUnique. By default this is approximate, using a variant of [HyperLogLog](http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf). To get exact counts set "useApproximateCountDistinct" to "false". If you do this, expr must be string or numeric, since exact counts are not possible using hyperUnique columns. See also `APPROX_COUNT_DISTINCT(expr)`. In exact mode, only one distinct count per query is permitted.|
|`SUM(expr)`|Sums numbers.|
|`MIN(expr)`|Takes the minimum of numbers.|
|`MAX(expr)`|Takes the maximum of numbers.|
|`AVG(expr)`|Averages numbers.|
|`APPROX_COUNT_DISTINCT(expr)`|Counts distinct values of expr, which can be a regular column or a hyperUnique column. This is always approximate, regardless of the value of "useApproximateCountDistinct". See also `COUNT(DISTINCT expr)`.|
|`APPROX_QUANTILE(expr, probability, [resolution])`|Computes approximate quantiles on numeric or approxHistogram exprs. The "probability" should be between 0 and 1 (exclusive). The "resolution" is the number of centroids to use for the computation. Higher resolutions will give more precise results but also have higher overhead. If not provided, the default resolution is 50. The [approximate histogram extension](../development/extensions-core/approximate-histograms.html) must be loaded to use this function.|

### Numeric functions

Numeric functions will return 64 bit integers or 64 bit floats, depending on their inputs.

|Function|Notes|
|--------|-----|
|`ABS(expr)`|Absolute value.|
|`CEIL(expr)`|Ceiling.|
|`EXP(expr)`|e to the power of expr.|
|`FLOOR(expr)`|Floor.|
|`LN(expr)`|Logarithm (base e).|
|`LOG10(expr)`|Logarithm (base 10).|
|`POW(expr, power)`|expr to a power.|
|`SQRT(expr)`|Square root.|
|`x + y`|Addition.|
|`x - y`|Subtraction.|
|`x * y`|Multiplication.|
|`x / y`|Division.|
|`x % y`|Mod.|

### String functions

String functions accept strings, and return a type appropriate to the function.

|Function|Notes|
|--------|-----|
|`x \|\| y`|Concat strings x and y.|
|`CHARACTER_LENGTH(expr)`|Length of expr in UTF-16 code units.|
|`LOOKUP(expr, lookupName)`|Look up expr in a registered [query-time lookup table](lookups.html).|
|`LOWER(expr)`|Returns expr in all lowercase.|
|`REGEXP_EXTRACT(expr, pattern, [index])`|Apply regular expression pattern and extract a capture group, or null if there is no match. If index is unspecified or zero, returns the substring that matched the pattern.|
|`REPLACE(expr, pattern, replacement)`|Replaces pattern with replacement in expr, and returns the result.|
|`SUBSTRING(expr, index, [length])`|Returns a substring of expr starting at index, with a max length, both measured in UTF-16 code units.|
|`TRIM(expr)`|Returns expr with leading and trailing whitespace removed.|
|`UPPER(expr)`|Returns expr in all uppercase.|

### Time functions

Time functions can be used with Druid's `__time` column, with any column storing millisecond timestamps through use
of the `MILLIS_TO_TIMESTAMP` function, or with any column storing string timestamps through use of the `TIME_PARSE`
function. By default, time operations use the UTC time zone. You can change the time zone by setting the connection
context parameter "sqlTimeZone" to the name of another time zone, like "America/Los_Angeles", or to an offset like
"-08:00". If you need to mix multiple time zones in the same query, or if you need to use a time zone other than
the connection time zone, some functions also accept time zones as parameters. These parameters always take precedence
over the connection time zone.

|Function|Notes|
|--------|-----|
|`CURRENT_TIMESTAMP`|Current timestamp in the connection's time zone.|
|`CURRENT_DATE`|Current date in the connection's time zone.|
|`TIME_FLOOR(<timestamp_expr>, <period>, [<origin>, [<timezone>]])`|Rounds down a timestamp, returning it as a new timestamp. Period can be any ISO8601 period, like P3M (quarters) or PT12H (half-days). The time zone, if provided, should be a time zone name like "America/Los_Angeles" or offset like "-08:00". This function is similar to `FLOOR` but is more flexible.|
|`TIME_SHIFT(<timestamp_expr>, <period>, <step>, [<timezone>])`|Shifts a timestamp by a period (step times), returning it as a new timestamp. Period can be any ISO8601 period. Step may be negative. The time zone, if provided, should be a time zone name like "America/Los_Angeles" or offset like "-08:00".|
|`TIME_EXTRACT(<timestamp_expr>, [<unit>, [<timezone>]])`|Extracts a time part from expr, returning it as a number. Unit can be EPOCH, SECOND, MINUTE, HOUR, DAY (day of month), DOW (day of week), DOY (day of year), WEEK (week of [week year](https://en.wikipedia.org/wiki/ISO_week_date)), MONTH (1 through 12), QUARTER (1 through 4), or YEAR. The time zone, if provided, should be a time zone name like "America/Los_Angeles" or offset like "-08:00". This function is similar to `EXTRACT` but is more flexible. Unit and time zone must be literals, and must be provided quoted, like `TIME_EXTRACT(__time, 'HOUR')` or `TIME_EXTRACT(__time, 'HOUR', 'America/Los_Angeles')`.|
|`TIME_PARSE(<string_expr>, [<pattern>, [<timezone>]])`|Parses a string into a timestamp using a given [Joda DateTimeFormat pattern](http://www.joda.org/joda-time/apidocs/org/joda/time/format/DateTimeFormat.html), or ISO8601 (e.g. `2000-01-02T03:04:05Z`) if the pattern is not provided. The time zone, if provided, should be a time zone name like "America/Los_Angeles" or offset like "-08:00", and will be used as the time zone for strings that do not include a time zone offset. Pattern and time zone must be literals. Strings that cannot be parsed as timestamps will be returned as NULL.|
|`TIME_FORMAT(<timestamp_expr>, [<pattern>, [<timezone>]])`|Formats a timestamp as a string with a given [Joda DateTimeFormat pattern](http://www.joda.org/joda-time/apidocs/org/joda/time/format/DateTimeFormat.html), or ISO8601 (e.g. `2000-01-02T03:04:05Z`) if the pattern is not provided. The time zone, if provided, should be a time zone name like "America/Los_Angeles" or offset like "-08:00". Pattern and time zone must be literals.|
|`MILLIS_TO_TIMESTAMP(millis_expr)`|Converts a number of milliseconds since the epoch into a timestamp.|
|`TIMESTAMP_TO_MILLIS(timestamp_expr)`|Converts a timestamp into a number of milliseconds since the epoch.|
|`EXTRACT(<unit> FROM timestamp_expr)`|Extracts a time part from expr, returning it as a number. Unit can be EPOCH, SECOND, MINUTE, HOUR, DAY (day of month), DOW (day of week), DOY (day of year), WEEK (week of year), MONTH, QUARTER, or YEAR. Units must be provided unquoted, like `EXTRACT(HOUR FROM __time)`.|
|`FLOOR(timestamp_expr TO <unit>)`|Rounds down a timestamp, returning it as a new timestamp. Unit can be SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, or YEAR.|
|`CEIL(timestamp_expr TO <unit>)`|Rounds up a timestamp, returning it as a new timestamp. Unit can be SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, or YEAR.|
|`timestamp_expr { + \| - } <interval_expr>`|Add or subtract an amount of time from a timestamp. interval_expr can include interval literals like `INTERVAL '2' HOUR`, and may include interval arithmetic as well. This operator treats days as uniformly 86400 seconds long, and does not take into account daylight savings time. To account for daylight savings time, use TIME_SHIFT instead.|

### Comparison operators

|Function|Notes|
|--------|-----|
|`x = y`|Equals.|
|`x <> y`|Not-equals.|
|`x > y`|Greater than.|
|`x >= y`|Greater than or equal to.|
|`x < y`|Less than.|
|`x <= y`|Less than or equal to.|
|`x LIKE pattern [ESCAPE esc]`|True if x matches a SQL LIKE pattern (with an optional escape).|
|`x NOT LIKE pattern [ESCAPE esc]`|True if x does not match a SQL LIKE pattern (with an optional escape).|
|`x IS NULL`|True if x is NULL or empty string.|
|`x IS NOT NULL`|True if x is neither NULL nor empty string.|
|`x IS TRUE`|True if x is true.|
|`x IS NOT TRUE`|True if x is not true.|
|`x IS FALSE`|True if x is false.|
|`x IS NOT FALSE`|True if x is not false.|
|`x IN (values)`|True if x is one of the listed values.|
|`x NOT IN (values)`|True if x is not one of the listed values.|
|`x IN (subquery)`|True if x is returned by the subquery. See [Syntax and execution](#syntax-and-execution) above for details about how Druid SQL handles `IN (subquery)`.|
|`x NOT IN (subquery)`|True if x is not returned by the subquery. See [Syntax and execution](#syntax-and-execution) for details about how Druid SQL handles `IN (subquery)`.|
|`x AND y`|Boolean AND.|
|`x OR y`|Boolean OR.|
|`NOT x`|Boolean NOT.|

### Other functions

|Function|Notes|
|--------|-----|
|`CAST(value AS TYPE)`|Cast value to another type. See [Data types and casts](#data-types-and-casts) for details about how Druid SQL handles CAST.|
|`CASE expr WHEN value1 THEN result1 \[ WHEN value2 THEN result2 ... \] \[ ELSE resultN \] END`|Simple CASE.|
|`CASE WHEN boolean_expr1 THEN result1 \[ WHEN boolean_expr2 THEN result2 ... \] \[ ELSE resultN \] END`|Searched CASE.|
|`NULLIF(value1, value2)`|Returns NULL if value1 and value2 match, else returns value1.|
|`COALESCE(value1, value2, ...)`|Returns the first value that is neither NULL nor empty string.|

### Unsupported features

Druid does not support all SQL features, including:

- OVER clauses, and analytic functions such as `LAG` and `LEAD`.
- JOIN clauses, other than semi-joins as described above.
- OFFSET clauses.
- DDL and DML.

Additionally, some Druid features are not supported by the SQL language. Some unsupported Druid features include:

- [Multi-value dimensions](multi-value-dimensions.html).
- [DataSketches aggregators](../development/extensions-core/datasketches-aggregators.html).

## Data types and casts

Druid natively supports five basic column types: "long" (64 bit signed int), "float" (32 bit float), "double" (64 bit
float) "string" (UTF-8 encoded strings), and "complex" (catch-all for more exotic data types like hyperUnique and
approxHistogram columns). Timestamps (including the `__time` column) are stored as longs, with the value being the
number of milliseconds since 1 January 1970 UTC.

At runtime, Druid may widen 32-bit floats to 64-bit for certain operators, like SUM aggregators. The reverse will not
happen: 64-bit floats are not be narrowed to 32-bit.

Druid generally treats NULLs and empty strings interchangeably, rather than according to the SQL standard. As such,
Druid SQL only has partial support for NULLs. For example, the expressions `col IS NULL` and `col = ''` are equivalent,
and both will evaluate to true if `col` contains an empty string. Similarly, the expression `COALESCE(col1, col2)` will
return `col2` if `col1` is an empty string. While the `COUNT(*)` aggregator counts all rows, the `COUNT(expr)`
aggregator will count the number of rows where expr is neither null nor the empty string. String columns in Druid are
NULLable. Numeric columns are NOT NULL; if you query a numeric column that is not present in all segments of your Druid
datasource, then it will be treated as zero for rows from those segments.

For mathematical operations, Druid SQL will use integer math if all operands involved in an expression are integers.
Otherwise, Druid will switch to floating point math. You can force this to happen by casting one of your operands
to FLOAT.

The following table describes how SQL types map onto Druid types during query runtime. Casts between two SQL types
that have the same Druid runtime type will have no effect, other than exceptions noted in the table. Casts between two
SQL types that have different Druid runtime types will generate a runtime cast in Druid. If a value cannot be properly
cast to another value, as in `CAST('foo' AS BIGINT)`, the runtime will substitute a default value. NULL values cast
to non-nullable types will also be substitued with a default value (for example, nulls cast to numbers will be
converted to zeroes).

|SQL type|Druid runtime type|Default value|Notes|
|--------|------------------|-------------|-----|
|CHAR|STRING|`''`||
|VARCHAR|STRING|`''`|Druid STRING columns are reported as VARCHAR|
|DECIMAL|DOUBLE|`0.0`|DECIMAL uses floating point, not fixed point math|
|FLOAT|FLOAT|`0.0`|Druid FLOAT columns are reported as FLOAT|
|REAL|DOUBLE|`0.0`||
|DOUBLE|DOUBLE|`0.0`|Druid DOUBLE columns are reported as DOUBLE|
|BOOLEAN|LONG|`false`||
|TINYINT|LONG|`0`||
|SMALLINT|LONG|`0`||
|INTEGER|LONG|`0`||
|BIGINT|LONG|`0`|Druid LONG columns (except `__time`) are reported as BIGINT|
|TIMESTAMP|LONG|`0`, meaning 1970-01-01 00:00:00 UTC|Druid's `__time` column is reported as TIMESTAMP. Casts between string and timestamp types assume standard SQL formatting, e.g. `2000-01-02 03:04:05`, _not_ ISO8601 formatting. For handling other formats, use one of the [time functions](#time-functions)|
|DATE|LONG|`0`, meaning 1970-01-01|Casting TIMESTAMP to DATE rounds down the timestamp to the nearest day. Casts between string and date types assume standard SQL formatting, e.g. `2000-01-02`. For handling other formats, use one of the [time functions](#time-functions)|
|OTHER|COMPLEX|none|May represent various Druid column types such as hyperUnique, approxHistogram, etc|

## Query execution

Queries without aggregations will use Druid's [Select](select-query.html) native query type.

Aggregation queries (using GROUP BY, DISTINCT, or any aggregation functions) will use one of Druid's three native
aggregation query types. Two (Timeseries and TopN) are specialized for specific types of aggregations, whereas the other
(GroupBy) is general-purpose.

- [Timeseries](timeseriesquery.html) is used for queries that GROUP BY `FLOOR(__time TO <unit>)` or `TIME_FLOOR(__time,
period)`, have no other grouping expressions, no HAVING or LIMIT clauses, no nesting, and either no ORDER BY, or an
ORDER BY that orders by same expression as present in GROUP BY. It also uses Timeseries for "grand total" queries that
have aggregation functions but no GROUP BY. This query type takes advantage of the fact that Druid segments are sorted
by time.

- [TopN](topnquery.html) is used by default for queries that group by a single expression, do have ORDER BY and LIMIT
clauses, do not have HAVING clauses, and are not nested. However, the TopN query type will deliver approximate ranking
and results in some cases; if you want to avoid this, set "useApproximateTopN" to "false". TopN results are always
computed in memory. See the TopN documentation for more details.

- [GroupBy](groupbyquery.html) is used for all other aggregations, including any nested aggregation queries. Druid's
GroupBy is a traditional aggregation engine: it delivers exact results and rankings and supports a wide variety of
features. GroupBy aggregates in memory if it can, but it may spill to disk if it doesn't have enough memory to complete
your query. Results are streamed back from data nodes through the broker if you ORDER BY the same expressions in your
GROUP BY clause, or if you don't have an ORDER BY at all. If your query has an ORDER BY referencing expressions that
don't appear in the GROUP BY clause (like aggregation functions) then the broker will materialize a list of results in
memory, up to a max of your LIMIT, if any. See the GroupBy documentation for details about tuning performance and memory
use.

If your query does nested aggregations (an aggregation subquery in your FROM clause) then Druid will execute it as a
[nested GroupBy](groupbyquery.html#nested-groupbys). In nested GroupBys, the innermost aggregation is distributed, but
all outer aggregations beyond that take place locally on the query broker.

Semi-join queries containing WHERE clauses like `col IN (SELECT expr FROM ...)` are executed with a special process. The
broker will first translate the subquery into a GroupBy to find distinct values of `expr`. Then, the broker will rewrite
the subquery to a literal filter, like `col IN (val1, val2, ...)` and run the outer query. The configuration parameter
druid.sql.planner.maxSemiJoinRowsInMemory controls the maximum number of values that will be materialized for this kind
of plan.

For all native query types, filters on the `__time` column will be translated into top-level query "intervals" whenever
possible, which allows Druid to use its global time index to quickly prune the set of data that must be scanned. In
addition, Druid will use indexes local to each data node to further speed up WHERE evaluation. This can typically be
done for filters that involve boolean combinations of references to and functions of single columns, like
`WHERE col1 = 'a' AND col2 = 'b'`, but not `WHERE col1 = col2`.

### Approximate algorithms

Druid SQL will use approximate algorithms in some situations:

- The `COUNT(DISTINCT col)` aggregation functions by default uses a variant of
[HyperLogLog](http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf), a fast approximate distinct counting
algorithm. Druid SQL will switch to exact distinct counts if you set "useApproximateCountDistinct" to "false", either
through query context or through broker configuration.
- GROUP BY queries over a single column with ORDER BY and LIMIT may be executed using the TopN engine, which uses an
approximate algorithm. Druid SQL will switch to an exact grouping algorithm if you set "useApproximateTopN" to "false",
either through query context or through broker configuration.
- The APPROX_COUNT_DISTINCT and APPROX_QUANTILE aggregation functions always use approximate algorithms, regardless
of configuration.

## Client APIs

### JSON over HTTP

You can make Druid SQL queries using JSON over HTTP by posting to the endpoint `/druid/v2/sql/`. The request should
be a JSON object with a "query" field, like `{"query" : "SELECT COUNT(*) FROM data_source WHERE foo = 'bar'"}`. You can
use _curl_ to send these queries from the command-line:

```bash
$ cat query.json
{"query":"SELECT COUNT(*) FROM data_source"}

$ curl -XPOST -H'Content-Type: application/json' http://BROKER:8082/druid/v2/sql/ -d @query.json
[{"EXPR$0":24433}]
```

You can also provide [connection context parameters](#connection-context) by adding a "context" map, like:

```json
{
  "query" : "SELECT COUNT(*) FROM data_source WHERE foo = 'bar' AND __time > TIMESTAMP '2000-01-01 00:00:00'",
  "context" : {
    "sqlTimeZone" : "America/Los_Angeles"
  }
}
```

Metadata is available over the HTTP API by querying the ["INFORMATION_SCHEMA" tables](#retrieving-metadata).

### JDBC

You can make Druid SQL queries using the [Avatica JDBC driver](https://calcite.apache.org/avatica/downloads/). Once
you've downloaded the Avatica client jar, add it to your classpath and use the connect string
`jdbc:avatica:remote:url=http://BROKER:8082/druid/v2/sql/avatica/`.

Example code:

```java
// Connect to /druid/v2/sql/avatica/ on your broker.
String url = "jdbc:avatica:remote:url=http://localhost:8082/druid/v2/sql/avatica/";

// Set any connection context parameters you need here (see "Connection context" below).
// Or leave empty for default behavior.
Properties connectionProperties = new Properties();

try (Connection connection = DriverManager.getConnection(url, connectionProperties)) {
  try (
      final Statement statement = client.createStatement();
      final ResultSet resultSet = statement.executeQuery(query)
  ) {
    while (resultSet.next()) {
      // Do something
    }
  }
}
```

Table metadata is available over JDBC using `connection.getMetaData()` or by querying the
["INFORMATION_SCHEMA" tables](#retrieving-metadata). Parameterized queries (using `?` or other placeholders) don't work properly,
so avoid those.

Druid's JDBC server does not share connection state between brokers. This means that if you're using JDBC and have
multiple Druid brokers, you should either connect to a specific broker, or use a load balancer with sticky sessions
enabled.

### Connection context

Druid SQL supports setting connection parameters on the client. The parameters in the table below affect SQL planning.
All other context parameters you provide will be attached to Druid queries and can affect how they run. See
[Query context](query-context.html) for details on the possible options.

|Parameter|Description|Default value|
|---------|-----------|-------------|
|`sqlTimeZone`|Sets the time zone for this connection, which will affect how time functions and timestamp literals behave. Should be a time zone name like "America/Los_Angeles" or offset like "-08:00".|UTC|
|`useApproximateCountDistinct`|Whether to use an approximate cardinalty algorithm for `COUNT(DISTINCT foo)`.|druid.sql.planner.useApproximateCountDistinct on the broker|
|`useApproximateTopN`|Whether to use approximate [TopN queries](topnquery.html) when a SQL query could be expressed as such. If false, exact [GroupBy queries](groupbyquery.html) will be used instead.|druid.sql.planner.useApproximateTopN on the broker|
|`useFallback`|Whether to evaluate operations on the broker when they cannot be expressed as Druid queries. This option is not recommended for production since it can generate unscalable query plans. If false, SQL queries that cannot be translated to Druid queries will fail.|druid.sql.planner.useFallback on the broker|

Connection context can be specified as JDBC connection properties or as a "context" object in the JSON API.

### Retrieving metadata

Druid brokers infer table and column metadata for each dataSource from segments loaded in the cluster, and use this to
plan SQL queries. This metadata is cached on broker startup and also updated periodically in the background through
[SegmentMetadata queries](segmentmetadataquery.html). Background metadata refreshing is triggered by
segments entering and exiting the cluster, and can also be throttled through configuration.

You can access table and column metadata through JDBC using `connection.getMetaData()`, or through the
INFORMATION_SCHEMA tables described below. For example, to retrieve metadata for the Druid
datasource "foo", use the query:

```sql
SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'druid' AND TABLE_NAME = 'foo'
```

### SCHEMATA table

|Column|Notes|
|------|-----|
|CATALOG_NAME|Unused|
|SCHEMA_NAME||
|SCHEMA_OWNER|Unused|
|DEFAULT_CHARACTER_SET_CATALOG|Unused|
|DEFAULT_CHARACTER_SET_SCHEMA|Unused|
|DEFAULT_CHARACTER_SET_NAME|Unused|
|SQL_PATH|Unused|

### TABLES table

|Column|Notes|
|------|-----|
|TABLE_CATALOG|Unused|
|TABLE_SCHEMA||
|TABLE_NAME||
|TABLE_TYPE|"TABLE" or "SYSTEM_TABLE"|

### COLUMNS table

|Column|Notes|
|------|-----|
|TABLE_CATALOG|Unused|
|TABLE_SCHEMA||
|TABLE_NAME||
|COLUMN_NAME||
|ORDINAL_POSITION||
|COLUMN_DEFAULT|Unused|
|IS_NULLABLE||
|DATA_TYPE||
|CHARACTER_MAXIMUM_LENGTH|Unused|
|CHARACTER_OCTET_LENGTH|Unused|
|NUMERIC_PRECISION||
|NUMERIC_PRECISION_RADIX||
|NUMERIC_SCALE||
|DATETIME_PRECISION||
|CHARACTER_SET_NAME||
|COLLATION_NAME||
|JDBC_TYPE|Type code from java.sql.Types (Druid extension)|

## Server configuration

The Druid SQL server is configured through the following properties on the broker.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.sql.enable`|Whether to enable SQL at all, including background metadata fetching. If false, this overrides all other SQL-related properties and disables SQL metadata, serving, and planning completely.|false|
|`druid.sql.avatica.enable`|Whether to enable JDBC querying at `/druid/v2/sql/avatica/`.|true|
|`druid.sql.avatica.maxConnections`|Maximum number of open connections for the Avatica server. These are not HTTP connections, but are logical client connections that may span multiple HTTP connections.|50|
|`druid.sql.avatica.maxRowsPerFrame`|Maximum number of rows to return in a single JDBC frame. Setting this property to -1 indicates that no row limit should be applied. Clients can optionally specify a row limit in their requests; if a client specifies a row limit, the lesser value of the client-provided limit and `maxRowsPerFrame` will be used.|100,000|
|`druid.sql.avatica.maxStatementsPerConnection`|Maximum number of simultaneous open statements per Avatica client connection.|1|
|`druid.sql.avatica.connectionIdleTimeout`|Avatica client connection idle timeout.|PT5M|
|`druid.sql.http.enable`|Whether to enable JSON over HTTP querying at `/druid/v2/sql/`.|true|
|`druid.sql.planner.maxQueryCount`|Maximum number of queries to issue, including nested queries. Set to 1 to disable sub-queries, or set to 0 for unlimited.|8|
|`druid.sql.planner.maxSemiJoinRowsInMemory`|Maximum number of rows to keep in memory for executing two-stage semi-join queries like `SELECT * FROM Employee WHERE DeptName IN (SELECT DeptName FROM Dept)`.|100000|
|`druid.sql.planner.maxTopNLimit`|Maximum threshold for a [TopN query](../querying/topnquery.html). Higher limits will be planned as [GroupBy queries](../querying/groupbyquery.html) instead.|100000|
|`druid.sql.planner.metadataRefreshPeriod`|Throttle for metadata refreshes.|PT1M|
|`druid.sql.planner.selectPageSize`|Page size threshold for [Select queries](../querying/select-query.html). Select queries for larger resultsets will be issued back-to-back using pagination.|1000|
|`druid.sql.planner.useApproximateCountDistinct`|Whether to use an approximate cardinalty algorithm for `COUNT(DISTINCT foo)`.|true|
|`druid.sql.planner.useApproximateTopN`|Whether to use approximate [TopN queries](../querying/topnquery.html) when a SQL query could be expressed as such. If false, exact [GroupBy queries](../querying/groupbyquery.html) will be used instead.|true|
|`druid.sql.planner.useFallback`|Whether to evaluate operations on the broker when they cannot be expressed as Druid queries. This option is not recommended for production since it can generate unscalable query plans. If false, SQL queries that cannot be translated to Druid queries will fail.|false|
