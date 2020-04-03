---
id: datatypes
title: "Data types and casts"
sidebar_label: "Data types"
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

<!--
  The format of the tables that describe the functions and operators
  should not be changed without updating the script create-sql-function-doc
  in web-console/script/create-sql-function-doc, because the script detects
  patterns in this markdown file and parse it to TypeScript file for web console
-->


## Supported data types

Druid natively supports five basic column types: 

- `long` &ndash; 64 bit signed int
- `float` &ndash; 32 bit float
- `double` &ndash; 64 bit float 
- `string` &ndash; UTF-8 encoded strings and string arrays
- `complex` &ndash; catch-all for more exotic data types like hyperUnique and approxHistogram columns

Timestamps (including the `__time` column) are treated by Druid as longs, with the value being the number of
milliseconds since 1970-01-01 00:00:00 UTC, not counting leap seconds. Therefore, timestamps in Druid do not carry any
timezone information, but only carry information about the exact moment in time they represent. See the
[Time functions](#time-functions) section for more information about timestamp handling.

## Casts and SQL-to-Druid SQL type mapping

The following table describes how SQL types map onto Druid types during query runtime.  

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
|OTHER|COMPLEX|none|May represent various Druid column types such as hyperUnique, approxHistogram, etc.|

Casts between two SQL types
that have the same Druid runtime type will have no effect, other than exceptions noted in the table. Casts between two
SQL types that have different Druid runtime types will generate a runtime cast in Druid. If a value cannot be properly
cast to another value, as in `CAST('foo' AS BIGINT)`, the runtime will substitute a default value. NULL values cast
to non-nullable types will also be substituted with a default value (for example, nulls cast to numbers will be
converted to zeroes). See more about NULL handling in [Null handling modes](#null-handling-modes).

For mathematical operations, Druid SQL uses integer math if all operands involved in an expression are integers.
Otherwise, Druid switches to floating point math. You can force this result by casting one of your operands
to FLOAT. At runtime, Druid may widen 32-bit floats to 64 bits for certain operators, like SUM aggregators.

Druid [multi-value string dimensions](multi-value-dimensions.html) appear in the table schema as `VARCHAR` typed,
and may be interacted with in expressions as such. Additionally, they can be treated as `ARRAY` 'like', via a handful of
special multi-value operators. Expressions against multi-value string dimensions will apply the expression to all values
of the row; however, a caveat is that aggregations on these multi-value string columns will observe the native Druid
multi-value aggregation behavior, which is equivalent to the `UNNEST` function available in many dialects.
Refer to the documentation on [multi-value string dimensions](multi-value-dimensions.html) and
[Druid expressions documentation](../misc/math-expr.html) for additional details.


## Null handling modes

By default Druid treats NULLs and empty strings interchangeably, rather than according to the SQL standard. As such,
in this mode Druid SQL only has partial support for NULLs. For example, the expressions `col IS NULL` and `col = ''` are equivalent,
and both will evaluate to true if `col` contains an empty string. Similarly, the expression `COALESCE(col1, col2)` will
return `col2` if `col1` is an empty string. While the `COUNT(*)` aggregator counts all rows, the `COUNT(expr)`
aggregator will count the number of rows where expr is neither null nor the empty string. String columns in Druid are
NULLable. Numeric columns are NOT NULL; if you query a numeric column that is not present in all segments of your Druid
datasource, then it will be treated as zero for rows from those segments.

If `druid.generic.useDefaultValueForNull` is set to `false` _system-wide, at indexing time_, data
will be stored in a manner that allows distinguishing empty strings from NULL values for string columns, and will allow NULL values to be stored for numeric columns. Druid SQL will generally operate more properly and the SQL optimizer will work best in this mode, however this does come at a cost. See the [segment documentation on SQL compatible null-handling](../design/segments.md#sql-compatible-null-handling) for more details.
