/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// This file is auto generated and should not be modified

// prettier-ignore
exports.SQL_DATA_TYPES = [
  {
    "name": "CHAR",
    "description": "Druid runtime type: STRING"
  },
  {
    "name": "VARCHAR",
    "description": "Druid STRING columns are reported as VARCHAR"
  },
  {
    "name": "DECIMAL",
    "description": "DECIMAL uses floating point, not fixed point math"
  },
  {
    "name": "FLOAT",
    "description": "Druid FLOAT columns are reported as FLOAT"
  },
  {
    "name": "REAL",
    "description": "Druid runtime type: DOUBLE"
  },
  {
    "name": "DOUBLE",
    "description": "Druid DOUBLE columns are reported as DOUBLE"
  },
  {
    "name": "BOOLEAN",
    "description": "Druid runtime type: LONG"
  },
  {
    "name": "TINYINT",
    "description": "Druid runtime type: LONG"
  },
  {
    "name": "SMALLINT",
    "description": "Druid runtime type: LONG"
  },
  {
    "name": "INTEGER",
    "description": "Druid runtime type: LONG"
  },
  {
    "name": "BIGINT",
    "description": "Druid LONG columns (except `__time`) are reported as BIGINT"
  },
  {
    "name": "TIMESTAMP",
    "description": "Druid's `__time` column is reported as TIMESTAMP. Casts between string and timestamp types assume standard SQL formatting, e.g. `2000-01-02 03:04:05`, _not_ ISO8601 formatting. For handling other formats, use one of the [time functions](#time-functions)"
  },
  {
    "name": "DATE",
    "description": "Casting TIMESTAMP to DATE rounds down the timestamp to the nearest day. Casts between string and date types assume standard SQL formatting, e.g. `2000-01-02`. For handling other formats, use one of the [time functions](#time-functions)"
  },
  {
    "name": "OTHER",
    "description": "May represent various Druid column types such as hyperUnique, approxHistogram, etc"
  }
];

// prettier-ignore
exports.SQL_FUNCTIONS = [
  {
    "name": "COUNT",
    "arguments": "(*)",
    "description": "Counts the number of rows."
  },
  {
    "name": "COUNT",
    "arguments": "(DISTINCT expr)",
    "description": "Counts distinct values of expr, which can be string, numeric, or hyperUnique. By default this is approximate, using a variant of [HyperLogLog](http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf). To get exact counts set \"useApproximateCountDistinct\" to \"false\". If you do this, expr must be string or numeric, since exact counts are not possible using hyperUnique columns. See also `APPROX_COUNT_DISTINCT(expr)`. In exact mode, only one distinct count per query is permitted."
  },
  {
    "name": "SUM",
    "arguments": "(expr)",
    "description": "Sums numbers."
  },
  {
    "name": "MIN",
    "arguments": "(expr)",
    "description": "Takes the minimum of numbers."
  },
  {
    "name": "MAX",
    "arguments": "(expr)",
    "description": "Takes the maximum of numbers."
  },
  {
    "name": "AVG",
    "arguments": "(expr)",
    "description": "Averages numbers."
  },
  {
    "name": "APPROX_COUNT_DISTINCT",
    "arguments": "(expr)",
    "description": "Counts distinct values of expr, which can be a regular column or a hyperUnique column. This is always approximate, regardless of the value of \"useApproximateCountDistinct\". This uses Druid's built-in \"cardinality\" or \"hyperUnique\" aggregators. See also `COUNT(DISTINCT expr)`."
  },
  {
    "name": "APPROX_COUNT_DISTINCT_DS_HLL",
    "arguments": "(expr, [lgK, tgtHllType])",
    "description": "Counts distinct values of expr, which can be a regular column or an [HLL sketch](../development/extensions-core/datasketches-hll.html) column. The `lgK` and `tgtHllType` parameters are described in the HLL sketch documentation. This is always approximate, regardless of the value of \"useApproximateCountDistinct\". See also `COUNT(DISTINCT expr)`. The [DataSketches extension](../development/extensions-core/datasketches-extension.html) must be loaded to use this function."
  },
  {
    "name": "APPROX_COUNT_DISTINCT_DS_THETA",
    "arguments": "(expr, [size])",
    "description": "Counts distinct values of expr, which can be a regular column or a [Theta sketch](../development/extensions-core/datasketches-theta.html) column. The `size` parameter is described in the Theta sketch documentation. This is always approximate, regardless of the value of \"useApproximateCountDistinct\". See also `COUNT(DISTINCT expr)`. The [DataSketches extension](../development/extensions-core/datasketches-extension.html) must be loaded to use this function."
  },
  {
    "name": "APPROX_QUANTILE",
    "arguments": "(expr, probability, [resolution])",
    "description": "Computes approximate quantiles on numeric or [approxHistogram](../development/extensions-core/approximate-histograms.html#approximate-histogram-aggregator) exprs. The \"probability\" should be between 0 and 1 (exclusive). The \"resolution\" is the number of centroids to use for the computation. Higher resolutions will give more precise results but also have higher overhead. If not provided, the default resolution is 50. The [approximate histogram extension](../development/extensions-core/approximate-histograms.html) must be loaded to use this function."
  },
  {
    "name": "APPROX_QUANTILE_DS",
    "arguments": "(expr, probability, [k])",
    "description": "Computes approximate quantiles on numeric or [Quantiles sketch](../development/extensions-core/datasketches-quantiles.html) exprs. The \"probability\" should be between 0 and 1 (exclusive). The `k` parameter is described in the Quantiles sketch documentation. The [DataSketches extension](../development/extensions-core/datasketches-extension.html) must be loaded to use this function."
  },
  {
    "name": "APPROX_QUANTILE_FIXED_BUCKETS",
    "arguments": "(expr, probability, numBuckets, lowerLimit, upperLimit, [outlierHandlingMode])",
    "description": "Computes approximate quantiles on numeric or [fixed buckets histogram](../development/extensions-core/approximate-histograms.html#fixed-buckets-histogram) exprs. The \"probability\" should be between 0 and 1 (exclusive). The `numBuckets`, `lowerLimit`, `upperLimit`, and `outlierHandlingMode` parameters are described in the fixed buckets histogram documentation. The [approximate histogram extension](../development/extensions-core/approximate-histograms.html) must be loaded to use this function."
  },
  {
    "name": "BLOOM_FILTER",
    "arguments": "(expr, numEntries)",
    "description": "Computes a bloom filter from values produced by `expr`, with `numEntries` maximum number of distinct values before false positive rate increases. See [bloom filter extension](../development/extensions-core/bloom-filter.html) documentation for additional details."
  },
  {
    "name": "TDIGEST_QUANTILE",
    "arguments": "(expr, quantileFraction, [compression])",
    "description": "Builds a T-Digest sketch on values produced by `expr` and returns the value for the quantile. Compression parameter (default value 100) determines the accuracy and size of the sketch. Higher compression means higher accuracy but more space to store sketches. See [t-digest extension](../development/extensions-contrib/tdigestsketch-quantiles.html) documentation for additional details."
  },
  {
    "name": "TDIGEST_GENERATE_SKETCH",
    "arguments": "(expr, [compression])",
    "description": "Builds a T-Digest sketch on values produced by `expr`. Compression parameter (default value 100) determines the accuracy and size of the sketch Higher compression means higher accuracy but more space to store sketches. See [t-digest extension](../development/extensions-contrib/tdigestsketch-quantiles.html) documentation for additional details."
  },
  {
    "name": "VAR_POP",
    "arguments": "(expr)",
    "description": "Computes variance population of `expr`. See [stats extension](../development/extensions-core/stats.html) documentation for additional details."
  },
  {
    "name": "VAR_SAMP",
    "arguments": "(expr)",
    "description": "Computes variance sample of `expr`. See [stats extension](../development/extensions-core/stats.html) documentation for additional details."
  },
  {
    "name": "VARIANCE",
    "arguments": "(expr)",
    "description": "Computes variance sample of `expr`. See [stats extension](../development/extensions-core/stats.html) documentation for additional details."
  },
  {
    "name": "STDDEV_POP",
    "arguments": "(expr)",
    "description": "Computes standard deviation population of `expr`. See [stats extension](../development/extensions-core/stats.html) documentation for additional details."
  },
  {
    "name": "STDDEV_SAMP",
    "arguments": "(expr)",
    "description": "Computes standard deviation sample of `expr`. See [stats extension](../development/extensions-core/stats.html) documentation for additional details."
  },
  {
    "name": "STDDEV",
    "arguments": "(expr)",
    "description": "Computes standard deviation sample of `expr`. See [stats extension](../development/extensions-core/stats.html) documentation for additional details."
  },
  {
    "name": "ABS",
    "arguments": "(expr)",
    "description": "Absolute value."
  },
  {
    "name": "CEIL",
    "arguments": "(expr)",
    "description": "Ceiling."
  },
  {
    "name": "EXP",
    "arguments": "(expr)",
    "description": "e to the power of expr."
  },
  {
    "name": "FLOOR",
    "arguments": "(expr)",
    "description": "Floor."
  },
  {
    "name": "LN",
    "arguments": "(expr)",
    "description": "Logarithm (base e)."
  },
  {
    "name": "LOG10",
    "arguments": "(expr)",
    "description": "Logarithm (base 10)."
  },
  {
    "name": "POWER",
    "arguments": "(expr, power)",
    "description": "expr to a power."
  },
  {
    "name": "SQRT",
    "arguments": "(expr)",
    "description": "Square root."
  },
  {
    "name": "TRUNCATE",
    "arguments": "(expr[, digits])",
    "description": "Truncate expr to a specific number of decimal digits. If digits is negative, then this truncates that many places to the left of the decimal point. Digits defaults to zero if not specified."
  },
  {
    "name": "TRUNC",
    "arguments": "(expr[, digits])",
    "description": "Synonym for `TRUNCATE`."
  },
  {
    "name": "ROUND",
    "arguments": "(expr[, digits])",
    "description": "`ROUND(x, y)` would return the value of the x rounded to the y decimal places. While x can be an integer or floating-point number, y must be an integer. The type of the return value is specified by that of x. y defaults to 0 if omitted. When y is negative, x is rounded on the left side of the y decimal points."
  },
  {
    "name": "MOD",
    "arguments": "(x, y)",
    "description": "Modulo (remainder of x divided by y)."
  },
  {
    "name": "SIN",
    "arguments": "(expr)",
    "description": "Trigonometric sine of an angle expr."
  },
  {
    "name": "COS",
    "arguments": "(expr)",
    "description": "Trigonometric cosine of an angle expr."
  },
  {
    "name": "TAN",
    "arguments": "(expr)",
    "description": "Trigonometric tangent of an angle expr."
  },
  {
    "name": "COT",
    "arguments": "(expr)",
    "description": "Trigonometric cotangent of an angle expr."
  },
  {
    "name": "ASIN",
    "arguments": "(expr)",
    "description": "Arc sine of expr."
  },
  {
    "name": "ACOS",
    "arguments": "(expr)",
    "description": "Arc cosine of expr."
  },
  {
    "name": "ATAN",
    "arguments": "(expr)",
    "description": "Arc tangent of expr."
  },
  {
    "name": "ATAN2",
    "arguments": "(y, x)",
    "description": "Angle theta from the conversion of rectangular coordinates (x, y) to polar * coordinates (r, theta)."
  },
  {
    "name": "DEGREES",
    "arguments": "(expr)",
    "description": "Converts an angle measured in radians to an approximately equivalent angle measured in degrees"
  },
  {
    "name": "RADIANS",
    "arguments": "(expr)",
    "description": "Converts an angle measured in degrees to an approximately equivalent angle measured in radians"
  },
  {
    "name": "CONCAT",
    "arguments": "(expr, expr...)",
    "description": "Concats a list of expressions."
  },
  {
    "name": "TEXTCAT",
    "arguments": "(expr, expr)",
    "description": "Two argument version of CONCAT."
  },
  {
    "name": "STRING_FORMAT",
    "arguments": "(pattern[, args...])",
    "description": "Returns a string formatted in the manner of Java's [String.format](https://docs.oracle.com/javase/8/docs/api/java/lang/String.html#format-java.lang.String-java.lang.Object...-)."
  },
  {
    "name": "LENGTH",
    "arguments": "(expr)",
    "description": "Length of expr in UTF-16 code units."
  },
  {
    "name": "CHAR_LENGTH",
    "arguments": "(expr)",
    "description": "Synonym for `LENGTH`."
  },
  {
    "name": "CHARACTER_LENGTH",
    "arguments": "(expr)",
    "description": "Synonym for `LENGTH`."
  },
  {
    "name": "STRLEN",
    "arguments": "(expr)",
    "description": "Synonym for `LENGTH`."
  },
  {
    "name": "LOOKUP",
    "arguments": "(expr, lookupName)",
    "description": "Look up expr in a registered [query-time lookup table](lookups.html)."
  },
  {
    "name": "LOWER",
    "arguments": "(expr)",
    "description": "Returns expr in all lowercase."
  },
  {
    "name": "PARSE_LONG",
    "arguments": "(string[, radix])",
    "description": "Parses a string into a long (BIGINT) with the given radix, or 10 (decimal) if a radix is not provided."
  },
  {
    "name": "POSITION",
    "arguments": "(needle IN haystack [FROM fromIndex])",
    "description": "Returns the index of needle within haystack, with indexes starting from 1. The search will begin at fromIndex, or 1 if fromIndex is not specified. If the needle is not found, returns 0."
  },
  {
    "name": "REGEXP_EXTRACT",
    "arguments": "(expr, pattern, [index])",
    "description": "Apply regular expression pattern and extract a capture group, or null if there is no match. If index is unspecified or zero, returns the substring that matched the pattern."
  },
  {
    "name": "REPLACE",
    "arguments": "(expr, pattern, replacement)",
    "description": "Replaces pattern with replacement in expr, and returns the result."
  },
  {
    "name": "STRPOS",
    "arguments": "(haystack, needle)",
    "description": "Returns the index of needle within haystack, with indexes starting from 1. If the needle is not found, returns 0."
  },
  {
    "name": "SUBSTRING",
    "arguments": "(expr, index, [length])",
    "description": "Returns a substring of expr starting at index, with a max length, both measured in UTF-16 code units."
  },
  {
    "name": "RIGHT",
    "arguments": "(expr, [length])",
    "description": "Returns the rightmost length characters from expr."
  },
  {
    "name": "LEFT",
    "arguments": "(expr, [length])",
    "description": "Returns the leftmost length characters from expr."
  },
  {
    "name": "SUBSTR",
    "arguments": "(expr, index, [length])",
    "description": "Synonym for SUBSTRING."
  },
  {
    "name": "BTRIM",
    "arguments": "(expr[, chars])",
    "description": "Alternate form of `TRIM(BOTH <chars> FROM <expr>`)."
  },
  {
    "name": "LTRIM",
    "arguments": "(expr[, chars])",
    "description": "Alternate form of `TRIM(LEADING <chars> FROM <expr>`)."
  },
  {
    "name": "RTRIM",
    "arguments": "(expr[, chars])",
    "description": "Alternate form of `TRIM(TRAILING <chars> FROM <expr>`)."
  },
  {
    "name": "UPPER",
    "arguments": "(expr)",
    "description": "Returns expr in all uppercase."
  },
  {
    "name": "REVERSE",
    "arguments": "(expr)",
    "description": "Reverses expr."
  },
  {
    "name": "REPEAT",
    "arguments": "(expr, [N])",
    "description": "Repeats expr N times"
  },
  {
    "name": "LPAD",
    "arguments": "(expr, length[, chars])",
    "description": "Returns a string of \"length\" from \"expr\" left-padded with \"chars\". If \"length\" is shorter than the length of \"expr\", the result is \"expr\" which is truncated to \"length\". If either \"expr\" or \"chars\" are null, the result will be null."
  },
  {
    "name": "RPAD",
    "arguments": "(expr, length[, chars])",
    "description": "Returns a string of \"length\" from \"expr\" right-padded with \"chars\". If \"length\" is shorter than the length of \"expr\", the result is \"expr\" which is truncated to \"length\". If either \"expr\" or \"chars\" are null, the result will be null."
  },
  {
    "name": "DATE_TRUNC",
    "arguments": "(<unit>, <timestamp_expr>)",
    "description": "Rounds down a timestamp, returning it as a new timestamp. Unit can be 'milliseconds', 'second', 'minute', 'hour', 'day', 'week', 'month', 'quarter', 'year', 'decade', 'century', or 'millennium'."
  },
  {
    "name": "TIME_CEIL",
    "arguments": "(<timestamp_expr>, <period>, [<origin>, [<timezone>]])",
    "description": "Rounds up a timestamp, returning it as a new timestamp. Period can be any ISO8601 period, like P3M (quarters) or PT12H (half-days). The time zone, if provided, should be a time zone name like \"America/Los_Angeles\" or offset like \"-08:00\". This function is similar to `CEIL` but is more flexible."
  },
  {
    "name": "TIME_FLOOR",
    "arguments": "(<timestamp_expr>, <period>, [<origin>, [<timezone>]])",
    "description": "Rounds down a timestamp, returning it as a new timestamp. Period can be any ISO8601 period, like P3M (quarters) or PT12H (half-days). The time zone, if provided, should be a time zone name like \"America/Los_Angeles\" or offset like \"-08:00\". This function is similar to `FLOOR` but is more flexible."
  },
  {
    "name": "TIME_SHIFT",
    "arguments": "(<timestamp_expr>, <period>, <step>, [<timezone>])",
    "description": "Shifts a timestamp by a period (step times), returning it as a new timestamp. Period can be any ISO8601 period. Step may be negative. The time zone, if provided, should be a time zone name like \"America/Los_Angeles\" or offset like \"-08:00\"."
  },
  {
    "name": "TIME_EXTRACT",
    "arguments": "(<timestamp_expr>, [<unit>, [<timezone>]])",
    "description": "Extracts a time part from expr, returning it as a number. Unit can be EPOCH, SECOND, MINUTE, HOUR, DAY (day of month), DOW (day of week), DOY (day of year), WEEK (week of [week year](https://en.wikipedia.org/wiki/ISO_week_date)), MONTH (1 through 12), QUARTER (1 through 4), or YEAR. The time zone, if provided, should be a time zone name like \"America/Los_Angeles\" or offset like \"-08:00\". This function is similar to `EXTRACT` but is more flexible. Unit and time zone must be literals, and must be provided quoted, like `TIME_EXTRACT(__time, 'HOUR')` or `TIME_EXTRACT(__time, 'HOUR', 'America/Los_Angeles')`."
  },
  {
    "name": "TIME_PARSE",
    "arguments": "(<string_expr>, [<pattern>, [<timezone>]])",
    "description": "Parses a string into a timestamp using a given [Joda DateTimeFormat pattern](http://www.joda.org/joda-time/apidocs/org/joda/time/format/DateTimeFormat.html), or ISO8601 (e.g. `2000-01-02T03:04:05Z`) if the pattern is not provided. The time zone, if provided, should be a time zone name like \"America/Los_Angeles\" or offset like \"-08:00\", and will be used as the time zone for strings that do not include a time zone offset. Pattern and time zone must be literals. Strings that cannot be parsed as timestamps will be returned as NULL."
  },
  {
    "name": "TIME_FORMAT",
    "arguments": "(<timestamp_expr>, [<pattern>, [<timezone>]])",
    "description": "Formats a timestamp as a string with a given [Joda DateTimeFormat pattern](http://www.joda.org/joda-time/apidocs/org/joda/time/format/DateTimeFormat.html), or ISO8601 (e.g. `2000-01-02T03:04:05Z`) if the pattern is not provided. The time zone, if provided, should be a time zone name like \"America/Los_Angeles\" or offset like \"-08:00\". Pattern and time zone must be literals."
  },
  {
    "name": "MILLIS_TO_TIMESTAMP",
    "arguments": "(millis_expr)",
    "description": "Converts a number of milliseconds since the epoch into a timestamp."
  },
  {
    "name": "TIMESTAMP_TO_MILLIS",
    "arguments": "(timestamp_expr)",
    "description": "Converts a timestamp into a number of milliseconds since the epoch."
  },
  {
    "name": "EXTRACT",
    "arguments": "(<unit> FROM timestamp_expr)",
    "description": "Extracts a time part from expr, returning it as a number. Unit can be EPOCH, MICROSECOND, MILLISECOND, SECOND, MINUTE, HOUR, DAY (day of month), DOW (day of week), ISODOW (ISO day of week), DOY (day of year), WEEK (week of year), MONTH, QUARTER, YEAR, ISOYEAR, DECADE, CENTURY or MILLENNIUM. Units must be provided unquoted, like `EXTRACT(HOUR FROM __time)`."
  },
  {
    "name": "FLOOR",
    "arguments": "(timestamp_expr TO <unit>)",
    "description": "Rounds down a timestamp, returning it as a new timestamp. Unit can be SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, or YEAR."
  },
  {
    "name": "CEIL",
    "arguments": "(timestamp_expr TO <unit>)",
    "description": "Rounds up a timestamp, returning it as a new timestamp. Unit can be SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, or YEAR."
  },
  {
    "name": "TIMESTAMPADD",
    "arguments": "(<unit>, <count>, <timestamp>)",
    "description": "Equivalent to `timestamp + count * INTERVAL '1' UNIT`."
  },
  {
    "name": "TIMESTAMPDIFF",
    "arguments": "(<unit>, <timestamp1>, <timestamp2>)",
    "description": "Returns the (signed) number of `unit` between `timestamp1` and `timestamp2`. Unit can be SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, or YEAR."
  },
  {
    "name": "IPV4_MATCH",
    "arguments": "(address, subnet)",
    "description": "Returns true if the `address` belongs to the `subnet` literal, else false. If `address` is not a valid IPv4 address, then false is returned. This function is more efficient if `address` is an integer instead of a string."
  },
  {
    "name": "IPV4_PARSE",
    "arguments": "(address)",
    "description": "Parses `address` into an IPv4 address stored as an integer . If `address` is an integer that is a valid IPv4 address, then it is passed through. Returns null if `address` cannot be represented as an IPv4 address."
  },
  {
    "name": "IPV4_STRINGIFY",
    "arguments": "(address)",
    "description": "Converts `address` into an IPv4 address dotted-decimal string. If `address` is a string that is a valid IPv4 address, then it is passed through. Returns null if `address` cannot be represented as an IPv4 address."
  },
  {
    "name": "CAST",
    "arguments": "(value AS TYPE)",
    "description": "Cast value to another type. See [Data types and casts](#data-types-and-casts) for details about how Druid SQL handles CAST."
  },
  {
    "name": "NULLIF",
    "arguments": "(value1, value2)",
    "description": "Returns NULL if value1 and value2 match, else returns value1."
  },
  {
    "name": "COALESCE",
    "arguments": "(value1, value2, ...)",
    "description": "Returns the first value that is neither NULL nor empty string."
  },
  {
    "name": "NVL",
    "arguments": "(expr,expr-for-null)",
    "description": "Returns 'expr-for-null' if 'expr' is null (or empty string for string type)."
  },
  {
    "name": "BLOOM_FILTER_TEST",
    "arguments": "(<expr>, <serialized-filter>)",
    "description": "Returns true if the value is contained in the base64 serialized bloom filter. See [bloom filter extension](../development/extensions-core/bloom-filter.html) documentation for additional details."
  }
];
