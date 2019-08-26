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

export interface SyntaxDescription {
  syntax: string;
  description: string;
}

// prettier-ignore
export const SQL_FUNCTIONS: SyntaxDescription[] = [
  {
    "syntax": "COUNT(*)",
    "description": "Counts the number of rows."
  },
  {
    "syntax": "COUNT(DISTINCT expr)",
    "description": "Counts distinct values of expr, which can be string, numeric, or hyperUnique. By default this is approximate, using a variant of [HyperLogLog](http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf). To get exact counts set \"useApproximateCountDistinct\" to \"false\". If you do this, expr must be string or numeric, since exact counts are not possible using hyperUnique columns. See also `APPROX_COUNT_DISTINCT(expr)`. In exact mode, only one distinct count per query is permitted."
  },
  {
    "syntax": "SUM(expr)",
    "description": "Sums numbers."
  },
  {
    "syntax": "MIN(expr)",
    "description": "Takes the minimum of numbers."
  },
  {
    "syntax": "MAX(expr)",
    "description": "Takes the maximum of numbers."
  },
  {
    "syntax": "AVG(expr)",
    "description": "Averages numbers."
  },
  {
    "syntax": "APPROX_COUNT_DISTINCT(expr)",
    "description": "Counts distinct values of expr, which can be a regular column or a hyperUnique column. This is always approximate, regardless of the value of \"useApproximateCountDistinct\". This uses Druid's builtin \"cardinality\" or \"hyperUnique\" aggregators. See also `COUNT(DISTINCT expr)`."
  },
  {
    "syntax": "APPROX_COUNT_DISTINCT_DS_HLL(expr, [lgK, tgtHllType])",
    "description": "Counts distinct values of expr, which can be a regular column or an [HLL sketch](../development/extensions-core/datasketches-hll.html) column. The `lgK` and `tgtHllType` parameters are described in the HLL sketch documentation. This is always approximate, regardless of the value of \"useApproximateCountDistinct\". See also `COUNT(DISTINCT expr)`. The [DataSketches extension](../development/extensions-core/datasketches-extension.html) must be loaded to use this function."
  },
  {
    "syntax": "APPROX_COUNT_DISTINCT_DS_THETA(expr, [size])",
    "description": "Counts distinct values of expr, which can be a regular column or a [Theta sketch](../development/extensions-core/datasketches-theta.html) column. The `size` parameter is described in the Theta sketch documentation. This is always approximate, regardless of the value of \"useApproximateCountDistinct\". See also `COUNT(DISTINCT expr)`. The [DataSketches extension](../development/extensions-core/datasketches-extension.html) must be loaded to use this function."
  },
  {
    "syntax": "APPROX_QUANTILE(expr, probability, [resolution])",
    "description": "Computes approximate quantiles on numeric or [approxHistogram](../development/extensions-core/approximate-histograms.html#approximate-histogram-aggregator) exprs. The \"probability\" should be between 0 and 1 (exclusive). The \"resolution\" is the number of centroids to use for the computation. Higher resolutions will give more precise results but also have higher overhead. If not provided, the default resolution is 50. The [approximate histogram extension](../development/extensions-core/approximate-histograms.html) must be loaded to use this function."
  },
  {
    "syntax": "APPROX_QUANTILE_DS(expr, probability, [k])",
    "description": "Computes approximate quantiles on numeric or [Quantiles sketch](../development/extensions-core/datasketches-quantiles.html) exprs. The \"probability\" should be between 0 and 1 (exclusive). The `k` parameter is described in the Quantiles sketch documentation. The [DataSketches extension](../development/extensions-core/datasketches-extension.html) must be loaded to use this function."
  },
  {
    "syntax": "APPROX_QUANTILE_FIXED_BUCKETS(expr, probability, numBuckets, lowerLimit, upperLimit, [outlierHandlingMode])",
    "description": "Computes approximate quantiles on numeric or [fixed buckets histogram](../development/extensions-core/approximate-histograms.html#fixed-buckets-histogram) exprs. The \"probability\" should be between 0 and 1 (exclusive). The `numBuckets`, `lowerLimit`, `upperLimit`, and `outlierHandlingMode` parameters are described in the fixed buckets histogram documentation. The [approximate histogram extension](../development/extensions-core/approximate-histograms.html) must be loaded to use this function."
  },
  {
    "syntax": "BLOOM_FILTER(expr, numEntries)",
    "description": "Computes a bloom filter from values produced by `expr`, with `numEntries` maximum number of distinct values before false positive rate increases. See [bloom filter extension](../development/extensions-core/bloom-filter.html) documentation for additional details."
  },
  {
    "syntax": "TDIGEST_QUANTILE(expr, quantileFraction, [compression])",
    "description": "Builds a T-Digest sketch on values produced by `expr` and returns the value for the quantile. Compression parameter (default value 100) determines the accuracy and size of the sketch. Higher compression means higher accuracy but more space to store sketches. See [t-digest extension](../development/extensions-contrib/tdigestsketch-quantiles.html) documentation for additional details."
  },
  {
    "syntax": "TDIGEST_GENERATE_SKETCH(expr, [compression])",
    "description": "Builds a T-Digest sketch on values produced by `expr`. Compression parameter (default value 100) determines the accuracy and size of the sketch Higher compression means higher accuracy but more space to store sketches. See [t-digest extension](../development/extensions-contrib/tdigestsketch-quantiles.html) documentation for additional details."
  },
  {
    "syntax": "VAR_POP(expr)",
    "description": "Computes variance population of `expr`. See [stats extension](../development/extensions-core/stats.html) documentation for additional details."
  },
  {
    "syntax": "VAR_SAMP(expr)",
    "description": "Computes variance sample of `expr`. See [stats extension](../development/extensions-core/stats.html) documentation for additional details."
  },
  {
    "syntax": "VARIANCE(expr)",
    "description": "Computes variance sample of `expr`. See [stats extension](../development/extensions-core/stats.html) documentation for additional details."
  },
  {
    "syntax": "STDDEV_POP(expr)",
    "description": "Computes standard deviation population of `expr`. See [stats extension](../development/extensions-core/stats.html) documentation for additional details."
  },
  {
    "syntax": "STDDEV_SAMP(expr)",
    "description": "Computes standard deviation sample of `expr`. See [stats extension](../development/extensions-core/stats.html) documentation for additional details."
  },
  {
    "syntax": "STDDEV(expr)",
    "description": "Computes standard deviation sample of `expr`. See [stats extension](../development/extensions-core/stats.html) documentation for additional details."
  },
  {
    "syntax": "ABS(expr)",
    "description": "Absolute value."
  },
  {
    "syntax": "CEIL(expr)",
    "description": "Ceiling."
  },
  {
    "syntax": "EXP(expr)",
    "description": "e to the power of expr."
  },
  {
    "syntax": "FLOOR(expr)",
    "description": "Floor."
  },
  {
    "syntax": "LN(expr)",
    "description": "Logarithm (base e)."
  },
  {
    "syntax": "LOG10(expr)",
    "description": "Logarithm (base 10)."
  },
  {
    "syntax": "POWER(expr, power)",
    "description": "expr to a power."
  },
  {
    "syntax": "SQRT(expr)",
    "description": "Square root."
  },
  {
    "syntax": "TRUNCATE(expr[, digits])",
    "description": "Truncate expr to a specific number of decimal digits. If digits is negative, then this truncates that many places to the left of the decimal point. Digits defaults to zero if not specified."
  },
  {
    "syntax": "TRUNC(expr[, digits])",
    "description": "Synonym for `TRUNCATE`."
  },
  {
    "syntax": "ROUND(expr[, digits])",
    "description": "`ROUND(x, y)` would return the value of the x rounded to the y decimal places. While x can be an integer or floating-point number, y must be an integer. The type of the return value is specified by that of x. y defaults to 0 if omitted. When y is negative, x is rounded on the left side of the y decimal points."
  },
  {
    "syntax": "MOD(x, y)",
    "description": "Modulo (remainder of x divided by y)."
  },
  {
    "syntax": "SIN(expr)",
    "description": "Trigonometric sine of an angle expr."
  },
  {
    "syntax": "COS(expr)",
    "description": "Trigonometric cosine of an angle expr."
  },
  {
    "syntax": "TAN(expr)",
    "description": "Trigonometric tangent of an angle expr."
  },
  {
    "syntax": "COT(expr)",
    "description": "Trigonometric cotangent of an angle expr."
  },
  {
    "syntax": "ASIN(expr)",
    "description": "Arc sine of expr."
  },
  {
    "syntax": "ACOS(expr)",
    "description": "Arc cosine of expr."
  },
  {
    "syntax": "ATAN(expr)",
    "description": "Arc tangent of expr."
  },
  {
    "syntax": "ATAN2(y, x)",
    "description": "Angle theta from the conversion of rectangular coordinates (x, y) to polar * coordinates (r, theta)."
  },
  {
    "syntax": "DEGREES(expr)",
    "description": "Converts an angle measured in radians to an approximately equivalent angle measured in degrees"
  },
  {
    "syntax": "RADIANS(expr)",
    "description": "Converts an angle measured in degrees to an approximately equivalent angle measured in radians"
  },
  {
    "syntax": "CONCAT(expr, expr...)",
    "description": "Concats a list of expressions."
  },
  {
    "syntax": "TEXTCAT(expr, expr)",
    "description": "Two argument version of CONCAT."
  },
  {
    "syntax": "STRING_FORMAT(pattern[, args...])",
    "description": "Returns a string formatted in the manner of Java's [String.format](https://docs.oracle.com/javase/8/docs/api/java/lang/String.html#format-java.lang.String-java.lang.Object...-)."
  },
  {
    "syntax": "LENGTH(expr)",
    "description": "Length of expr in UTF-16 code units."
  },
  {
    "syntax": "CHAR_LENGTH(expr)",
    "description": "Synonym for `LENGTH`."
  },
  {
    "syntax": "CHARACTER_LENGTH(expr)",
    "description": "Synonym for `LENGTH`."
  },
  {
    "syntax": "STRLEN(expr)",
    "description": "Synonym for `LENGTH`."
  },
  {
    "syntax": "LOOKUP(expr, lookupName)",
    "description": "Look up expr in a registered [query-time lookup table](lookups.html)."
  },
  {
    "syntax": "LOWER(expr)",
    "description": "Returns expr in all lowercase."
  },
  {
    "syntax": "PARSE_LONG(string[, radix])",
    "description": "Parses a string into a long (BIGINT) with the given radix, or 10 (decimal) if a radix is not provided."
  },
  {
    "syntax": "POSITION(needle IN haystack [FROM fromIndex])",
    "description": "Returns the index of needle within haystack, with indexes starting from 1. The search will begin at fromIndex, or 1 if fromIndex is not specified. If the needle is not found, returns 0."
  },
  {
    "syntax": "REGEXP_EXTRACT(expr, pattern, [index])",
    "description": "Apply regular expression pattern and extract a capture group, or null if there is no match. If index is unspecified or zero, returns the substring that matched the pattern."
  },
  {
    "syntax": "REPLACE(expr, pattern, replacement)",
    "description": "Replaces pattern with replacement in expr, and returns the result."
  },
  {
    "syntax": "STRPOS(haystack, needle)",
    "description": "Returns the index of needle within haystack, with indexes starting from 1. If the needle is not found, returns 0."
  },
  {
    "syntax": "SUBSTRING(expr, index, [length])",
    "description": "Returns a substring of expr starting at index, with a max length, both measured in UTF-16 code units."
  },
  {
    "syntax": "RIGHT(expr, [length])",
    "description": "Returns the rightmost length characters from expr."
  },
  {
    "syntax": "LEFT(expr, [length])",
    "description": "Returns the leftmost length characters from expr."
  },
  {
    "syntax": "SUBSTR(expr, index, [length])",
    "description": "Synonym for SUBSTRING."
  },
  {
    "syntax": "BTRIM(expr[, chars])",
    "description": "Alternate form of `TRIM(BOTH <chars> FROM <expr>`)."
  },
  {
    "syntax": "LTRIM(expr[, chars])",
    "description": "Alternate form of `TRIM(LEADING <chars> FROM <expr>`)."
  },
  {
    "syntax": "RTRIM(expr[, chars])",
    "description": "Alternate form of `TRIM(TRAILING <chars> FROM <expr>`)."
  },
  {
    "syntax": "UPPER(expr)",
    "description": "Returns expr in all uppercase."
  },
  {
    "syntax": "REVERSE(expr)",
    "description": "Reverses expr."
  },
  {
    "syntax": "REPEAT(expr, [N])",
    "description": "Repeats expr N times"
  },
  {
    "syntax": "LPAD(expr, length[, chars])",
    "description": "Returns a string of \"length\" from \"expr\" left-padded with \"chars\". If \"length\" is shorter than the length of \"expr\", the result is \"expr\" which is truncated to \"length\". If either \"expr\" or \"chars\" are null, the result will be null."
  },
  {
    "syntax": "RPAD(expr, length[, chars])",
    "description": "Returns a string of \"length\" from \"expr\" right-padded with \"chars\". If \"length\" is shorter than the length of \"expr\", the result is \"expr\" which is truncated to \"length\". If either \"expr\" or \"chars\" are null, the result will be null."
  },
  {
    "syntax": "DATE_TRUNC(<unit>, <timestamp_expr>)",
    "description": "Rounds down a timestamp, returning it as a new timestamp. Unit can be 'milliseconds', 'second', 'minute', 'hour', 'day', 'week', 'month', 'quarter', 'year', 'decade', 'century', or 'millennium'."
  },
  {
    "syntax": "TIME_CEIL(<timestamp_expr>, <period>, [<origin>, [<timezone>]])",
    "description": "Rounds up a timestamp, returning it as a new timestamp. Period can be any ISO8601 period, like P3M (quarters) or PT12H (half-days). The time zone, if provided, should be a time zone name like \"America/Los_Angeles\" or offset like \"-08:00\". This function is similar to `CEIL` but is more flexible."
  },
  {
    "syntax": "TIME_FLOOR(<timestamp_expr>, <period>, [<origin>, [<timezone>]])",
    "description": "Rounds down a timestamp, returning it as a new timestamp. Period can be any ISO8601 period, like P3M (quarters) or PT12H (half-days). The time zone, if provided, should be a time zone name like \"America/Los_Angeles\" or offset like \"-08:00\". This function is similar to `FLOOR` but is more flexible."
  },
  {
    "syntax": "TIME_SHIFT(<timestamp_expr>, <period>, <step>, [<timezone>])",
    "description": "Shifts a timestamp by a period (step times), returning it as a new timestamp. Period can be any ISO8601 period. Step may be negative. The time zone, if provided, should be a time zone name like \"America/Los_Angeles\" or offset like \"-08:00\"."
  },
  {
    "syntax": "TIME_EXTRACT(<timestamp_expr>, [<unit>, [<timezone>]])",
    "description": "Extracts a time part from expr, returning it as a number. Unit can be EPOCH, SECOND, MINUTE, HOUR, DAY (day of month), DOW (day of week), DOY (day of year), WEEK (week of [week year](https://en.wikipedia.org/wiki/ISO_week_date)), MONTH (1 through 12), QUARTER (1 through 4), or YEAR. The time zone, if provided, should be a time zone name like \"America/Los_Angeles\" or offset like \"-08:00\". This function is similar to `EXTRACT` but is more flexible. Unit and time zone must be literals, and must be provided quoted, like `TIME_EXTRACT(__time, 'HOUR')` or `TIME_EXTRACT(__time, 'HOUR', 'America/Los_Angeles')`."
  },
  {
    "syntax": "TIME_PARSE(<string_expr>, [<pattern>, [<timezone>]])",
    "description": "Parses a string into a timestamp using a given [Joda DateTimeFormat pattern](http://www.joda.org/joda-time/apidocs/org/joda/time/format/DateTimeFormat.html), or ISO8601 (e.g. `2000-01-02T03:04:05Z`) if the pattern is not provided. The time zone, if provided, should be a time zone name like \"America/Los_Angeles\" or offset like \"-08:00\", and will be used as the time zone for strings that do not include a time zone offset. Pattern and time zone must be literals. Strings that cannot be parsed as timestamps will be returned as NULL."
  },
  {
    "syntax": "TIME_FORMAT(<timestamp_expr>, [<pattern>, [<timezone>]])",
    "description": "Formats a timestamp as a string with a given [Joda DateTimeFormat pattern](http://www.joda.org/joda-time/apidocs/org/joda/time/format/DateTimeFormat.html), or ISO8601 (e.g. `2000-01-02T03:04:05Z`) if the pattern is not provided. The time zone, if provided, should be a time zone name like \"America/Los_Angeles\" or offset like \"-08:00\". Pattern and time zone must be literals."
  },
  {
    "syntax": "MILLIS_TO_TIMESTAMP(millis_expr)",
    "description": "Converts a number of milliseconds since the epoch into a timestamp."
  },
  {
    "syntax": "TIMESTAMP_TO_MILLIS(timestamp_expr)",
    "description": "Converts a timestamp into a number of milliseconds since the epoch."
  },
  {
    "syntax": "EXTRACT(<unit> FROM timestamp_expr)",
    "description": "Extracts a time part from expr, returning it as a number. Unit can be EPOCH, MICROSECOND, MILLISECOND, SECOND, MINUTE, HOUR, DAY (day of month), DOW (day of week), ISODOW (ISO day of week), DOY (day of year), WEEK (week of year), MONTH, QUARTER, YEAR, ISOYEAR, DECADE, CENTURY or MILLENNIUM. Units must be provided unquoted, like `EXTRACT(HOUR FROM __time)`."
  },
  {
    "syntax": "FLOOR(timestamp_expr TO <unit>)",
    "description": "Rounds down a timestamp, returning it as a new timestamp. Unit can be SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, or YEAR."
  },
  {
    "syntax": "CEIL(timestamp_expr TO <unit>)",
    "description": "Rounds up a timestamp, returning it as a new timestamp. Unit can be SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, or YEAR."
  },
  {
    "syntax": "TIMESTAMPADD(<unit>, <count>, <timestamp>)",
    "description": "Equivalent to `timestamp + count * INTERVAL '1' UNIT`."
  },
  {
    "syntax": "TIMESTAMPDIFF(<unit>, <timestamp1>, <timestamp2>)",
    "description": "Returns the (signed) number of `unit` between `timestamp1` and `timestamp2`. Unit can be SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, or YEAR."
  },
  {
    "syntax": "IPV4_MATCH(address, subnet)",
    "description": "Returns true if the `address` belongs to the `subnet` literal, else false. If `address` is not a valid IPv4 address, then false is returned. This function is more efficient if `address` is an integer instead of a string."
  },
  {
    "syntax": "IPV4_PARSE(address)",
    "description": "Parses `address` into an IPv4 address stored as an integer . If `address` is an integer that is a valid IPv4 address, then it is passed through. Returns null if `address` cannot be represented as an IPv4 address."
  },
  {
    "syntax": "IPV4_STRINGIFY(address)",
    "description": "Converts `address` into an IPv4 address dotted-decimal string. If `address` is a string that is a valid IPv4 address, then it is passed through. Returns null if `address` cannot be represented as an IPv4 address."
  },
  {
    "syntax": "x IN (values)",
    "description": "True if x is one of the listed values."
  },
  {
    "syntax": "x NOT IN (values)",
    "description": "True if x is not one of the listed values."
  },
  {
    "syntax": "x IN (subquery)",
    "description": "True if x is returned by the subquery. See [Syntax and execution](#syntax-and-execution) above for details about how Druid SQL handles `IN (subquery)`."
  },
  {
    "syntax": "x NOT IN (subquery)",
    "description": "True if x is not returned by the subquery. See [Syntax and execution](#syntax-and-execution) for details about how Druid SQL handles `IN (subquery)`."
  },
  {
    "syntax": "CAST(value AS TYPE)",
    "description": "Cast value to another type. See [Data types and casts](#data-types-and-casts) for details about how Druid SQL handles CAST."
  },
  {
    "syntax": "NULLIF(value1, value2)",
    "description": "Returns NULL if value1 and value2 match, else returns value1."
  },
  {
    "syntax": "COALESCE(value1, value2, ...)",
    "description": "Returns the first value that is neither NULL nor empty string."
  },
  {
    "syntax": "NVL(expr,expr-for-null)",
    "description": "Returns 'expr-for-null' if 'expr' is null (or empty string for string type)."
  },
  {
    "syntax": "BLOOM_FILTER_TEST(<expr>, <serialized-filter>)",
    "description": "Returns true if the value is contained in the base64 serialized bloom filter. See [bloom filter extension](../development/extensions-core/bloom-filter.html) documentation for additional details."
  }
];

// prettier-ignore
export const SQL_DATE_TYPES: SyntaxDescription[] = [
  {
    "syntax": "CHAR",
    "description": "Druid runtime type: STRING"
  },
  {
    "syntax": "VARCHAR",
    "description": "Druid STRING columns are reported as VARCHAR"
  },
  {
    "syntax": "DECIMAL",
    "description": "DECIMAL uses floating point, not fixed point math"
  },
  {
    "syntax": "FLOAT",
    "description": "Druid FLOAT columns are reported as FLOAT"
  },
  {
    "syntax": "REAL",
    "description": "Druid runtime type: DOUBLE"
  },
  {
    "syntax": "DOUBLE",
    "description": "Druid DOUBLE columns are reported as DOUBLE"
  },
  {
    "syntax": "BOOLEAN",
    "description": "Druid runtime type: LONG"
  },
  {
    "syntax": "TINYINT",
    "description": "Druid runtime type: LONG"
  },
  {
    "syntax": "SMALLINT",
    "description": "Druid runtime type: LONG"
  },
  {
    "syntax": "INTEGER",
    "description": "Druid runtime type: LONG"
  },
  {
    "syntax": "BIGINT",
    "description": "Druid LONG columns (except `__time`) are reported as BIGINT"
  },
  {
    "syntax": "TIMESTAMP",
    "description": "Druid's `__time` column is reported as TIMESTAMP. Casts between string and timestamp types assume standard SQL formatting, e.g. `2000-01-02 03:04:05`, _not_ ISO8601 formatting. For handling other formats, use one of the [time functions](#time-functions)"
  },
  {
    "syntax": "DATE",
    "description": "Casting TIMESTAMP to DATE rounds down the timestamp to the nearest day. Casts between string and date types assume standard SQL formatting, e.g. `2000-01-02`. For handling other formats, use one of the [time functions](#time-functions)"
  },
  {
    "syntax": "OTHER",
    "description": "May represent various Druid column types such as hyperUnique, approxHistogram, etc"
  }
];
