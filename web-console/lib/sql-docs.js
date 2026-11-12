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
exports.SQL_DATA_TYPES = {
  "CHAR": [
    "STRING",
    ""
  ],
  "VARCHAR": [
    "STRING",
    "Druid STRING columns are reported as VARCHAR. Can include multi-value strings as well."
  ],
  "DECIMAL": [
    "DOUBLE",
    "DECIMAL uses floating point, not fixed point math"
  ],
  "FLOAT": [
    "FLOAT",
    "Druid FLOAT columns are reported as FLOAT"
  ],
  "REAL": [
    "DOUBLE",
    ""
  ],
  "DOUBLE": [
    "DOUBLE",
    "Druid DOUBLE columns are reported as DOUBLE"
  ],
  "BOOLEAN": [
    "LONG",
    ""
  ],
  "TINYINT": [
    "LONG",
    ""
  ],
  "SMALLINT": [
    "LONG",
    ""
  ],
  "INTEGER": [
    "LONG",
    ""
  ],
  "BIGINT": [
    "LONG",
    "Druid LONG columns (except <code>__time</code>) are reported as BIGINT"
  ],
  "TIMESTAMP": [
    "LONG",
    "Druid's <code>__time</code> column is reported as TIMESTAMP. Casts between string and timestamp types assume standard SQL formatting, such as <code>2000-01-02 03:04:05</code>, not ISO 8601 formatting. For handling other formats, use one of the time functions."
  ],
  "DATE": [
    "LONG",
    "Casting TIMESTAMP to DATE rounds down the timestamp to the nearest day. Casts between string and date types assume standard SQL formatting&mdash;for example, <code>2000-01-02</code>. For handling other formats, use one of the time functions."
  ],
  "ARRAY": [
    "ARRAY",
    "Druid native array types work as SQL arrays, and multi-value strings can be converted to arrays. See Arrays for more information."
  ],
  "OTHER": [
    "COMPLEX",
    "May represent various Druid column types such as hyperUnique, approxHistogram, etc."
  ]
};

// prettier-ignore
exports.SQL_FUNCTIONS = {
  "TABLE": [
    [
      "external",
      "Defines a logical table from an external."
    ]
  ],
  "EXTERN": [
    [
      "inputSource, inputFormat, rowSignature?",
      "Reads external data."
    ]
  ],
  "TYPE": [
    [
      "nativeType",
      "A purely type system modification function what wraps a Druid native type to make it into a SQL type."
    ]
  ],
  "ABS": [
    [
      "expr",
      "Absolute value."
    ]
  ],
  "CEIL": [
    [
      "expr",
      "Ceiling."
    ],
    [
      "timestamp_expr TO unit",
      "Rounds up a timestamp, returning it as a new timestamp. Unit can be SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, or YEAR."
    ]
  ],
  "EXP": [
    [
      "expr",
      "e to the power of <code>expr</code>."
    ]
  ],
  "FLOOR": [
    [
      "expr",
      "Floor."
    ],
    [
      "timestamp_expr TO unit",
      "Rounds down a timestamp, returning it as a new timestamp. Unit can be SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, or YEAR."
    ]
  ],
  "LN": [
    [
      "expr",
      "Logarithm (base e)."
    ]
  ],
  "LOG10": [
    [
      "expr",
      "Logarithm (base 10)."
    ]
  ],
  "POWER": [
    [
      "expr, power",
      "<code>expr</code> raised to the power of <code>power</code>."
    ]
  ],
  "SQRT": [
    [
      "expr",
      "Square root."
    ]
  ],
  "TRUNCATE": [
    [
      "expr, [digits]",
      "Truncate <code>expr</code> to a specific number of decimal digits. If digits is negative, then this truncates that many places to the left of the decimal point. Digits defaults to zero if not specified."
    ]
  ],
  "TRUNC": [
    [
      "expr, [digits]",
      "Alias for <code>TRUNCATE</code>."
    ]
  ],
  "ROUND": [
    [
      "expr, [digits]",
      "<code>ROUND(x, y)</code> would return the value of the x rounded to the y decimal places. While x can be an integer or floating-point number, y must be an integer. The type of the return value is specified by that of x. y defaults to 0 if omitted. When y is negative, x is rounded on the left side of the y decimal points. If <code>expr</code> evaluates to either <code>NaN</code>, <code>expr</code> will be converted to 0. If <code>expr</code> is infinity, <code>expr</code> will be converted to the nearest finite double. "
    ]
  ],
  "MOD": [
    [
      "x, y",
      "Modulo (remainder of x divided by y)."
    ]
  ],
  "SIN": [
    [
      "expr",
      "Trigonometric sine of an angle <code>expr</code>."
    ]
  ],
  "COS": [
    [
      "expr",
      "Trigonometric cosine of an angle <code>expr</code>."
    ]
  ],
  "TAN": [
    [
      "expr",
      "Trigonometric tangent of an angle <code>expr</code>."
    ]
  ],
  "COT": [
    [
      "expr",
      "Trigonometric cotangent of an angle <code>expr</code>."
    ]
  ],
  "ASIN": [
    [
      "expr",
      "Arc sine of <code>expr</code>."
    ]
  ],
  "ACOS": [
    [
      "expr",
      "Arc cosine of <code>expr</code>."
    ]
  ],
  "ATAN": [
    [
      "expr",
      "Arc tangent of <code>expr</code>."
    ]
  ],
  "ATAN2": [
    [
      "y, x",
      "Angle theta from the conversion of rectangular coordinates (x, y) to polar <em> coordinates (r, theta).</em>"
    ]
  ],
  "DEGREES": [
    [
      "expr",
      "Converts an angle measured in radians to an approximately equivalent angle measured in degrees."
    ]
  ],
  "RADIANS": [
    [
      "expr",
      "Converts an angle measured in degrees to an approximately equivalent angle measured in radians."
    ]
  ],
  "BITWISE_AND": [
    [
      "expr1, expr2",
      "Returns the result of <code>expr1 & expr2</code>. Double values will be implicitly cast to longs, use <code>BITWISE_CONVERT_DOUBLE_TO_LONG_BITS</code> to perform bitwise operations directly with doubles."
    ]
  ],
  "BITWISE_COMPLEMENT": [
    [
      "expr",
      "Returns the result of <code>~expr</code>. Double values will be implicitly cast to longs, use <code>BITWISE_CONVERT_DOUBLE_TO_LONG_BITS</code> to perform bitwise operations directly with doubles."
    ]
  ],
  "BITWISE_CONVERT_DOUBLE_TO_LONG_BITS": [
    [
      "expr",
      "Converts the bits of an IEEE 754 floating-point double value to a long. If the input is not a double, it is implicitly cast to a double prior to conversion."
    ]
  ],
  "BITWISE_CONVERT_LONG_BITS_TO_DOUBLE": [
    [
      "expr",
      "Converts a long to the IEEE 754 floating-point double specified by the bits stored in the long. If the input is not a long, it is implicitly cast to a long prior to conversion."
    ]
  ],
  "BITWISE_OR": [
    [
      "expr1, expr2",
      "Returns the result of <code>expr1 [PIPE] expr2</code>. Double values will be implicitly cast to longs, use <code>BITWISE_CONVERT_DOUBLE_TO_LONG_BITS</code> to perform bitwise operations directly with doubles."
    ]
  ],
  "BITWISE_SHIFT_LEFT": [
    [
      "expr1, expr2",
      "Returns the result of <code>expr1 &lt;&lt; expr2</code>. Double values will be implicitly cast to longs, use <code>BITWISE_CONVERT_DOUBLE_TO_LONG_BITS</code> to perform bitwise operations directly with doubles."
    ]
  ],
  "BITWISE_SHIFT_RIGHT": [
    [
      "expr1, expr2",
      "Returns the result of <code>expr1 &gt;&gt; expr2</code>. Double values will be implicitly cast to longs, use <code>BITWISE_CONVERT_DOUBLE_TO_LONG_BITS</code> to perform bitwise operations directly with doubles."
    ]
  ],
  "BITWISE_XOR": [
    [
      "expr1, expr2",
      "Returns the result of <code>expr1 ^ expr2</code>. Double values will be implicitly cast to longs, use <code>BITWISE_CONVERT_DOUBLE_TO_LONG_BITS</code> to perform bitwise operations directly with doubles."
    ]
  ],
  "DIV": [
    [
      "x, y",
      "Returns the result of integer division of x by y "
    ]
  ],
  "HUMAN_READABLE_BINARY_BYTE_FORMAT": [
    [
      "value, [precision]",
      " Format a number in human-readable IEC format. For example, HUMAN<em>READABLE</em>BINARY<em>BYTE</em>FORMAT(1048576) returns <code>1.00 MiB</code>. <code>precision</code> must be in the range of <code>[0, 3]</code> (default: 2). "
    ]
  ],
  "HUMAN_READABLE_DECIMAL_BYTE_FORMAT": [
    [
      "value, [precision]",
      " Format a number in human-readable SI format. HUMAN<em>READABLE</em>DECIMAL<em>BYTE</em>FORMAT(1048576) returns <code>1.04 MB</code>. <code>precision</code> must be in the range of <code>[0, 3]</code> (default: 2). <code>precision</code> must be in the range of <code>[0, 3]</code> (default: 2). "
    ]
  ],
  "HUMAN_READABLE_DECIMAL_FORMAT": [
    [
      "value, [precision]",
      " Format a number in human-readable SI format. For example, HUMAN<em>READABLE</em>DECIMAL<em>FORMAT(1048576) returns <code>1.04 M</code>. <code>precision</code> must be in the range of <code>[0, 3]</code> (default: 2). </em>"
    ]
  ],
  "SAFE_DIVIDE": [
    [
      "x, y",
      "Returns the division of x by y guarded on division by 0. In case y is 0 it returns 0, or <code>null</code> if <code>druid.generic.useDefaultValueForNull=false</code> "
    ]
  ],
  "CONCAT": [
    [
      "expr, expr...",
      "Concats a list of expressions. Also see the concatenation operator."
    ]
  ],
  "TEXTCAT": [
    [
      "expr, expr",
      "Two argument version of <code>CONCAT</code>."
    ]
  ],
  "CONTAINS_STRING": [
    [
      "expr, str",
      "Returns true if the <code>str</code> is a substring of <code>expr</code>."
    ]
  ],
  "ICONTAINS_STRING": [
    [
      "expr, str",
      "Returns true if the <code>str</code> is a substring of <code>expr</code>. The match is case-insensitive."
    ]
  ],
  "DECODE_BASE64_UTF8": [
    [
      "expr",
      "Decodes a Base64-encoded string into a UTF-8 encoded string."
    ]
  ],
  "LEFT": [
    [
      "expr, [length]",
      "Returns the leftmost length characters from <code>expr</code>."
    ]
  ],
  "RIGHT": [
    [
      "expr, [length]",
      "Returns the rightmost length characters from <code>expr</code>."
    ]
  ],
  "LENGTH": [
    [
      "expr",
      "Length of <code>expr</code> in UTF-16 code units."
    ]
  ],
  "CHAR_LENGTH": [
    [
      "expr",
      "Alias for <code>LENGTH</code>."
    ]
  ],
  "CHARACTER_LENGTH": [
    [
      "expr",
      "Alias for <code>LENGTH</code>."
    ]
  ],
  "STRLEN": [
    [
      "expr",
      "Alias for <code>LENGTH</code>."
    ]
  ],
  "LOOKUP": [
    [
      "expr, lookupName, [replaceMissingValueWith]",
      "Look up <code>expr</code> in a registered query-time lookup table. Note that lookups can also be queried directly using the <code>lookup</code> schema. Optional constant <code>replaceMissingValueWith</code> can be passed as a third argument to be returned when value is missing from lookup."
    ]
  ],
  "LOWER": [
    [
      "expr",
      "Returns <code>expr</code> in all lowercase."
    ]
  ],
  "UPPER": [
    [
      "expr",
      "Returns <code>expr</code> in all uppercase."
    ]
  ],
  "LPAD": [
    [
      "expr, length, [chars]",
      "Returns a string of <code>length</code> from <code>expr</code> left-padded with <code>chars</code>. If <code>length</code> is shorter than the length of <code>expr</code>, the result is <code>expr</code> which is truncated to <code>length</code>. The result will be null if either <code>expr</code> or <code>chars</code> is null. If <code>chars</code> is an empty string, no padding is added, however <code>expr</code> may be trimmed if necessary."
    ]
  ],
  "RPAD": [
    [
      "expr, length, [chars]",
      "Returns a string of <code>length</code> from <code>expr</code> right-padded with <code>chars</code>. If <code>length</code> is shorter than the length of <code>expr</code>, the result is <code>expr</code> which is truncated to <code>length</code>. The result will be null if either <code>expr</code> or <code>chars</code> is null. If <code>chars</code> is an empty string, no padding is added, however <code>expr</code> may be trimmed if necessary."
    ]
  ],
  "PARSE_LONG": [
    [
      "string, [radix]",
      "Parses a string into a long (BIGINT) with the given radix, or 10 (decimal) if a radix is not provided."
    ]
  ],
  "POSITION": [
    [
      "needle IN haystack [FROM fromIndex]",
      "Returns the index of <code>needle</code> within <code>haystack</code>, with indexes starting from 1. The search will begin at <code>fromIndex</code>, or 1 if <code>fromIndex</code> is not specified. If <code>needle</code> is not found, returns 0."
    ]
  ],
  "REGEXP_EXTRACT": [
    [
      "expr, pattern, [index]",
      "Apply regular expression <code>pattern</code> to <code>expr</code> and extract a capture group, or <code>NULL</code> if there is no match. If index is unspecified or zero, returns the first substring that matched the pattern. The pattern may match anywhere inside <code>expr</code>; if you want to match the entire string instead, use the <code>^</code> and <code>$</code> markers at the start and end of your pattern. Note: when <code>druid.generic.useDefaultValueForNull = true</code>, it is not possible to differentiate an empty-string match from a non-match (both will return <code>NULL</code>)."
    ]
  ],
  "REGEXP_LIKE": [
    [
      "expr, pattern",
      "Returns whether <code>expr</code> matches regular expression <code>pattern</code>. The pattern may match anywhere inside <code>expr</code>; if you want to match the entire string instead, use the <code>^</code> and <code>$</code> markers at the start and end of your pattern. Similar to <code>LIKE</code>, but uses regexps instead of LIKE patterns. Especially useful in WHERE clauses."
    ]
  ],
  "REGEXP_REPLACE": [
    [
      "expr, pattern, replacement",
      "Replaces all occurrences of regular expression <code>pattern</code> within <code>expr</code> with <code>replacement</code>. The replacement string may refer to capture groups using <code>$1</code>, <code>$2</code>, etc. The pattern may match anywhere inside <code>expr</code>; if you want to match the entire string instead, use the <code>^</code> and <code>$</code> markers at the start and end of your pattern."
    ]
  ],
  "REPLACE": [
    [
      "expr, pattern, replacement",
      "Replaces pattern with replacement in <code>expr</code>, and returns the result."
    ]
  ],
  "REPEAT": [
    [
      "expr, [N]",
      "Repeats <code>expr</code> N times."
    ]
  ],
  "REVERSE": [
    [
      "expr",
      "Reverses <code>expr</code>."
    ]
  ],
  "STRING_FORMAT": [
    [
      "pattern, [args...]",
      "Returns a string formatted in the manner of Java's String.format."
    ]
  ],
  "STRPOS": [
    [
      "haystack, needle",
      "Returns the index of <code>needle</code> within <code>haystack</code>, with indexes starting from 1. If <code>needle</code> is not found, returns 0."
    ]
  ],
  "SUBSTRING": [
    [
      "expr, index, [length]",
      "Returns a substring of <code>expr</code> starting at index, with a max length, both measured in UTF-16 code units."
    ]
  ],
  "SUBSTR": [
    [
      "expr, index, [length]",
      "Alias for <code>SUBSTRING</code>."
    ]
  ],
  "TRIM": [
    [
      "[BOTH | LEADING | TRAILING] [chars FROM] expr",
      "Returns <code>expr</code> with characters removed from the leading, trailing, or both ends of <code>expr</code> if they are in <code>chars</code>. If <code>chars</code> is not provided, it defaults to <code>''</code> (a space). If the directional argument is not provided, it defaults to <code>BOTH</code>."
    ]
  ],
  "BTRIM": [
    [
      "expr, [chars]",
      "Alternate form of <code>TRIM(BOTH chars FROM expr)</code>."
    ]
  ],
  "LTRIM": [
    [
      "expr, [chars]",
      "Alternate form of <code>TRIM(LEADING chars FROM expr)</code>."
    ]
  ],
  "RTRIM": [
    [
      "expr, [chars]",
      "Alternate form of <code>TRIM(TRAILING chars FROM expr)</code>."
    ]
  ],
  "DATE_TRUNC": [
    [
      "unit, timestamp_expr",
      "Rounds down a timestamp, returning it as a new timestamp. Unit can be 'milliseconds', 'second', 'minute', 'hour', 'day', 'week', 'month', 'quarter', 'year', 'decade', 'century', or 'millennium'."
    ]
  ],
  "TIME_CEIL": [
    [
      "timestamp_expr, period, [origin, [timezone]]",
      "Rounds up a timestamp, returning it as a new timestamp. Period can be any ISO8601 period, like P3M (quarters) or PT12H (half-days). Specify <code>origin</code> as a timestamp to set the reference time for rounding. For example, <code>TIME_CEIL(__time, 'PT1H', TIMESTAMP '2016-06-27 00:30:00')</code> measures an hourly period from 00:30-01:30 instead of 00:00-01:00. See Period granularities for details on the default starting boundaries. The time zone, if provided, should be a time zone name like \"America/Los<em>Angeles\" or offset like \"-08:00\". This function is similar to <code>CEIL</code> but is more flexible.</em>"
    ]
  ],
  "TIME_FLOOR": [
    [
      "timestamp_expr, period, [origin, [timezone]]",
      "Rounds down a timestamp, returning it as a new timestamp. Period can be any ISO8601 period, like P3M (quarters) or PT12H (half-days). Specify <code>origin</code> as a timestamp to set the reference time for rounding. For example, <code>TIME_FLOOR(__time, 'PT1H', TIMESTAMP '2016-06-27 00:30:00')</code> measures an hourly period from 00:30-01:30 instead of 00:00-01:00. See Period granularities for details on the default starting boundaries. The time zone, if provided, should be a time zone name like \"America/Los<em>Angeles\" or offset like \"-08:00\". This function is similar to <code>FLOOR</code> but is more flexible.</em>"
    ]
  ],
  "TIME_SHIFT": [
    [
      "timestamp_expr, period, step, [timezone]",
      "Shifts a timestamp by a period (step times), returning it as a new timestamp. Period can be any ISO8601 period. Step may be negative. The time zone, if provided, should be a time zone name like \"America/Los<em>Angeles\" or offset like \"-08:00\".</em>"
    ]
  ],
  "TIME_EXTRACT": [
    [
      "timestamp_expr, [unit, [timezone]]",
      "Extracts a time part from <code>expr</code>, returning it as a number. Unit can be EPOCH, SECOND, MINUTE, HOUR, DAY (day of month), DOW (day of week), DOY (day of year), WEEK (week of week year), MONTH (1 through 12), QUARTER (1 through 4), or YEAR. The time zone, if provided, should be a time zone name like \"America/Los<em>Angeles\" or offset like \"-08:00\". This function is similar to <code>EXTRACT</code> but is more flexible. Unit and time zone must be literals, and must be provided quoted, like <code>TIME_EXTRACT(__time, 'HOUR')</code> or <code>TIME_EXTRACT(__time, 'HOUR', 'America/Los_Angeles')</code>.</em>"
    ]
  ],
  "TIME_PARSE": [
    [
      "string_expr, [pattern, [timezone]]",
      "Parses a string into a timestamp using a given Joda DateTimeFormat pattern, or ISO8601 (e.g. <code>2000-01-02T03:04:05Z</code>) if the pattern is not provided. The time zone, if provided, should be a time zone name like \"America/Los<em>Angeles\" or offset like \"-08:00\", and will be used as the time zone for strings that do not include a time zone offset. Pattern and time zone must be literals. Strings that cannot be parsed as timestamps will be returned as NULL.</em>"
    ]
  ],
  "TIME_FORMAT": [
    [
      "timestamp_expr, [pattern, [timezone]]",
      "Formats a timestamp as a string with a given Joda DateTimeFormat pattern, or ISO8601 (e.g. <code>2000-01-02T03:04:05Z</code>) if the pattern is not provided. The time zone, if provided, should be a time zone name like \"America/Los<em>Angeles\" or offset like \"-08:00\". Pattern and time zone must be literals.</em>"
    ]
  ],
  "TIME_IN_INTERVAL": [
    [
      "timestamp_expr, interval",
      "Returns whether a timestamp is contained within a particular interval. The interval must be a literal string containing any ISO8601 interval, such as <code>'2001-01-01/P1D'</code> or <code>'2001-01-01T01:00:00/2001-01-02T01:00:00'</code>. The start instant of the interval is inclusive and the end instant is exclusive."
    ]
  ],
  "MILLIS_TO_TIMESTAMP": [
    [
      "millis_expr",
      "Converts a number of milliseconds since the epoch (1970-01-01 00:00:00 UTC) into a timestamp."
    ]
  ],
  "TIMESTAMP_TO_MILLIS": [
    [
      "timestamp_expr",
      "Converts a timestamp into a number of milliseconds since the epoch."
    ]
  ],
  "EXTRACT": [
    [
      "unit FROM timestamp_expr",
      "Extracts a time part from <code>expr</code>, returning it as a number. Unit can be EPOCH, MICROSECOND, MILLISECOND, SECOND, MINUTE, HOUR, DAY (day of month), DOW (day of week), ISODOW (ISO day of week), DOY (day of year), WEEK (week of year), MONTH, QUARTER, YEAR, ISOYEAR, DECADE, CENTURY or MILLENNIUM. Units must be provided unquoted, like <code>EXTRACT(HOUR FROM __time)</code>."
    ]
  ],
  "TIMESTAMPADD": [
    [
      "unit, count, timestamp",
      "Equivalent to <code>timestamp + count * INTERVAL '1' UNIT</code>."
    ]
  ],
  "TIMESTAMPDIFF": [
    [
      "unit, timestamp1, timestamp2",
      "Returns the (signed) number of <code>unit</code> between <code>timestamp1</code> and <code>timestamp2</code>. Unit can be SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, or YEAR."
    ]
  ],
  "GREATEST": [
    [
      "[expr1, ...]",
      "Evaluates zero or more expressions and returns the maximum value based on comparisons as described above."
    ]
  ],
  "LEAST": [
    [
      "[expr1, ...]",
      "Evaluates zero or more expressions and returns the minimum value based on comparisons as described above."
    ]
  ],
  "IPV4_MATCH": [
    [
      "address, subnet",
      "Returns true if the <code>address</code> belongs to the <code>subnet</code> literal, else false. If <code>address</code> is not a valid IPv4 address, then false is returned. This function is more efficient if <code>address</code> is an integer instead of a string."
    ]
  ],
  "IPV4_PARSE": [
    [
      "address",
      "Parses <code>address</code> into an IPv4 address stored as an integer . If <code>address</code> is an integer that is a valid IPv4 address, then it is passed through. Returns null if <code>address</code> cannot be represented as an IPv4 address."
    ]
  ],
  "IPV4_STRINGIFY": [
    [
      "address",
      "Converts <code>address</code> into an IPv4 address dotted-decimal string. If <code>address</code> is a string that is a valid IPv4 address, then it is passed through. Returns null if <code>address</code> cannot be represented as an IPv4 address."
    ]
  ],
  "HLL_SKETCH_ESTIMATE": [
    [
      "expr, [round]",
      "Returns the distinct count estimate from an HLL sketch. <code>expr</code> must return an HLL sketch. The optional <code>round</code> boolean parameter will round the estimate if set to <code>true</code>, with a default of <code>false</code>."
    ]
  ],
  "HLL_SKETCH_ESTIMATE_WITH_ERROR_BOUNDS": [
    [
      "expr, [numStdDev]",
      "Returns the distinct count estimate and error bounds from an HLL sketch. <code>expr</code> must return an HLL sketch. An optional <code>numStdDev</code> argument can be provided."
    ]
  ],
  "HLL_SKETCH_UNION": [
    [
      "[lgK, tgtHllType], expr0, expr1, ...",
      "Returns a union of HLL sketches, where each input expression must return an HLL sketch. The <code>lgK</code> and <code>tgtHllType</code> can be optionally specified as the first parameter; if provided, both optional parameters must be specified."
    ]
  ],
  "HLL_SKETCH_TO_STRING": [
    [
      "expr",
      "Returns a human-readable string representation of an HLL sketch for debugging. <code>expr</code> must return an HLL sketch."
    ]
  ],
  "THETA_SKETCH_ESTIMATE": [
    [
      "expr",
      "Returns the distinct count estimate from a theta sketch. <code>expr</code> must return a theta sketch."
    ]
  ],
  "THETA_SKETCH_ESTIMATE_WITH_ERROR_BOUNDS": [
    [
      "expr, errorBoundsStdDev",
      "Returns the distinct count estimate and error bounds from a theta sketch. <code>expr</code> must return a theta sketch."
    ]
  ],
  "THETA_SKETCH_UNION": [
    [
      "[size], expr0, expr1, ...",
      "Returns a union of theta sketches, where each input expression must return a theta sketch. The <code>size</code> can be optionally specified as the first parameter."
    ]
  ],
  "THETA_SKETCH_INTERSECT": [
    [
      "[size], expr0, expr1, ...",
      "Returns an intersection of theta sketches, where each input expression must return a theta sketch. The <code>size</code> can be optionally specified as the first parameter."
    ]
  ],
  "THETA_SKETCH_NOT": [
    [
      "[size], expr0, expr1, ...",
      "Returns a set difference of theta sketches, where each input expression must return a theta sketch. The <code>size</code> can be optionally specified as the first parameter."
    ]
  ],
  "DS_GET_QUANTILE": [
    [
      "expr, fraction",
      "Returns the quantile estimate corresponding to <code>fraction</code> from a quantiles sketch. <code>expr</code> must return a quantiles sketch."
    ]
  ],
  "DS_GET_QUANTILES": [
    [
      "expr, fraction0, fraction1, ...",
      "Returns a string representing an array of quantile estimates corresponding to a list of fractions from a quantiles sketch. <code>expr</code> must return a quantiles sketch."
    ]
  ],
  "DS_HISTOGRAM": [
    [
      "expr, splitPoint0, splitPoint1, ...",
      "Returns a string representing an approximation to the histogram given a list of split points that define the histogram bins from a quantiles sketch. <code>expr</code> must return a quantiles sketch."
    ]
  ],
  "DS_CDF": [
    [
      "expr, splitPoint0, splitPoint1, ...",
      "Returns a string representing approximation to the Cumulative Distribution Function given a list of split points that define the edges of the bins from a quantiles sketch. <code>expr</code> must return a quantiles sketch."
    ]
  ],
  "DS_RANK": [
    [
      "expr, value",
      "Returns an approximation to the rank of a given value that is the fraction of the distribution less than that value from a quantiles sketch. <code>expr</code> must return a quantiles sketch."
    ]
  ],
  "DS_QUANTILE_SUMMARY": [
    [
      "expr",
      "Returns a string summary of a quantiles sketch, useful for debugging. <code>expr</code> must return a quantiles sketch."
    ]
  ],
  "DS_TUPLE_DOUBLES_INTERSECT": [
    [
      "expr, ..., [nominalEntries]",
      "Returns an intersection of tuple sketches, where each input expression must return a tuple sketch which contains an array of double values as its Summary Object. The values contained in the Summary Objects are summed when combined. If the last value of the array is a numeric literal, Druid assumes that the value is an override parameter for nominal entries."
    ]
  ],
  "DS_TUPLE_DOUBLES_NOT": [
    [
      "expr, ..., [nominalEntries]",
      "Returns a set difference of tuple sketches, where each input expression must return a tuple sketch which contains an array of double values as its Summary Object. The values contained in the Summary Object are preserved as is. If the last value of the array is a numeric literal, Druid assumes that the value is an override parameter for nominal entries."
    ]
  ],
  "DS_TUPLE_DOUBLES_UNION": [
    [
      "expr, ..., [nominalEntries]",
      "Returns a union of tuple sketches, where each input expression must return a tuple sketch which contains an array of double values as its Summary Object. The values contained in the Summary Objects are summed when combined. If the last value of the array is a numeric literal, Druid assumes that the value is an override parameter for nominal entries."
    ]
  ],
  "BLOOM_FILTER_TEST": [
    [
      "expr, serialized-filter",
      "Returns true if the value of <code>expr</code> is contained in the base64-serialized Bloom filter. See the Bloom filter extension documentation for additional details. See the <code>BLOOM_FILTER</code> function for computing Bloom filters."
    ]
  ],
  "CAST": [
    [
      "value AS TYPE",
      "Cast value to another type. See Data types for details about how Druid SQL handles CAST."
    ]
  ],
  "COALESCE": [
    [
      "value1, value2, ...",
      "Returns the first value that is neither NULL nor empty string."
    ]
  ],
  "DECODE_BASE64_COMPLEX": [
    [
      "dataType, expr",
      " Decodes a Base64-encoded string into a complex data type, where <code>dataType</code> is the complex data type and <code>expr</code> is the Base64-encoded string to decode. The <code>hyperUnique</code> and <code>serializablePairLongString</code> data types are supported by default. You can enable support for the following complex data types by loading their extensions:\n<ul><li><code>druid-bloom-filter</code>: <code>bloom</code></li><li><code>druid-datasketches</code>: <code>arrayOfDoublesSketch</code>, <code>HLLSketch</code>, <code>KllDoublesSketch</code>, <code>KllFloatsSketch</code>, <code>quantilesDoublesSketch</code>, <code>thetaSketch</code></li><li><code>druid-histogram</code>: <code>approximateHistogram</code>, <code>fixedBucketsHistogram</code></li><li><code>druid-stats</code>: <code>variance</code></li><li><code>druid-compressed-big-decimal</code>: <code>compressedBigDecimal</code></li><li><code>druid-momentsketch</code>: <code>momentSketch</code></li><li><code>druid-tdigestsketch</code>: <code>tDigestSketch</code></li></ul>"
    ]
  ],
  "NULLIF": [
    [
      "value1, value2",
      "Returns NULL if <code>value1</code> and <code>value2</code> match, else returns <code>value1</code>."
    ]
  ],
  "NVL": [
    [
      "value1, value2",
      "Returns <code>value1</code> if <code>value1</code> is not null, otherwise <code>value2</code>."
    ]
  ],
  "COUNT": [
    [
      "*",
      "Counts the number of rows."
    ],
    [
      "[DISTINCT] expr",
      "Counts the values of <code>expr</code>.<br /><br />By default, using DISTINCT serves as an alias for <code>APPROX_COUNT_DISTINCT</code> (<code>useApproximateCountDistinct=true</code>). The specific algorithm depends on the value of <code>druid.sql.approxCountDistinct.function</code>. In this mode, you can use strings, numbers, or prebuilt sketches. If counting prebuilt sketches, the prebuilt sketch type must match the selected algorithm.<br /><br />When <code>useApproximateCountDistinct=false</code>, returns the exact computation. In this case, <code>expr</code> must be string or numeric, since exact counts are not possible using prebuilt sketches. In exact mode, only one distinct count per query is permitted unless <code>useGroupingSetForExactDistinct</code> is enabled.<br /><br />Counts each distinct value in a <code>multi-value</code>-row separately."
    ]
  ],
  "SUM": [
    [
      "expr",
      "Sums numbers."
    ]
  ],
  "MIN": [
    [
      "expr",
      "Takes the minimum of numbers."
    ]
  ],
  "MAX": [
    [
      "expr",
      "Takes the maximum of numbers."
    ]
  ],
  "AVG": [
    [
      "expr",
      "Averages numbers."
    ]
  ],
  "APPROX_COUNT_DISTINCT": [
    [
      "expr",
      "Counts distinct values of <code>expr</code> using an approximate algorithm. The <code>expr</code> can be a regular column or a prebuilt sketch column.<br /><br />The specific algorithm depends on the value of <code>druid.sql.approxCountDistinct.function</code>. By default, this is <code>APPROX_COUNT_DISTINCT_BUILTIN</code>. If the DataSketches extension is loaded, you can set it to <code>APPROX_COUNT_DISTINCT_DS_HLL</code> or <code>APPROX_COUNT_DISTINCT_DS_THETA</code>.<br /><br />When run on prebuilt sketch columns, the sketch column type must match the implementation of this function. For example: when <code>druid.sql.approxCountDistinct.function</code> is set to <code>APPROX_COUNT_DISTINCT_BUILTIN</code>, this function runs on prebuilt hyperUnique columns, but not on prebuilt HLLSketchBuild columns."
    ]
  ],
  "APPROX_COUNT_DISTINCT_BUILTIN": [
    [
      "expr",
      "<em>Usage note:</em> consider using <code>APPROX_COUNT_DISTINCT_DS_HLL</code> instead, which offers better accuracy in many cases.<br />Counts distinct values of <code>expr</code> using Druid's built-in \"cardinality\" or \"hyperUnique\" aggregators, which implement a variant of HyperLogLog. The <code>expr</code> can be a string, a number, or a prebuilt hyperUnique column. Results are always approximate, regardless of the value of <code>useApproximateCountDistinct</code>."
    ]
  ],
  "APPROX_QUANTILE": [
    [
      "expr, probability, [resolution]",
      "<em>Deprecated.</em> Use <code>APPROX_QUANTILE_DS</code> instead, which provides a superior distribution-independent algorithm with formal error guarantees.<br />Computes approximate quantiles on numeric or approxHistogram expressions. <code>probability</code> should be between 0 and 1, exclusive. <code>resolution</code> is the number of centroids to use for the computation. Higher resolutions will give more precise results but also have higher overhead. If not provided, the default resolution is 50. Load the approximate histogram extension to use this function."
    ]
  ],
  "APPROX_QUANTILE_FIXED_BUCKETS": [
    [
      "expr, probability, numBuckets, lowerLimit, upperLimit, [outlierHandlingMode]",
      "Computes approximate quantiles on numeric or fixed buckets histogram expressions. <code>probability</code> should be between 0 and 1, exclusive. The <code>numBuckets</code>, <code>lowerLimit</code>, <code>upperLimit</code>, and <code>outlierHandlingMode</code> parameters are described in the fixed buckets histogram documentation. Load the approximate histogram extension to use this function."
    ]
  ],
  "BLOOM_FILTER": [
    [
      "expr, numEntries",
      "Computes a bloom filter from values produced by <code>expr</code>, with <code>numEntries</code> maximum number of distinct values before false positive rate increases. See bloom filter extension documentation for additional details."
    ]
  ],
  "VAR_POP": [
    [
      "expr",
      "Computes variance population of <code>expr</code>. See stats extension documentation for additional details."
    ]
  ],
  "VAR_SAMP": [
    [
      "expr",
      "Computes variance sample of <code>expr</code>. See stats extension documentation for additional details."
    ]
  ],
  "VARIANCE": [
    [
      "expr",
      "Computes variance sample of <code>expr</code>. See stats extension documentation for additional details."
    ]
  ],
  "STDDEV_POP": [
    [
      "expr",
      "Computes standard deviation population of <code>expr</code>. See stats extension documentation for additional details."
    ]
  ],
  "STDDEV_SAMP": [
    [
      "expr",
      "Computes standard deviation sample of <code>expr</code>. See stats extension documentation for additional details."
    ]
  ],
  "STDDEV": [
    [
      "expr",
      "Computes standard deviation sample of <code>expr</code>. See stats extension documentation for additional details."
    ]
  ],
  "EARLIEST": [
    [
      "expr, [maxBytesPerValue]",
      "Returns the earliest value of <code>expr</code>.<br />If <code>expr</code> comes from a relation with a timestamp column (like <code>__time</code> in a Druid datasource), the \"earliest\" is taken from the row with the overall earliest non-null value of the timestamp column.<br />If the earliest non-null value of the timestamp column appears in multiple rows, the <code>expr</code> may be taken from any of those rows. If <code>expr</code> does not come from a relation with a timestamp, then it is simply the first value encountered.<br /><br />If <code>expr</code> is a string or complex type <code>maxBytesPerValue</code> amount of space is allocated for the aggregation. Strings longer than this limit are truncated. The  <code>maxBytesPerValue</code> parameter should be set as low as possible, since high values will lead to wasted memory.\nIf <code>maxBytesPerValue</code>is omitted; it defaults to <code>1024</code>. "
    ]
  ],
  "EARLIEST_BY": [
    [
      "expr, timestampExpr, [maxBytesPerValue]",
      "Returns the earliest value of <code>expr</code>.<br />The earliest value of <code>expr</code> is taken from the row with the overall earliest non-null value of <code>timestampExpr</code>. <br />If the earliest non-null value of <code>timestampExpr</code> appears in multiple rows, the <code>expr</code> may be taken from any of those rows.<br /><br />If <code>expr</code> is a string or complex type <code>maxBytesPerValue</code> amount of space is allocated for the aggregation. Strings longer than this limit are truncated. The  <code>maxBytesPerValue</code> parameter should be set as low as possible, since high values will lead to wasted memory.\nIf <code>maxBytesPerValue</code>is omitted; it defaults to <code>1024</code>.<br /><br />Use <code>EARLIEST</code> instead of <code>EARLIEST_BY</code> on a table that has rollup enabled and was created with any variant of <code>EARLIEST</code>, <code>LATEST</code>, <code>EARLIEST_BY</code>, or <code>LATEST_BY</code>. In these cases, the intermediate type already stores the timestamp, and Druid ignores the value passed in <code>timestampExpr</code>. "
    ]
  ],
  "LATEST": [
    [
      "expr, [maxBytesPerValue]",
      "Returns the latest value of <code>expr</code><br />The <code>expr</code> must come from a relation with a timestamp column (like <code>__time</code> in a Druid datasource) and the \"latest\" is taken from the row with the overall latest non-null value of the timestamp column.<br />If the latest non-null value of the timestamp column appears in multiple rows, the <code>expr</code> may be taken from any of those rows.<br /><br />If <code>expr</code> is a string or complex type <code>maxBytesPerValue</code> amount of space is allocated for the aggregation. Strings longer than this limit are truncated. The  <code>maxBytesPerValue</code> parameter should be set as low as possible, since high values will lead to wasted memory.\nIf <code>maxBytesPerValue</code>is omitted; it defaults to <code>1024</code>. "
    ]
  ],
  "LATEST_BY": [
    [
      "expr, timestampExpr, [maxBytesPerValue]",
      "Returns the latest value of <code>expr</code>.<br />The latest value of <code>expr</code> is taken from the row with the overall latest non-null value of <code>timestampExpr</code>.<br />If the overall latest non-null value of <code>timestampExpr</code> appears in multiple rows, the <code>expr</code> may be taken from any of those rows.<br /><br />If <code>expr</code> is a string or complex type <code>maxBytesPerValue</code> amount of space is allocated for the aggregation. Strings longer than this limit are truncated. The <code>maxBytesPerValue</code> parameter should be set as low as possible, since high values will lead to wasted memory.\nIf <code>maxBytesPerValue</code>is omitted; it defaults to <code>1024</code>.<br /><br />Use <code>LATEST</code> instead of <code>LATEST_BY</code> on a table that has rollup enabled and was created with any variant of <code>EARLIEST</code>, <code>LATEST</code>, <code>EARLIEST_BY</code>, or <code>LATEST_BY</code>. In these cases, the intermediate type already stores the timestamp, and Druid ignores the value passed in <code>timestampExpr</code>. "
    ]
  ],
  "ANY_VALUE": [
    [
      "expr, [maxBytesPerValue, [aggregateMultipleValues]]",
      "Returns any value of <code>expr</code> including null. This aggregator can simplify and optimize the performance by returning the first encountered value (including <code>null</code>).<br /><br />If <code>expr</code> is a string or complex type <code>maxBytesPerValue</code> amount of space is allocated for the aggregation. Strings longer than this limit are truncated. The <code>maxBytesPerValue</code> parameter should be set as low as possible, since high values will lead to wasted memory.\nIf <code>maxBytesPerValue</code> is omitted; it defaults to <code>1024</code>. <code>aggregateMultipleValues</code> is an optional boolean flag controls the behavior of aggregating a multi-value dimension. <code>aggregateMultipleValues</code> is set as true by default and returns the stringified array in case of a multi-value dimension. By setting it to false, function will return first value instead. "
    ]
  ],
  "GROUPING": [
    [
      "expr, expr...",
      "Returns a number to indicate which groupBy dimension is included in a row, when using <code>GROUPING SETS</code>. Refer to additional documentation on how to infer this number."
    ]
  ],
  "ARRAY_AGG": [
    [
      "expr, [size]",
      "Collects all values of <code>expr</code> into an ARRAY, including null values, with <code>size</code> in bytes limit on aggregation size (default of 1024 bytes). If the aggregated array grows larger than the maximum size in bytes, the query will fail. Use of <code>ORDER BY</code> within the <code>ARRAY_AGG</code> expression is not currently supported, and the ordering of results within the output array may vary depending on processing order."
    ],
    [
      "DISTINCT expr, [size]",
      "Collects all distinct values of <code>expr</code> into an ARRAY, including null values, with <code>size</code> in bytes limit on aggregation size (default of 1024 bytes) per aggregate. If the aggregated array grows larger than the maximum size in bytes, the query will fail. Use of <code>ORDER BY</code> within the <code>ARRAY_AGG</code> expression is not currently supported, and the ordering of results will be based on the default for the element type."
    ]
  ],
  "ARRAY_CONCAT_AGG": [
    [
      "expr, [size]",
      "Concatenates all array <code>expr</code> into a single ARRAY, with <code>size</code> in bytes limit on aggregation size (default of 1024 bytes).   Input <code>expr</code> <em>must</em> be an array. Null <code>expr</code> will be ignored, but any null values within an <code>expr</code> <em>will</em> be included in the resulting array. If the aggregated array grows larger than the maximum size in bytes, the query will fail. Use of <code>ORDER BY</code> within the <code>ARRAY_CONCAT_AGG</code> expression is not currently supported, and the ordering of results within the output array may vary depending on processing order."
    ],
    [
      "DISTINCT expr, [size]",
      "Concatenates all distinct values of all array <code>expr</code> into a single ARRAY, with <code>size</code> in bytes limit on aggregation size (default of 1024 bytes) per aggregate. Input <code>expr</code> <em>must</em> be an array. Null <code>expr</code> will be ignored, but any null values within an <code>expr</code> <em>will</em> be included in the resulting array. If the aggregated array grows larger than the maximum size in bytes, the query will fail. Use of <code>ORDER BY</code> within the <code>ARRAY_CONCAT_AGG</code> expression is not currently supported, and the ordering of results will be based on the default for the element type."
    ]
  ],
  "STRING_AGG": [
    [
      "[DISTINCT] expr, [separator, [size]]",
      "Collects all values (or all distinct values) of <code>expr</code> into a single STRING, ignoring null values. Each value is joined by an optional <code>separator</code>, which must be a literal STRING. If the <code>separator</code> is not provided, strings are concatenated without a separator.<br /><br />An optional <code>size</code> in bytes can be supplied to limit aggregation size (default of 1024 bytes). If the aggregated string grows larger than the maximum size in bytes, the query will fail. Use of <code>ORDER BY</code> within the <code>STRING_AGG</code> expression is not currently supported, and the ordering of results within the output string may vary depending on processing order."
    ]
  ],
  "LISTAGG": [
    [
      "[DISTINCT] expr, [separator, [size]]",
      "Synonym for <code>STRING_AGG</code>."
    ]
  ],
  "BIT_AND": [
    [
      "expr",
      "Performs a bitwise AND operation on all input values."
    ]
  ],
  "BIT_OR": [
    [
      "expr",
      "Performs a bitwise OR operation on all input values."
    ]
  ],
  "BIT_XOR": [
    [
      "expr",
      "Performs a bitwise XOR operation on all input values."
    ]
  ],
  "APPROX_COUNT_DISTINCT_DS_HLL": [
    [
      "expr, [lgK, tgtHllType]",
      "Counts distinct values of <code>expr</code>, which can be a regular column or an HLL sketch column. Results are always approximate, regardless of the value of <code>useApproximateCountDistinct</code>. The <code>lgK</code> and <code>tgtHllType</code> parameters here are, like the equivalents in the aggregator, described in the HLL sketch documentation. See also <code>COUNT(DISTINCT expr)</code>."
    ]
  ],
  "DS_HLL": [
    [
      "expr, [lgK, tgtHllType]",
      "Creates an HLL sketch on the values of <code>expr</code>, which can be a regular column or a column containing HLL sketches. The <code>lgK</code> and <code>tgtHllType</code> parameters are described in the HLL sketch documentation."
    ]
  ],
  "APPROX_COUNT_DISTINCT_DS_THETA": [
    [
      "expr, [size]",
      "Counts distinct values of <code>expr</code>, which can be a regular column or a Theta sketch column. Results are always approximate, regardless of the value of <code>useApproximateCountDistinct</code>. The <code>size</code> parameter is described in the Theta sketch documentation. See also <code>COUNT(DISTINCT expr)</code>."
    ]
  ],
  "DS_THETA": [
    [
      "expr, [size]",
      "Creates a Theta sketch on the values of <code>expr</code>, which can be a regular column or a column containing Theta sketches. The <code>size</code> parameter is described in the Theta sketch documentation."
    ]
  ],
  "APPROX_QUANTILE_DS": [
    [
      "expr, probability, [k]",
      "Computes approximate quantiles on numeric or Quantiles sketch expressions. The <code>probability</code> value should be between 0 and 1, exclusive. The <code>k</code> parameter is described in the Quantiles sketch documentation.<br />See the known issue with this function."
    ]
  ],
  "DS_QUANTILES_SKETCH": [
    [
      "expr, [k]",
      "Creates a Quantiles sketch on the values of <code>expr</code>, which can be a regular column or a column containing quantiles sketches. The <code>k</code> parameter is described in the Quantiles sketch documentation.<br />See the known issue with this function."
    ]
  ],
  "TDIGEST_QUANTILE": [
    [
      "expr, quantileFraction, [compression]",
      "Builds a T-Digest sketch on values produced by <code>expr</code> and returns the value for the quantile. Compression parameter (default value 100) determines the accuracy and size of the sketch. Higher compression means higher accuracy but more space to store sketches."
    ]
  ],
  "TDIGEST_GENERATE_SKETCH": [
    [
      "expr, [compression]",
      "Builds a T-Digest sketch on values produced by <code>expr</code>. Compression parameter (default value 100) determines the accuracy and size of the sketch Higher compression means higher accuracy but more space to store sketches."
    ]
  ],
  "ARRAY_LENGTH": [
    [
      "arr",
      "Returns length of the array expression."
    ]
  ],
  "ARRAY_OFFSET": [
    [
      "arr, long",
      "Returns the array element at the 0-based index supplied, or null for an out of range index."
    ]
  ],
  "ARRAY_ORDINAL": [
    [
      "arr, long",
      "Returns the array element at the 1-based index supplied, or null for an out of range index."
    ]
  ],
  "ARRAY_CONTAINS": [
    [
      "arr, expr",
      "If <code>expr</code> is a scalar type, returns 1 if <code>arr</code> contains <code>expr</code>. If <code>expr</code> is an array, returns 1 if <code>arr</code> contains all elements of <code>expr</code>. Otherwise returns 0."
    ]
  ],
  "ARRAY_OVERLAP": [
    [
      "arr1, arr2",
      "Returns 1 if <code>arr1</code> and <code>arr2</code> have any elements in common, else 0."
    ]
  ],
  "ARRAY_OFFSET_OF": [
    [
      "arr, expr",
      "Returns the 0-based index of the first occurrence of <code>expr</code> in the array. If no matching elements exist in the array, returns <code>null</code> or <code>-1</code> if <code>druid.generic.useDefaultValueForNull=true</code> (deprecated legacy mode)."
    ]
  ],
  "ARRAY_ORDINAL_OF": [
    [
      "arr, expr",
      "Returns the 1-based index of the first occurrence of <code>expr</code> in the array. If no matching elements exist in the array, returns <code>null</code> or <code>-1</code> if <code>druid.generic.useDefaultValueForNull=true</code> (deprecated legacy mode)."
    ]
  ],
  "ARRAY_PREPEND": [
    [
      "expr, arr",
      "Adds <code>expr</code> to the beginning of <code>arr</code>, the resulting array type determined by the type of <code>arr</code>."
    ]
  ],
  "ARRAY_APPEND": [
    [
      "arr, expr",
      "Appends <code>expr</code> to <code>arr</code>, the resulting array type determined by the type of <code>arr</code>."
    ]
  ],
  "ARRAY_CONCAT": [
    [
      "arr1, arr2",
      "Concatenates <code>arr2</code> to <code>arr1</code>. The resulting array type is determined by the type of <code>arr1</code>."
    ]
  ],
  "ARRAY_SLICE": [
    [
      "arr, start, end",
      "Returns the subarray of <code>arr</code> from the 0-based index <code>start</code> (inclusive) to <code>end</code> (exclusive). Returns <code>null</code>, if <code>start</code> is less than 0, greater than length of <code>arr</code>, or greater than <code>end</code>."
    ]
  ],
  "ARRAY_TO_STRING": [
    [
      "arr, str",
      "Joins all elements of <code>arr</code> by the delimiter specified by <code>str</code>."
    ]
  ],
  "STRING_TO_ARRAY": [
    [
      "str1, str2",
      "Splits <code>str1</code> into an array on the delimiter specified by <code>str2</code>, which is a regular expression."
    ]
  ],
  "ARRAY_TO_MV": [
    [
      "arr",
      "Converts an <code>ARRAY</code> of any type into a multi-value string <code>VARCHAR</code>."
    ]
  ],
  "MV_FILTER_ONLY": [
    [
      "expr, arr",
      "Filters multi-value <code>expr</code> to include only values contained in array <code>arr</code>."
    ]
  ],
  "MV_FILTER_NONE": [
    [
      "expr, arr",
      "Filters multi-value <code>expr</code> to include no values contained in array <code>arr</code>."
    ]
  ],
  "MV_LENGTH": [
    [
      "arr",
      "Returns length of the array expression."
    ]
  ],
  "MV_OFFSET": [
    [
      "arr, long",
      "Returns the array element at the 0-based index supplied, or null for an out of range index."
    ]
  ],
  "MV_ORDINAL": [
    [
      "arr, long",
      "Returns the array element at the 1-based index supplied, or null for an out of range index."
    ]
  ],
  "MV_CONTAINS": [
    [
      "arr, expr",
      "If <code>expr</code> is a scalar type, returns 1 if <code>arr</code> contains <code>expr</code>. If <code>expr</code> is an array, returns 1 if <code>arr</code> contains all elements of <code>expr</code>. Otherwise returns 0."
    ]
  ],
  "MV_OVERLAP": [
    [
      "arr1, arr2",
      "Returns 1 if <code>arr1</code> and <code>arr2</code> have any elements in common, else 0."
    ]
  ],
  "MV_OFFSET_OF": [
    [
      "arr, expr",
      "Returns the 0-based index of the first occurrence of <code>expr</code> in the array. If no matching elements exist in the array, returns <code>null</code> or -1 if <code>druid.generic.useDefaultValueForNull=true</code> (deprecated legacy mode)."
    ]
  ],
  "MV_ORDINAL_OF": [
    [
      "arr, expr",
      "Returns the 1-based index of the first occurrence of <code>expr</code> in the array. If no matching elements exist in the array, returns <code>null</code> or <code>-1</code> if <code>druid.generic.useDefaultValueForNull=true</code> (deprecated legacy mode)."
    ]
  ],
  "MV_PREPEND": [
    [
      "expr, arr",
      "Adds <code>expr</code> to the beginning of <code>arr</code>, the resulting array type determined by the type <code>arr</code>."
    ]
  ],
  "MV_APPEND": [
    [
      "arr, expr",
      "Appends <code>expr</code> to <code>arr</code>, the resulting array type determined by the type of <code>arr</code>."
    ]
  ],
  "MV_CONCAT": [
    [
      "arr1, arr2",
      "Concatenates <code>arr2</code> to <code>arr1</code>. The resulting array type is determined by the type of <code>arr1</code>."
    ]
  ],
  "MV_SLICE": [
    [
      "arr, start, end",
      "Returns the subarray of <code>arr</code> from the 0-based index start(inclusive) to end(exclusive), or <code>null</code>, if start is less than 0, greater than length of arr or greater than end."
    ]
  ],
  "MV_TO_STRING": [
    [
      "arr, str",
      "Joins all elements of <code>arr</code> by the delimiter specified by <code>str</code>."
    ]
  ],
  "STRING_TO_MV": [
    [
      "str1, str2",
      "Splits <code>str1</code> into an array on the delimiter specified by <code>str2</code>, which is a regular expression."
    ]
  ],
  "MV_TO_ARRAY": [
    [
      "str",
      "Converts a multi-value string from a <code>VARCHAR</code> to a <code>VARCHAR ARRAY</code>."
    ]
  ],
  "JSON_KEYS": [
    [
      "expr, path",
      " Returns an array of field names from <code>expr</code> at the specified <code>path</code>."
    ]
  ],
  "JSON_OBJECT": [
    [
      "KEY expr1 VALUE expr2[, KEY expr3 VALUE expr4, ...]",
      " Constructs a new <code>COMPLEX&lt;json&gt;</code> object. The <code>KEY</code> expressions must evaluate to string types. The <code>VALUE</code> expressions can be composed of any input type, including other <code>COMPLEX&lt;json&gt;</code> values. <code>JSON_OBJECT</code> can accept colon-separated key-value pairs. The following syntax is equivalent: <code>JSON_OBJECT(expr1:expr2[, expr3:expr4, ...])</code>."
    ]
  ],
  "JSON_PATHS": [
    [
      "expr",
      " Returns an array of all paths which refer to literal values in <code>expr</code> in JSONPath format. "
    ]
  ],
  "JSON_QUERY": [
    [
      "expr, path",
      " Extracts a <code>COMPLEX&lt;json&gt;</code> value from <code>expr</code>, at the specified <code>path</code>. "
    ]
  ],
  "JSON_QUERY_ARRAY": [
    [
      "expr, path",
      " Extracts an <code>ARRAY&lt;COMPLEX&lt;json&gt;&gt;</code> value from <code>expr</code> at the specified <code>path</code>. If value is not an <code>ARRAY</code>, it gets translated into a single element <code>ARRAY</code> containing the value at <code>path</code>. The primary use of this function is to extract arrays of objects to use as inputs to other array functions."
    ]
  ],
  "JSON_VALUE": [
    [
      "expr, path [RETURNING sqlType]",
      " Extracts a literal value from <code>expr</code> at the specified <code>path</code>. If you specify <code>RETURNING</code> and an SQL type name (such as <code>VARCHAR</code>, <code>BIGINT</code>, <code>DOUBLE</code>, etc) the function plans the query using the suggested type. Otherwise, it attempts to infer the type based on the context. If it can't infer the type, it defaults to <code>VARCHAR</code>."
    ]
  ],
  "PARSE_JSON": [
    [
      "expr",
      "Parses <code>expr</code> into a <code>COMPLEX&lt;json&gt;</code> object. This operator deserializes JSON values when processing them, translating stringified JSON into a nested structure. If the input is not a <code>VARCHAR</code> or it is invalid JSON, this function will result in an error."
    ]
  ],
  "TRY_PARSE_JSON": [
    [
      "expr",
      "Parses <code>expr</code> into a <code>COMPLEX&lt;json&gt;</code> object. This operator deserializes JSON values when processing them, translating stringified JSON into a nested structure. If the input is not a <code>VARCHAR</code> or it is invalid JSON, this function will result in a <code>NULL</code> value."
    ]
  ],
  "TO_JSON_STRING": [
    [
      "expr",
      "Serializes <code>expr</code> into a JSON string."
    ]
  ]
};
