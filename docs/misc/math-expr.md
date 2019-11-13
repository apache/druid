---
id: math-expr
title: "Expressions"
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


> This feature is still experimental. It has not been optimized for performance yet, and its implementation is known to
>  have significant inefficiencies.

This expression language supports the following operators (listed in decreasing order of precedence).

|Operators|Description|
|---------|-----------|
|!, -|Unary NOT and Minus|
|^|Binary power op|
|*, /, %|Binary multiplicative|
|+, -|Binary additive|
|<, <=, >, >=, ==, !=|Binary Comparison|
|&&, &#124;&#124;|Binary Logical AND, OR|

Long, double, and string data types are supported. If a number contains a dot, it is interpreted as a double, otherwise
it is interpreted as a long. That means, always add a '.' to your number if you want it interpreted as a double value.
String literals should be quoted by single quotation marks.

Additionally, the expression language supports long, double, and string arrays. Array literals are created by wrapping
square brackets around a list of scalar literals values delimited by a comma or space character. All values in an array
literal must be the same type.

Expressions can contain variables. Variable names may contain letters, digits, '\_' and '$'. Variable names must not
begin with a digit. To escape other special characters, you can quote it with double quotation marks.

For logical operators, a number is true if and only if it is positive (0 or negative value means false). For string
type, it's the evaluation result of 'Boolean.valueOf(string)'.

[Multi-value string dimensions](../querying/multi-value-dimensions.html) are supported and may be treated as either
scalar or array typed values. When treated as a scalar type, an expression will automatically be transformed to apply
the scalar operation across all values of the multi-valued type, to mimic Druid's native behavior. Values that result in
arrays will be coerced back into the native Druid string type for aggregation. Druid aggregations on multi-value string
dimensions on the individual values, _not_ the 'array', behaving similar to the `UNNEST` operator available in many SQL
dialects. However, by using the `array_to_string` function, aggregations may be done on a stringified version of the
complete array, allowing the complete row to be preserved. Using `string_to_array` in an expression post-aggregator,
allows transforming the stringified dimension back into the true native array type.


The following built-in functions are available.

## General functions

|name|description|
|----|-----------|
|cast|cast(expr,'LONG' or 'DOUBLE' or 'STRING' or 'LONG_ARRAY', or 'DOUBLE_ARRAY' or 'STRING_ARRAY') returns expr with specified type. exception can be thrown. Scalar types may be cast to array types and will take the form of a single element list (null will still be null). |
|if|if(predicate,then,else) returns 'then' if 'predicate' evaluates to a positive number, otherwise it returns 'else' |
|nvl|nvl(expr,expr-for-null) returns 'expr-for-null' if 'expr' is null (or empty string for string type) |
|like|like(expr, pattern[, escape]) is equivalent to SQL `expr LIKE pattern`|
|case_searched|case_searched(expr1, result1, \[\[expr2, result2, ...\], else-result\])|
|case_simple|case_simple(expr, value1, result1, \[\[value2, result2, ...\], else-result\])|
|bloom_filter_test|bloom_filter_test(expr, filter) tests the value of 'expr' against 'filter', a bloom filter serialized as a base64 string. See [bloom filter extension](../development/extensions-core/bloom-filter.md) documentation for additional details.|

## String functions

|name|description|
|----|-----------|
|concat|concat(expr, expr...) concatenate a list of strings|
|format|format(pattern[, args...]) returns a string formatted in the manner of Java's [String.format](https://docs.oracle.com/javase/8/docs/api/java/lang/String.html#format-java.lang.String-java.lang.Object...-).|
|like|like(expr, pattern[, escape]) is equivalent to SQL `expr LIKE pattern`|
|lookup|lookup(expr, lookup-name) looks up expr in a registered [query-time lookup](../querying/lookups.md)|
|parse_long|parse_long(string[, radix]) parses a string as a long with the given radix, or 10 (decimal) if a radix is not provided.|
|regexp_extract|regexp_extract(expr, pattern[, index]) applies a regular expression pattern and extracts a capture group index, or null if there is no match. If index is unspecified or zero, returns the substring that matched the pattern.|
|replace|replace(expr, pattern, replacement) replaces pattern with replacement|
|substring|substring(expr, index, length) behaves like java.lang.String's substring|
|right|right(expr, length) returns the rightmost length characters from a string|
|left|left(expr, length) returns the leftmost length characters from a string|
|strlen|strlen(expr) returns length of a string in UTF-16 code units|
|strpos|strpos(haystack, needle[, fromIndex]) returns the position of the needle within the haystack, with indexes starting from 0. The search will begin at fromIndex, or 0 if fromIndex is not specified. If the needle is not found then the function returns -1.|
|trim|trim(expr[, chars]) remove leading and trailing characters from `expr` if they are present in `chars`. `chars` defaults to ' ' (space) if not provided.|
|ltrim|ltrim(expr[, chars]) remove leading characters from `expr` if they are present in `chars`. `chars` defaults to ' ' (space) if not provided.|
|rtrim|rtrim(expr[, chars]) remove trailing characters from `expr` if they are present in `chars`. `chars` defaults to ' ' (space) if not provided.|
|lower|lower(expr) converts a string to lowercase|
|upper|upper(expr) converts a string to uppercase|
|reverse|reverse(expr) reverses a string|
|repeat|repeat(expr, N) repeats a string N times|
|lpad|lpad(expr, length, chars) returns a string of `length` from `expr` left-padded with `chars`. If `length` is shorter than the length of `expr`, the result is `expr` which is truncated to `length`. If either `expr` or `chars` are null, the result will be null.|
|rpad|rpad(expr, length, chars) returns a string of `length` from `expr` right-padded with `chars`. If `length` is shorter than the length of `expr`, the result is `expr` which is truncated to `length`. If either `expr` or `chars` are null, the result will be null.|

## Time functions

|name|description|
|----|-----------|
|timestamp|timestamp(expr[,format-string]) parses string expr into date then returns milliseconds from java epoch. without 'format-string' it's regarded as ISO datetime format |
|unix_timestamp|same with 'timestamp' function but returns seconds instead |
|timestamp_ceil|timestamp_ceil(expr, period, \[origin, \[timezone\]\]) rounds up a timestamp, returning it as a new timestamp. Period can be any ISO8601 period, like P3M (quarters) or PT12H (half-days). The time zone, if provided, should be a time zone name like "America/Los_Angeles" or offset like "-08:00".|
|timestamp_floor|timestamp_floor(expr, period, \[origin, [timezone\]\]) rounds down a timestamp, returning it as a new timestamp. Period can be any ISO8601 period, like P3M (quarters) or PT12H (half-days). The time zone, if provided, should be a time zone name like "America/Los_Angeles" or offset like "-08:00".|
|timestamp_shift|timestamp_shift(expr, period, step, \[timezone\]) shifts a timestamp by a period (step times), returning it as a new timestamp. Period can be any ISO8601 period. Step may be negative. The time zone, if provided, should be a time zone name like "America/Los_Angeles" or offset like "-08:00".|
|timestamp_extract|timestamp_extract(expr, unit, \[timezone\]) extracts a time part from expr, returning it as a number. Unit can be EPOCH (number of seconds since 1970-01-01 00:00:00 UTC), SECOND, MINUTE, HOUR, DAY (day of month), DOW (day of week), DOY (day of year), WEEK (week of [week year](https://en.wikipedia.org/wiki/ISO_week_date)), MONTH (1 through 12), QUARTER (1 through 4), or YEAR. The time zone, if provided, should be a time zone name like "America/Los_Angeles" or offset like "-08:00"|
|timestamp_parse|timestamp_parse(string expr, \[pattern, [timezone\]\]) parses a string into a timestamp using a given [Joda DateTimeFormat pattern](http://www.joda.org/joda-time/apidocs/org/joda/time/format/DateTimeFormat.html). If the pattern is not provided, this parses time strings in either ISO8601 or SQL format. The time zone, if provided, should be a time zone name like "America/Los_Angeles" or offset like "-08:00", and will be used as the time zone for strings that do not include a time zone offset. Pattern and time zone must be literals. Strings that cannot be parsed as timestamps will be returned as nulls.|
|timestamp_format|timestamp_format(expr, \[pattern, \[timezone\]\]) formats a timestamp as a string with a given [Joda DateTimeFormat pattern](http://www.joda.org/joda-time/apidocs/org/joda/time/format/DateTimeFormat.html), or ISO8601 if the pattern is not provided. The time zone, if provided, should be a time zone name like "America/Los_Angeles" or offset like "-08:00". Pattern and time zone must be literals.|

## Math functions

See javadoc of java.lang.Math for detailed explanation for each function.

|name|description|
|----|-----------|
|abs|abs(x) would return the absolute value of x|
|acos|acos(x) would return the arc cosine of x|
|asin|asin(x) would return the arc sine of x|
|atan|atan(x) would return the arc tangent of x|
|atan2|atan2(y, x) would return the angle theta from the conversion of rectangular coordinates (x, y) to polar * coordinates (r, theta)|
|cbrt|cbrt(x) would return the cube root of x|
|ceil|ceil(x) would return the smallest (closest to negative infinity) double value that is greater than or equal to x and is equal to a mathematical integer|
|copysign|copysign(x) would return the first floating-point argument with the sign of the second floating-point argument|
|cos|cos(x) would return the trigonometric cosine of x|
|cosh|cosh(x) would return the hyperbolic cosine of x|
|cot|cot(x) would return the trigonometric cotangent of an angle x|
|div|div(x,y) is integer division of x by y|
|exp|exp(x) would return Euler's number raised to the power of x|
|expm1|expm1(x) would return e^x-1|
|floor|floor(x) would return the largest (closest to positive infinity) double value that is less than or equal to x and is equal to a mathematical integer|
|getExponent|getExponent(x) would return the unbiased exponent used in the representation of x|
|hypot|hypot(x, y) would return sqrt(x^2+y^2) without intermediate overflow or underflow|
|log|log(x) would return the natural logarithm of x|
|log10|log10(x) would return the base 10 logarithm of x|
|log1p|log1p(x) would the natural logarithm of x + 1|
|max|max(x, y) would return the greater of two values|
|min|min(x, y) would return the smaller of two values|
|nextafter|nextafter(x, y) would return the floating-point number adjacent to the x in the direction of the y|
|nextUp|nextUp(x) would return the floating-point value adjacent to x in the direction of positive infinity|
|pi|pi would return the constant value of the π |
|pow|pow(x, y) would return the value of the x raised to the power of y|
|remainder|remainder(x, y) would return the remainder operation on two arguments as prescribed by the IEEE 754 standard|
|rint|rint(x) would return value that is closest in value to x and is equal to a mathematical integer|
|round|round(x, y) would return the value of the x rounded to the y decimal places. While x can be an integer or floating-point number, y must be an integer. The type of the return value is specified by that of x. y defaults to 0 if omitted. When y is negative, x is rounded on the left side of the y decimal points.|
|scalb|scalb(d, sf) would return d * 2^sf rounded as if performed by a single correctly rounded floating-point multiply to a member of the double value set|
|signum|signum(x) would return the signum function of the argument x|
|sin|sin(x) would return the trigonometric sine of an angle x|
|sinh|sinh(x) would return the hyperbolic sine of x|
|sqrt|sqrt(x) would return the correctly rounded positive square root of x|
|tan|tan(x) would return the trigonometric tangent of an angle x|
|tanh|tanh(x) would return the hyperbolic tangent of x|
|todegrees|todegrees(x) converts an angle measured in radians to an approximately equivalent angle measured in degrees|
|toradians|toradians(x) converts an angle measured in degrees to an approximately equivalent angle measured in radians|
|ulp|ulp(x) would return the size of an ulp of the argument x|


## Array functions

| function | description |
| --- | --- |
| array(expr1,expr ...) | constructs an array from the expression arguments, using the type of the first argument as the output array type |
| array_length(arr) | returns length of array expression |
| array_offset(arr,long) | returns the array element at the 0 based index supplied, or null for an out of range index|
| array_ordinal(arr,long) | returns the array element at the 1 based index supplied, or null for an out of range index |
| array_contains(arr,expr) | returns 1 if the array contains the element specified by expr, or contains all elements specified by expr if expr is an array, else 0 |
| array_overlap(arr1,arr2) | returns 1 if arr1 and arr2 have any elements in common, else 0 |
| array_offset_of(arr,expr) | returns the 0 based index of the first occurrence of expr in the array, or `-1` or `null` if `druid.generic.useDefaultValueForNull=false`if no matching elements exist in the array. |
| array_ordinal_of(arr,expr) | returns the 1 based index of the first occurrence of expr in the array, or `-1` or `null` if `druid.generic.useDefaultValueForNull=false` if no matching elements exist in the array. |
| array_prepend(expr,arr) | adds expr to arr at the beginning, the resulting array type determined by the type of the array |
| array_append(arr1,expr) | appends expr to arr, the resulting array type determined by the type of the first array |
| array_concat(arr1,arr2) | concatenates 2 arrays, the resulting array type determined by the type of the first array |
| array_slice(arr,start,end) | return the subarray of arr from the 0 based index start(inclusive) to end(exclusive), or `null`, if start is less than 0, greater than length of arr or less than end|
| array_to_string(arr,str) | joins all elements of arr by the delimiter specified by str |
| string_to_array(str1,str2) | splits str1 into an array on the delimiter specified by str2 |


## Apply functions

| function | description |
| --- | --- |
| map(lambda,arr) | applies a transform specified by a single argument lambda expression to all elements of arr, returning a new array |
| cartesian_map(lambda,arr1,arr2,...) | applies a transform specified by a multi argument lambda expression to all elements of the Cartesian product of all input arrays, returning a new array; the number of lambda arguments and array inputs must be the same |
| filter(lambda,arr) | filters arr by a single argument lambda, returning a new array with all matching elements, or null if no elements match |
| fold(lambda,arr) | folds a 2 argument lambda across arr. The first argument of the lambda is the array element and the second the accumulator, returning a single accumulated value. |
| cartesian_fold(lambda,arr1,arr2,...) | folds a multi argument lambda across the Cartesian product of all input arrays. The first arguments of the lambda is the array element and the last is the accumulator, returning a single accumulated value. |
| any(lambda,arr) | returns 1 if any element in the array matches the lambda expression, else 0 |
| all(lambda,arr) | returns 1 if all elements in the array matches the lambda expression, else 0 |


## IP address functions

For the IPv4 address functions, the `address` argument can either be an IPv4 dotted-decimal string
(e.g., "192.168.0.1") or an IP address represented as a long (e.g., 3232235521). The `subnet`
argument should be a string formatted as an IPv4 address subnet in CIDR notation (e.g.,
"192.168.0.0/16").

| function | description |
| --- | --- |
| ipv4_match(address, subnet) | Returns 1 if the `address` belongs to the `subnet` literal, else 0. If `address` is not a valid IPv4 address, then 0 is returned. This function is more efficient if `address` is a long instead of a string.|
| ipv4_parse(address) | Parses `address` into an IPv4 address stored as a long. If `address` is a long that is a valid IPv4 address, then it is passed through. Returns null if `address` cannot be represented as an IPv4 address. |
| ipv4_stringify(address) | Converts `address` into an IPv4 address dotted-decimal string. If `address` is a string that is a valid IPv4 address, then it is passed through. Returns null if `address` cannot be represented as an IPv4 address.|
