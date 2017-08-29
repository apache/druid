---
layout: doc_page
---

This expression language supports the following operators (listed in decreasing order of precedence).

|Operators|Description|
|---------|-----------|
|!, -|Unary NOT and Minus|
|^|Binary power op|
|*, /, %|Binary multiplicative|
|+, -|Binary additive|
|<, <=, >, >=, ==, !=|Binary Comparison|
|&&,\|\||Binary Logical AND, OR|

Long, double and string data types are supported. If a number contains a dot, it is interpreted as a double, otherwise it is interpreted as a long. That means, always add a '.' to your number if you want it interpreted as a double value. String literal should be quoted by single quotation marks.

Expressions can contain variables. Variable names may contain letters, digits, '\_' and '$'. Variable names must not begin with a digit. To escape other special characters, user can quote it with double quotation marks.

For logical operators, a number is true if and only if it is positive (0 or minus value means false). For string type, it's evaluation result of 'Boolean.valueOf(string)'.

Also, the following built-in functions are supported.

## General functions

|name|description|
|----|-----------|
|cast|cast(expr,'LONG' or 'DOUBLE' or 'STRING') returns expr with specified type. exception can be thrown |
|if|if(predicate,then,else) returns 'then' if 'predicate' evaluates to a positive number, otherwise it returns 'else' |
|nvl|nvl(expr,expr-for-null) returns 'expr-for-null' if 'expr' is null (or empty string for string type) |
|like|like(expr, pattern[, escape]) is equivalent to SQL `expr LIKE pattern`|
|case_searched|case_searched(expr1, result1, \[\[expr2, result2, ...\], else-result\])|
|case_simple|case_simple(expr, value1, result1, \[\[value2, result2, ...\], else-result\])|

## String functions

|name|description|
|----|-----------|
|concat|concatenate a list of strings|
|like|like(expr, pattern[, escape]) is equivalent to SQL `expr LIKE pattern`|
|lookup|lookup(expr, lookup-name) looks up expr in a registered [query-time lookup](lookups.html)|
|regexp_extract|regexp_extract(expr, pattern[, index]) applies a regular expression pattern and extracts a capture group index, or null if there is no match. If index is unspecified or zero, returns the substring that matched the pattern.|
|replace|replace(expr, pattern, replacement) replaces pattern with replacement|
|substring|substring(expr, index, length) behaves like java.lang.String's substring|
|strlen|returns length of a string in UTF-16 code units|
|trim|remove leading and trailing whitespace from a string|
|lower|convert a string to lowercase|
|upper|convert a string to uppercase|

## Time functions

|name|description|
|----|-----------|
|timestamp|timestamp(expr[,format-string]) parses string expr into date then returns milli-seconds from java epoch. without 'format-string' it's regarded as ISO datetime format |
|unix_timestamp|same with 'timestamp' function but returns seconds instead |
|timestamp_ceil|timestamp_ceil(expr, period, \[origin, \[timezone\]\]) rounds up a timestamp, returning it as a new timestamp. Period can be any ISO8601 period, like P3M (quarters) or PT12H (half-days). The time zone, if provided, should be a time zone name like "America/Los_Angeles" or offset like "-08:00".|
|timestamp_floor|timestamp_floor(expr, period, \[origin, [timezone\]\]) rounds down a timestamp, returning it as a new timestamp. Period can be any ISO8601 period, like P3M (quarters) or PT12H (half-days). The time zone, if provided, should be a time zone name like "America/Los_Angeles" or offset like "-08:00".|
|timestamp_shift|timestamp_shift(expr, period, step, \[timezone\]) shifts a timestamp by a period (step times), returning it as a new timestamp. Period can be any ISO8601 period. Step may be negative. The time zone, if provided, should be a time zone name like "America/Los_Angeles" or offset like "-08:00".|
|timestamp_extract|timestamp_extract(expr, unit, \[timezone\]) extracts a time part from expr, returning it as a number. Unit can be EPOCH, SECOND, MINUTE, HOUR, DAY (day of month), DOW (day of week), DOY (day of year), WEEK (week of [week year](https://en.wikipedia.org/wiki/ISO_week_date)), MONTH (1 through 12), QUARTER (1 through 4), or YEAR. The time zone, if provided, should be a time zone name like "America/Los_Angeles" or offset like "-08:00"|
|timestamp_parse|timestamp_parse(string expr, \[pattern, [timezone\]\]) parses a string into a timestamp using a given [Joda DateTimeFormat pattern](http://www.joda.org/joda-time/apidocs/org/joda/time/format/DateTimeFormat.html), or ISO8601 if the pattern is not provided. The time zone, if provided, should be a time zone name like "America/Los_Angeles" or offset like "-08:00", and will be used as the time zone for strings that do not include a time zone offset. Pattern and time zone must be literals. Strings that cannot be parsed as timestamps will be returned as nulls.|
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
|pow|pow(x, y) would return the value of the x raised to the power of y|
|remainder|remainder(x, y) would return the remainder operation on two arguments as prescribed by the IEEE 754 standard|
|rint|rint(x) would return value that is closest in value to x and is equal to a mathematical integer|
|round|round(x) would return the closest long value to x, with ties rounding up|
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
