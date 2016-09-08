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

Long and double data types are supported. If a number contains a dot, it is interpreted as a double, otherwise it is interpreted as a long. That means, always add a '.' to your number if you want it intepreted as a double value.

Expressions can contain variables. Variable names may contain letters, digits, '\_' and '$'. Variable names must not begin with a digit.

For logical operators, a number is true if and only if it is positive. (0 means false)

Also, the following in-built functions are supported.

|name|description|
|----|-----------|
|sqrt|sqrt(x) would return square root of x|
|if|if(predicate,then,else) returns 'then' if 'predicate' evaluates to a positive number, otherwise it returns 'else'|

Built-in Math functions. See javadoc of java.lang.Math for detailed explanation for each function.

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
