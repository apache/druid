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

