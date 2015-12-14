---
layout: doc_page
---

This expression language supports following operators (listed in increasing order of precedence).

|Operators|Description|
|---------|-----------|
|and,or|Binary Logical AND, OR|
|<, <=, >, >=, ==, !=|Binary Comparison|
|+, -|Binary additive|
|*, /, %|Binary multiplicative|
|^|Binary power op|
|!, -|Unary NOT and Minus|

long and double data types are supported, number containing a dot is interpreted as a double or else a long. That means, always add a '.' to your number if you want it intepreted as a double value.
You can use variables inside the expression which must start with an alphabet or '_' or '$' and can contain a digit as well in subsequent characters. For logical operators, positive number is considered a boolean true and false otherwise.

Also, following in-built functions are supported.

|name|description|
|sqrt|sqrt(x) would return square root of x|
|if|if(predicate,then,else) would return `then` if predicate evaluates to a positive number or `else` is returned|

### How to use?

```
Map<String, Number> bindings = new HashMap<>();
bindings.put("x", 2);

Number result = Parser.parse("x + 2").eval(bindings);
Assert.assertEquals(4, result.longValue());
```

