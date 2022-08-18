---
id: sql-json-functions
title: "SQL JSON functions"
sidebar_label: "JSON functions"
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
  should not be changed without updating the script create-sql-docs
  in web-console/script/create-sql-docs, because the script detects
  patterns in this markdown file and parse it to TypeScript file for web console
-->

Druid supports nested columns, which provide optimized storage and indexes for nested data structures. These JSON
functions provide facilities to extract, transform, and create `COMPLEX<json>` values.

| function | notes |
| --- | --- |
|`JSON_KEYS(expr, path)`| Returns an array of field names in a `COMPLEX<json>` typed `expr`, at the specified `path`.|
|`JSON_OBJECT(KEY expr1 VALUE expr2[, KEY expr3 VALUE expr4, ...])` | Constructs a new `COMPLEX<json>` object. The `KEY` expressions must evaluate to string types, but the `VALUE` expressions can be composed of any input type, including other `COMPLEX<json>` values.|
|`JSON_PATHS(expr)`| Returns an array of all paths which refer to literal values in a `COMPLEX<json>` typed `expr`, in JSONPath format. |
|`JSON_QUERY(expr, path)`| Extracts a `COMPLEX<json>` value from a `COMPLEX<json>` typed  `expr`, at the specified `path`. |
|`JSON_VALUE(expr, path [RETURNING sqlType])`| Extracts a literal value from a `COMPLEX<json>` typed  `expr`, at the specified `path`. If you specify `RETURNING` and an SQL type name (such as varchar, bigint, decimal, or double) the function plans the query using the suggested type. Otherwise it attempts to infer the type based on the context. If it can't infer the type, it defaults to varchar.|
|`PARSE_JSON(expr)`|Parses a string type `expr` into a `COMPLEX<json>` object. This operator deserializes JSON values when processing them, translating stringified JSON into a nested structure. Non-`STRING` input or invalid JSON will result in an error.|
|`TRY_PARSE_JSON(expr)`|Parses a string type `expr` into a `COMPLEX<json>` object. This operator deserializes JSON values when processing them, translating stringified JSON into a nested structure. Non-`STRING` input or invalid JSON will result in a `NULL` value.|
|`TO_JSON_STRING(expr)`|Casts an `expr` of any type into a `COMPLEX<json>` object, then serializes the value into a JSON string.|

### JSONPath syntax

Druid supports a small, simplified subset of the [JSONPath syntax](https://github.com/json-path/JsonPath/blob/master/README.md) operators, primarily limited to extracting individual values from nested data structures.

|Operator|Description|
| --- | --- |
|`$`| Root element. All JSONPath expressions start with this operator. |
|`.<name>`| Child element in dot notation. |
|`['<name>']`| Child element in bracket notation. |
|`[<number>]`| Array index. |

Consider the following example input JSON:

```json
{"x":1, "y":[1, 2, 3]}
```

- To return the JSON object:<br>
  `$`      -> `{"x":1, "y":[1, 2, 3]}`
- To return the value of a key "x":<br>
  `$.x`    -> `1`
- For a key that contains an array, to return the entire array:<br>
  `$['y']` -> `[1, 2, 3]`
- For a key that contains an array, to return an item in the array:<br>
  `$.y[1]` -> `2`