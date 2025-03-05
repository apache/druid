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

Druid supports nested columns, which provide optimized storage and indexes for nested data structures. See [Nested columns](./nested-columns.md) for more information.

You can use the following JSON functions to extract, transform, and create `COMPLEX<json>` objects.

| Function | Notes |
| --- | --- |
|`JSON_KEYS(expr, path)`| Returns an array of field names from `expr` at the specified `path`.|
|`JSON_MERGE(expr1, expr2[, expr3 ...])`| Merges two or more JSON `STRING` or `COMPLEX<json>` values into one, preserving the rightmost value when there are key overlaps. Always returns a `COMPLEX<json>` object.|
|`JSON_OBJECT(KEY expr1 VALUE expr2[, KEY expr3 VALUE expr4, ...])` | Constructs a new `COMPLEX<json>` object from one or more expressions. The `KEY` expressions must evaluate to string types. The `VALUE` expressions can be composed of any input type, including other `COMPLEX<json>` objects. The function can accept colon-separated key-value pairs. The following syntax is equivalent: `JSON_OBJECT(expr1:expr2[, expr3:expr4, ...])`.|
|`JSON_PATHS(expr)`| Returns an array of all paths which refer to literal values in `expr` in JSONPath format. |
|`JSON_QUERY(expr, path)`| Extracts a `COMPLEX<json>` value from `expr`, at the specified `path`. |
|`JSON_QUERY_ARRAY(expr, path)`| Extracts an `ARRAY<COMPLEX<json>>` value from `expr` at the specified `path`. If the value isn't an `ARRAY`, the function translates it into a single element `ARRAY` containing the value at `path`. Mainly used to extract arrays of objects to use as inputs to other [array functions](./sql-array-functions.md).|
|`JSON_VALUE(expr, path [RETURNING sqlType])`| Extracts a literal value from `expr` at the specified `path`. If you include `RETURNING` and specify a SQL type (such as `VARCHAR`, `BIGINT`, `DOUBLE`) the function plans the query using the suggested type. If `RETURNING` isn't included, the function attempts to infer the type based on the context. If the function can't infer the type, it defaults to `VARCHAR`.|
|`PARSE_JSON(expr)`|Parses `expr` into a `COMPLEX<json>` object. This function deserializes JSON values when processing them, translating stringified JSON into a nested structure. If the input is invalid JSON or not a `VARCHAR`, it returns an error.|
|`TRY_PARSE_JSON(expr)`|Parses `expr` into a `COMPLEX<json>` object. This operator deserializes JSON values when processing them, translating stringified JSON into a nested structure. If the input is invalid JSON or not a `VARCHAR`, it returns a `NULL` value.|
|`TO_JSON_STRING(expr)`|Serializes `expr` into a JSON string.|

### JSONPath syntax

Druid supports a subset of the [JSONPath syntax](https://github.com/json-path/JsonPath/blob/master/README.md) operators, primarily limited to extracting individual values from nested data structures.

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

- To return the entire JSON object:<br />
  `$`      -> `{"x":1, "y":[1, 2, 3]}`
- To return the value of the key "x":<br />
  `$.x`    -> `1`
- For a key that contains an array, to return the entire array:<br />
  `$['y']` -> `[1, 2, 3]`
- For a key that contains an array, to return an item in the array:<br />
  `$.y[1]` -> `2`