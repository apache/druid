---
id: virtual-columns
title: "Virtual columns"
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

> Apache Druid supports two query languages: [Druid SQL](sql.md) and [native queries](querying.md).
> This document describes the native
> language. For information about functions available in SQL, refer to the
> [SQL documentation](sql-scalar.md).

Virtual columns are queryable column "views" created from a set of columns during a query.

A virtual column can potentially draw from multiple underlying columns, although a virtual column always presents itself as a single column.

Virtual columns can be used as dimensions or as inputs to aggregators.

Each Apache Druid query can accept a list of virtual columns as a parameter. The following scan query is provided as an example:

```
{
 "queryType": "scan",
 "dataSource": "page_data",
 "columns":[],
 "virtualColumns": [
    {
      "type": "expression",
      "name": "fooPage",
      "expression": "concat('foo' + page)",
      "outputType": "STRING"
    },
    {
      "type": "expression",
      "name": "tripleWordCount",
      "expression": "wordCount * 3",
      "outputType": "LONG"
    }
  ],
 "intervals": [
   "2013-01-01/2019-01-02"
 ]
}
```


## Virtual column types

### Expression virtual column
Expression virtual columns use Druid's native [expression](../misc/math-expr.md) system to allow defining query time
transforms of inputs from one or more columns.

The expression virtual column has the following syntax:

```
{
  "type": "expression",
  "name": <name of the virtual column>,
  "expression": <row expression>,
  "outputType": <output value type of expression>
}
```

|property|description|required?|
|--------|-----------|---------|
|name|The name of the virtual column.|yes|
|expression|An [expression](../misc/math-expr.md) that takes a row as input and outputs a value for the virtual column.|yes|
|outputType|The expression's output will be coerced to this type. Can be LONG, FLOAT, DOUBLE, STRING, ARRAY types, or COMPLEX types.|no, default is FLOAT|


### Nested field virtual column

The nested field virtual column is an optimized virtual column that can provide direct access into various paths of
a `COMPLEX<json>` column, including using their indexes.

Syntax (all 3 of these virtual columns produce the same output):
```json
    {
      "type": "nested-field",
      "columnName": "shipTo",
      "outputName": "v0",
      "expectedType": "STRING",
      "path": "$.phoneNumbers[1].number"
    }
```
```json
    {
      "type": "nested-field",
      "columnName": "shipTo",
      "outputName": "v1",
      "expectedType": "STRING",
      "path": ".phoneNumbers[1].number",
      "useJqSyntax": true
    }
```

```json
    {
      "type": "nested-field",
      "columnName": "shipTo",
      "outputName": "v2",
      "expectedType": "STRING",
      "pathParts": [
        {
          "type": "field",
          "field": "phoneNumbers"
        },
        {
          "type": "arrayElement",
          "index": 1
        },
        {
          "type": "field",
          "field": "number"
        }
      ]
    }
```

|property|description|required?|
|--------|-----------|---------|
|columnName|The name of the virtual column.|yes|
|outputName|The name of the virtual column.|yes|
|expectedType|The name of the virtual column.|yes|
|pathParts|The name of the virtual column.|yes|
|processFromRaw|If set to true, the virtual column will process the "raw" JSON data to extract values rather than using an optimized "literal" value selector. This option allows extracting non-literal values (such as nested JSON objects or arrays) as a `COMPLEX<json>` at the cost of much slower performance.|No, default false|
|path|'JSONPath' or 'jq' syntax path. One of `path` or `pathParts` must be set|no, if `pathParts` is defined|
|useJqSyntax||no, default is false|

#### Nested path part
|property|description|required?|
|--------|-----------|---------|
|type|Must be 'field' or 'arrayElement'|yes|
|field|The name of the 'field' in a 'field' `type` path part|yes, if `type` is 'field'|
|index|The array element index if `type` is `arrayElement`|yes, if `type` is 'arrayElement'|

This virtual column is used for the SQL operators `JSON_VALUE` (if `processFromRaw` is set to false) or `JSON_QUERY`
(if it is true), and accepts 'JSONPath' or 'jq' syntax string representations of paths, or a parsed
list of "path parts" in order to determine what should be selected from the column.

Type information for nested fields is absent at higher levels (it is contained within the segment, but not to segment
metadata queries or the SQL planner), so `expectedType` provides the context for how something is being used, e.g. an
aggregators default type or an explicit cast, or, if using the 'RETURNING' syntax which explicitly specifies type.
This might not be the same as if it had actual type information, so the results will be "best effort" cast to the
expected type if the column is not natively the expected type so that this column can fulfill the contract of the type
of selector that is likely to be created to read this column.


### List filtered virtual column
This virtual column provides an alternative way to use
['list filtered' dimension spec](./dimensionspecs.md#filtered-dimensionspecs) as a virtual column. It has optimized
access to the underlying column value indexes that can provide a small performance improvement in some cases.


```json
    {
      "type": "mv-filtered",
      "name": "filteredDim3",
      "delegate": "dim3",
      "values": ["hello", "world"],
      "isAllowList": true
    }
```

|property|description|required?|
|--------|-----------|---------|
|name|The output name of the virtual column|yes|
|delegate|The name of the multi-value STRING input column to filter|yes|
|values|Set of STRING values to allow or deny|yes|
|isAllowList|If true, the output of the virtual column will be limited to the set specified by `values`, else it will provide all values _except_ those specified.|No, default true|
