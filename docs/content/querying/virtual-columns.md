---
layout: doc_page
title: "Virtual Columns"
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

# Virtual Columns

Virtual columns are queryable column "views" created from a set of columns during a query. 

A virtual column can potentially draw from multiple underlying columns, although a virtual column always presents itself as a single column.

Virtual columns can be used as dimensions or as inputs to aggregators.

Each Apache Druid (incubating) query can accept a list of virtual columns as a parameter. The following scan query is provided as an example:

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


## Virtual Column Types

### Expression virtual column

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
|expression|An [expression](../misc/math-expr.html) that takes a row as input and outputs a value for the virtual column.|yes|
|outputType|The expression's output will be coerced to this type. Can be LONG, FLOAT, DOUBLE, or STRING.|no, default is FLOAT|
