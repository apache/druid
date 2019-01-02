---
layout: doc_page
title: "Transform Specs"
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

# Transform Specs

Transform specs allow Druid to filter and transform input data during ingestion. 

## Syntax

The syntax for the transformSpec is shown below:

```
"transformSpec": {
  "transforms: <List of transforms>,
  "filter": <filter>
}
```

|property|description|required?|
|--------|-----------|---------|
|transforms|A list of [transforms](#transforms) to be applied to input rows. |no|
|filter|A [filter](../querying/filters.html) that will be applied to input rows; only rows that pass the filter will be ingested.|no|

## Transforms

The `transforms` list allows the user to specify a set of column transformations to be performed on input data.

Transforms allow adding new fields to input rows. Each transform has a "name" (the name of the new field) which can be referred to by DimensionSpecs, AggregatorFactories, etc.

A transform behaves as a "row function", taking an entire row as input and outputting a column value.

If a transform has the same name as a field in an input row, then it will shadow the original field. Transforms that shadow fields may still refer to the fields they shadow. This can be used to transform a field "in-place".

Transforms do have some limitations. They can only refer to fields present in the actual input rows; in particular, they cannot refer to other transforms. And they cannot remove fields, only add them. However, they can shadow a field with another field containing all nulls, which will act similarly to removing the field.

Note that the transforms are applied before the filter.

### Expression Transform

Druid currently supports one kind of transform, the expression transform.

An expression transform has the following syntax:

```
{
  "type": "expression",
  "name": <output field name>,
  "expression": <expr>
}
```

|property|description|required?|
|--------|-----------|---------|
|name|The output field name of the expression transform.|yes|
|expression|An [expression](../misc/math-expr.html) that will be applied to input rows to produce a value for the transform's output field.|no|

For example, the following expression transform prepends "foo" to the values of a `page` column in the input data, and creates a `fooPage` column.

```
    {
      "type": "expression",
      "name": "fooPage",
      "expression": "concat('foo' + page)"
    }
```

## Filtering

The transformSpec allows Druid to filter out input rows during ingestion. A row that fails to pass the filter will not be ingested.

Any of Druid's standard [filters](../querying/filters.html) can be used.

Note that the filtering takes place after the transforms, so filters will operate on transformed rows and not the raw input data if transforms are present.

For example, the following filter would ingest only input rows where a `country` column has the value "United States":

```
"filter": {
  "type": "selector",
  "dimension": "country",
  "value": "United States"
}
```
