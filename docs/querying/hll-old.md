---
id: hll-old
title: "Cardinality/HyperUnique aggregators"
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


## Cardinality aggregator

Computes the cardinality of a set of Apache Druid (incubating) dimensions, using HyperLogLog to estimate the cardinality. Please note that this
aggregator will be much slower than indexing a column with the hyperUnique aggregator. This aggregator also runs over a dimension column, which
means the string dimension cannot be removed from the dataset to improve rollup. In general, we strongly recommend using the hyperUnique aggregator
instead of the cardinality aggregator if you do not care about the individual values of a dimension.

```json
{
  "type": "cardinality",
  "name": "<output_name>",
  "fields": [ <dimension1>, <dimension2>, ... ],
  "byRow": <false | true> # (optional, defaults to false),
  "round": <false | true> # (optional, defaults to false)
}
```

Each individual element of the "fields" list can be a String or [DimensionSpec](../querying/dimensionspecs.md). A String dimension in the fields list is equivalent to a DefaultDimensionSpec (no transformations).

The HyperLogLog algorithm generates decimal estimates with some error. "round" can be set to true to round off estimated
values to whole numbers. Note that even with rounding, the cardinality is still an estimate. The "round" field only
affects query-time behavior, and is ignored at ingestion-time.

### Cardinality by value

When setting `byRow` to `false` (the default) it computes the cardinality of the set composed of the union of all dimension values for all the given dimensions.

* For a single dimension, this is equivalent to

```sql
SELECT COUNT(DISTINCT(dimension)) FROM <datasource>
```

* For multiple dimensions, this is equivalent to something akin to

```sql
SELECT COUNT(DISTINCT(value)) FROM (
  SELECT dim_1 as value FROM <datasource>
  UNION
  SELECT dim_2 as value FROM <datasource>
  UNION
  SELECT dim_3 as value FROM <datasource>
)
```

### Cardinality by row

When setting `byRow` to `true` it computes the cardinality by row, i.e. the cardinality of distinct dimension combinations.
This is equivalent to something akin to

```sql
SELECT COUNT(*) FROM ( SELECT DIM1, DIM2, DIM3 FROM <datasource> GROUP BY DIM1, DIM2, DIM3 )
```

**Example**

Determine the number of distinct countries people are living in or have come from.

```json
{
  "type": "cardinality",
  "name": "distinct_countries",
  "fields": [ "country_of_origin", "country_of_residence" ]
}
```

Determine the number of distinct people (i.e. combinations of first and last name).

```json
{
  "type": "cardinality",
  "name": "distinct_people",
  "fields": [ "first_name", "last_name" ],
  "byRow" : true
}
```

Determine the number of distinct starting characters of last names

```json
{
  "type": "cardinality",
  "name": "distinct_last_name_first_char",
  "fields": [
    {
     "type" : "extraction",
     "dimension" : "last_name",
     "outputName" :  "last_name_first_char",
     "extractionFn" : { "type" : "substring", "index" : 0, "length" : 1 }
    }
  ],
  "byRow" : true
}
```


## HyperUnique aggregator

Uses [HyperLogLog](http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf) to compute the estimated cardinality of a dimension that has been aggregated as a "hyperUnique" metric at indexing time.

```json
{
  "type" : "hyperUnique",
  "name" : <output_name>,
  "fieldName" : <metric_name>,
  "isInputHyperUnique" : false,
  "round" : false
}
```

"isInputHyperUnique" can be set to true to index precomputed HLL (Base64 encoded output from druid-hll is expected).
The "isInputHyperUnique" field only affects ingestion-time behavior, and is ignored at query-time.

The HyperLogLog algorithm generates decimal estimates with some error. "round" can be set to true to round off estimated
values to whole numbers. Note that even with rounding, the cardinality is still an estimate. The "round" field only
affects query-time behavior, and is ignored at ingestion-time.