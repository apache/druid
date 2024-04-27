---
id: timeseriesquery
title: "Timeseries queries"
sidebar_label: "Timeseries"
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

:::info
 Apache Druid supports two query languages: [Druid SQL](sql.md) and [native queries](querying.md).
 This document describes a query
 type in the native language. For information about when Druid SQL will use this query type, refer to the
 [SQL documentation](sql-translation.md#query-types).
:::

These types of queries take a timeseries query object and return an array of JSON objects where each object represents a value asked for by the timeseries query.

An example timeseries query object is shown below:

```json
{
  "queryType": "timeseries",
  "dataSource": "sample_datasource",
  "granularity": "day",
  "descending": "true",
  "filter": {
    "type": "and",
    "fields": [
      { "type": "selector", "dimension": "sample_dimension1", "value": "sample_value1" },
      { "type": "or",
        "fields": [
          { "type": "selector", "dimension": "sample_dimension2", "value": "sample_value2" },
          { "type": "selector", "dimension": "sample_dimension3", "value": "sample_value3" }
        ]
      }
    ]
  },
  "aggregations": [
    { "type": "longSum", "name": "sample_name1", "fieldName": "sample_fieldName1" },
    { "type": "doubleSum", "name": "sample_name2", "fieldName": "sample_fieldName2" }
  ],
  "postAggregations": [
    { "type": "arithmetic",
      "name": "sample_divide",
      "fn": "/",
      "fields": [
        { "type": "fieldAccess", "name": "postAgg__sample_name1", "fieldName": "sample_name1" },
        { "type": "fieldAccess", "name": "postAgg__sample_name2", "fieldName": "sample_name2" }
      ]
    }
  ],
  "intervals": [ "2012-01-01T00:00:00.000/2012-01-03T00:00:00.000" ]
}
```

There are 7 main parts to a timeseries query:

|property|description|required?|
|--------|-----------|---------|
|queryType|This String should always be "timeseries"; this is the first thing Apache Druid looks at to figure out how to interpret the query|yes|
|dataSource|A String or Object defining the data source to query, very similar to a table in a relational database. See [DataSource](../querying/datasource.md) for more information.|yes|
|descending|Whether to make descending ordered result. Default is `false`(ascending).|no|
|intervals|A JSON Object representing ISO-8601 Intervals. This defines the time ranges to run the query over.|yes|
|granularity|Defines the granularity to bucket query results. See [Granularities](../querying/granularities.md)|yes|
|filter|See [Filters](../querying/filters.md)|no|
|virtualColumns|A JSON list of [virtual columns](./virtual-columns.md). You can reference the virtual columns in `aggregations` or `postAggregations`.| no (default none)|
|aggregations|See [Aggregations](../querying/aggregations.md)|no|
|postAggregations|See [Post Aggregations](../querying/post-aggregations.md)|no|
|limit|An integer that limits the number of results. The default is unlimited.|no|
|context|Can be used to modify query behavior, including [grand totals](#grand-totals) and [empty bucket values](#empty-bucket-values). See also [Context](../querying/query-context.md) for parameters that apply to all query types.|no|

To pull it all together, the above query would return 2 data points, one for each day between 2012-01-01 and 2012-01-03, from the "sample\_datasource" table. Each data point would be the (long) sum of sample\_fieldName1, the (double) sum of sample\_fieldName2 and the (double) result of sample\_fieldName1 divided by sample\_fieldName2 for the filter set. The output looks like this:

```json
[
  {
    "timestamp": "2012-01-01T00:00:00.000Z",
    "result": { "sample_name1": <some_value>, "sample_name2": <some_value>, "sample_divide": <some_value> }
  },
  {
    "timestamp": "2012-01-02T00:00:00.000Z",
    "result": { "sample_name1": <some_value>, "sample_name2": <some_value>, "sample_divide": <some_value> }
  }
]
```

## Grand totals

Druid can include an extra "grand totals" row as the last row of a timeseries result set. To enable this, add
`"grandTotal" : true` to your query context. For example:

```json
{
  "queryType": "timeseries",
  "dataSource": "sample_datasource",
  "intervals": [ "2012-01-01T00:00:00.000/2012-01-03T00:00:00.000" ],
  "granularity": "day",
  "aggregations": [
    { "type": "longSum", "name": "sample_name1", "fieldName": "sample_fieldName1" },
    { "type": "doubleSum", "name": "sample_name2", "fieldName": "sample_fieldName2" }
  ],
  "context": {
    "grandTotal": true
  }
}
```

The grand totals row will appear as the last row in the result array, and will have no timestamp. It will be the last
row even if the query is run in "descending" mode. Post-aggregations in the grand totals row will be computed based
upon the grand total aggregations.

## Empty bucket values

By default Druid fills empty interior time buckets in the results of timeseries queries with the default value for the [aggregator function](./sql-aggregations.md).
For example, if you issue a "day" granularity
timeseries query for the interval 2012-01-01/2012-01-04 using the SUM aggregator, and no data exists for 2012-01-02, Druid returns:

```json
[
  {
    "timestamp": "2012-01-01T00:00:00.000Z",
    "result": { "sample_name1": <some_value> }
  },
  {
   "timestamp": "2012-01-02T00:00:00.000Z",
   "result": { "sample_name1": NULL }
  },
  {
    "timestamp": "2012-01-03T00:00:00.000Z",
    "result": { "sample_name1": <some_value> }
  }
]
```

Time buckets that lie completely outside the data interval are not filled with the default value.

You can disable all empty bucket filling with the context flag `skipEmptyBuckets`.
In this mode, Druid omits the data point 2012-01-02 from the results.
For example:

```json
{
  "queryType": "timeseries",
  "dataSource": "sample_datasource",
  "granularity": "day",
  "aggregations": [
    { "type": "longSum", "name": "sample_name1", "fieldName": "sample_fieldName1" }
  ],
  "intervals": [ "2012-01-01T00:00:00.000/2012-01-04T00:00:00.000" ],
  "context" : {
    "skipEmptyBuckets": "true"
  }
}
```