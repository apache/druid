---
id: moving-average-query
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


## Overview
**Moving Average Query** is an extension which provides support for [Moving Average](https://en.wikipedia.org/wiki/Moving_average) and other Aggregate [Window Functions](https://en.wikibooks.org/wiki/Structured_Query_Language/Window_functions) in Druid queries.

These Aggregate Window Functions consume standard Druid Aggregators and outputs additional windowed aggregates called [Averagers](#averagers).

#### High level algorithm

Moving Average encapsulates the [groupBy query](../../querying/groupbyquery.md) (Or [timeseries](../../querying/timeseriesquery.md) in case of no dimensions) in order to rely on the maturity of these query types.

It runs the query in two main phases:

1. Runs an inner [groupBy](../../querying/groupbyquery.html) or [timeseries](../../querying/timeseriesquery.html) query to compute Aggregators (i.e. daily count of events).
2. Passes over aggregated results in Broker, in order to compute Averagers (i.e. moving 7 day average of the daily count).

#### Main enhancements provided by this extension:
1. Functionality: Extending druid query functionality (i.e. initial introduction of Window Functions).
2. Performance: Improving performance of such moving aggregations by eliminating multiple segment scans.

#### Further reading
[Moving Average](https://en.wikipedia.org/wiki/Moving_average)

[Window Functions](https://en.wikibooks.org/wiki/Structured_Query_Language/Window_functions)

[Analytic Functions](https://cloud.google.com/bigquery/docs/reference/standard-sql/analytic-function-concepts)


## Operations
To use this extension, make sure to [load](../../development/extensions.md#loading-extensions) `druid-moving-average-query` only to the Broker.

## Configuration
There are currently no configuration properties specific to Moving Average.

## Limitations
* movingAverage is missing support for the following groupBy properties: `subtotalsSpec`, `virtualColumns`.
* movingAverage is missing support for the following timeseries properties: `descending`.
* movingAverage is missing support for [SQL-compatible null handling](https://github.com/apache/incubator-druid/issues/4349) (So setting druid.generic.useDefaultValueForNull in configuration will give an error).

##Query spec:
* Most properties in the query spec derived from  [groupBy query](../../querying/groupbyquery.md) / [timeseries](../../querying/timeseriesquery.md), see documentation for these query types.

|property|description|required?|
|--------|-----------|---------|
|queryType|This String should always be "movingAverage"; this is the first thing Druid looks at to figure out how to interpret the query.|yes|
|dataSource|A String or Object defining the data source to query, very similar to a table in a relational database. See [DataSource](../../querying/datasource.md) for more information.|yes|
|dimensions|A JSON list of [DimensionSpec](../../querying/dimensionspecs.md) (Notice that property is optional)|no|
|limitSpec|See [LimitSpec](../../querying/limitspec.md)|no|
|having|See [Having](../../querying/having.md)|no|
|granularity|A period granularity; See [Period Granularities](../../querying/granularities.html#period-granularities)|yes|
|filter|See [Filters](../../querying/filters.md)|no|
|aggregations|Aggregations forms the input to Averagers; See [Aggregations](../../querying/aggregations.md)|yes|
|postAggregations|Supports only aggregations as input; See [Post Aggregations](../../querying/post-aggregations.md)|no|
|intervals|A JSON Object representing ISO-8601 Intervals. This defines the time ranges to run the query over.|yes|
|context|An additional JSON Object which can be used to specify certain flags.|no|
|averagers|Defines the moving average function; See [Averagers](#averagers)|yes|
|postAveragers|Support input of both averagers and aggregations; Syntax is identical to postAggregations (See [Post Aggregations](../../querying/post-aggregations.md))|no|

## Averagers

Averagers are used to define the Moving-Average function. Averagers are not limited to an average - they can also provide other types of window functions such as MAX()/MIN().

### Properties

These are properties which are common to all Averagers:

|property|description|required?|
|--------|-----------|---------|
|type|Averager type; See [Averager types](#averager-types)|yes|
|name|Averager name|yes|
|fieldName|Input name (An aggregation name)|yes|
|buckets|Number of lookback buckets (time periods), including current one. Must be >0|yes|
|cycleSize|Cycle size; Used to calculate day-of-week option; See [Cycle size (Day of Week)](#cycle-size-day-of-week)|no, defaults to 1|


### Averager types:

* [Standard averagers](#standard-averagers):
  * doubleMean
  * doubleMeanNoNulls
  * doubleMax
  * doubleMin
  * longMean
  * longMeanNoNulls
  * longMax
  * longMin

#### Standard averagers

These averagers offer four functions:

* Mean (Average)
* MeanNoNulls (Ignores empty buckets).
* Max
* Min

**Ignoring nulls**:
Using a MeanNoNulls averager is useful when the interval starts at the dataset beginning time.
In that case, the first records will ignore missing buckets and average won't be artificially low.
However, this also means that empty days in a sparse dataset will also be ignored.

Example of usage:

```json
{ "type" : "doubleMean", "name" : <output_name>, "fieldName": <input_name> }
```

### Cycle size (Day of Week)
This optional parameter is used to calculate over a single bucket within each cycle instead of all buckets.
A prime example would be weekly buckets, resulting in a Day of Week calculation. (Other examples: Month of year, Hour of day).

I.e. when using these parameters:

* *granularity*: period=P1D (daily)
* *buckets*: 28
* *cycleSize*: 7

Within each output record, the averager will compute the result over the following buckets: current (#0), #7, #14, #21.
Whereas without specifying cycleSize it would have computed over all 28 buckets.

## Examples

All examples are based on the Wikipedia dataset provided in the Druid [tutorials](../../tutorials/index.md).

### Basic example

Calculating a 7-buckets moving average for Wikipedia edit deltas.

Query syntax:

```json
{
  "queryType": "movingAverage",
  "dataSource": "wikipedia",
  "granularity": {
    "type": "period",
    "period": "PT30M"
  },
  "intervals": [
    "2015-09-12T00:00:00Z/2015-09-13T00:00:00Z"
  ],
  "aggregations": [
    {
      "name": "delta30Min",
      "fieldName": "delta",
      "type": "longSum"
    }
  ],
  "averagers": [
    {
      "name": "trailing30MinChanges",
      "fieldName": "delta30Min",
      "type": "longMean",
      "buckets": 7
    }
  ]
}
```

Result:

```json
[ {
   "version" : "v1",
   "timestamp" : "2015-09-12T00:30:00.000Z",
   "event" : {
     "delta30Min" : 30490,
     "trailing30MinChanges" : 4355.714285714285
   }
 }, {
   "version" : "v1",
   "timestamp" : "2015-09-12T01:00:00.000Z",
   "event" : {
     "delta30Min" : 96526,
     "trailing30MinChanges" : 18145.14285714286
   }
 }, {
...
...
...
}, {
  "version" : "v1",
  "timestamp" : "2015-09-12T23:00:00.000Z",
  "event" : {
    "delta30Min" : 119100,
    "trailing30MinChanges" : 198697.2857142857
  }
}, {
  "version" : "v1",
  "timestamp" : "2015-09-12T23:30:00.000Z",
  "event" : {
    "delta30Min" : 177882,
    "trailing30MinChanges" : 193890.0
  }
}
```

### Post averager example

Calculating a 7-buckets moving average for Wikipedia edit deltas, plus a ratio between the current period and the moving average.

Query syntax:

```json
{
  "queryType": "movingAverage",
  "dataSource": "wikipedia",
  "granularity": {
    "type": "period",
    "period": "PT30M"
  },
  "intervals": [
    "2015-09-12T22:00:00Z/2015-09-13T00:00:00Z"
  ],
  "aggregations": [
    {
      "name": "delta30Min",
      "fieldName": "delta",
      "type": "longSum"
    }
  ],
  "averagers": [
    {
      "name": "trailing30MinChanges",
      "fieldName": "delta30Min",
      "type": "longMean",
      "buckets": 7
    }
  ],
  "postAveragers" : [
    {
      "name": "ratioTrailing30MinChanges",
      "type": "arithmetic",
      "fn": "/",
      "fields": [
        {
          "type": "fieldAccess",
          "fieldName": "delta30Min"
        },
        {
          "type": "fieldAccess",
          "fieldName": "trailing30MinChanges"
        }
      ]
    }
  ]
}
```

Result:

```json
[ {
  "version" : "v1",
  "timestamp" : "2015-09-12T22:00:00.000Z",
  "event" : {
    "delta30Min" : 144269,
    "trailing30MinChanges" : 204088.14285714287,
    "ratioTrailing30MinChanges" : 0.7068955500319539
  }
}, {
  "version" : "v1",
  "timestamp" : "2015-09-12T22:30:00.000Z",
  "event" : {
    "delta30Min" : 242860,
    "trailing30MinChanges" : 214031.57142857142,
    "ratioTrailing30MinChanges" : 1.134692411867141
  }
}, {
  "version" : "v1",
  "timestamp" : "2015-09-12T23:00:00.000Z",
  "event" : {
    "delta30Min" : 119100,
    "trailing30MinChanges" : 198697.2857142857,
    "ratioTrailing30MinChanges" : 0.5994042624782422
  }
}, {
  "version" : "v1",
  "timestamp" : "2015-09-12T23:30:00.000Z",
  "event" : {
    "delta30Min" : 177882,
    "trailing30MinChanges" : 193890.0,
    "ratioTrailing30MinChanges" : 0.9174377224199288
  }
} ]
```


### Cycle size example

Calculating an average of every first 10-minutes of the last 3 hours:

Query syntax:

```json
{
  "queryType": "movingAverage",
  "dataSource": "wikipedia",
  "granularity": {
    "type": "period",
    "period": "PT10M"
  },
  "intervals": [
    "2015-09-12T00:00:00Z/2015-09-13T00:00:00Z"
  ],
  "aggregations": [
    {
      "name": "delta10Min",
      "fieldName": "delta",
      "type": "doubleSum"
    }
  ],
  "averagers": [
    {
      "name": "trailing10MinPerHourChanges",
      "fieldName": "delta10Min",
      "type": "doubleMeanNoNulls",
      "buckets": 18,
      "cycleSize": 6
    }
  ]
}
```
