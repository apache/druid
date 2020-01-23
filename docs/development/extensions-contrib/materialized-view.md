---
id: materialized-view
title: "Materialized View"
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


To use this Apache Druid feature, make sure to only load `materialized-view-selection` on Broker and load `materialized-view-maintenance` on Overlord. In addition, this feature currently requires a Hadoop cluster.

This feature enables Druid to greatly improve the query performance, especially when the query dataSource has a very large number of dimensions but the query only required several dimensions. This feature includes two parts. One is `materialized-view-maintenance`, and the other is `materialized-view-selection`.

## Materialized-view-maintenance
In materialized-view-maintenance, dataSources user ingested are called "base-dataSource". For each base-dataSource, we can submit `derivativeDataSource` supervisors to create and maintain other dataSources which we called  "derived-dataSource". The dimensions and metrics of derived-dataSources are the subset of base-dataSource's.
The `derivativeDataSource` supervisor is used to keep the timeline of derived-dataSource consistent with base-dataSource. Each `derivativeDataSource` supervisor  is responsible for one derived-dataSource.

A sample derivativeDataSource supervisor spec is shown below:

```json
   {
       "type": "derivativeDataSource",
       "baseDataSource": "wikiticker",
       "dimensionsSpec": {
           "dimensions": [
               "isUnpatrolled",
               "metroCode",
               "namespace",
               "page",
               "regionIsoCode",
               "regionName",
               "user"
           ]
       },
       "metricsSpec": [
           {
               "name": "count",
               "type": "count"
           },
           {
               "name": "added",
               "type": "longSum",
               "fieldName": "added"
           }
       ],
       "tuningConfig": {
           "type": "hadoop"
       }
   }
```

**Supervisor Configuration**

|Field|Description|Required|
|--------|-----------|---------|
|Type	|The supervisor type. This should always be `derivativeDataSource`.|yes|
|baseDataSource	|The name of base dataSource. This dataSource data should be already stored inside Druid, and the dataSource will be used as input data.|yes|
|dimensionsSpec	|Specifies the dimensions of the data. These dimensions must be the subset of baseDataSource's dimensions.|yes|
|metricsSpec	|A list of aggregators. These metrics must be the subset of baseDataSource's metrics. See [aggregations](../../querying/aggregations.md).|yes|
|tuningConfig	|TuningConfig must be HadoopTuningConfig. See [Hadoop tuning config](../../ingestion/hadoop.html#tuningconfig).|yes|
|dataSource	|The name of this derived dataSource. 	|no(default=baseDataSource-hashCode of supervisor)|
|hadoopDependencyCoordinates	|A JSON array of Hadoop dependency coordinates that Druid will use, this property will override the default Hadoop coordinates. Once specified, Druid will look for those Hadoop dependencies from the location specified by druid.extensions.hadoopDependenciesDir	|no|
|classpathPrefix	|Classpath that will be prepended for the Peon process.	|no|
|context	|See below.	|no|

**Context**

|Field|Description|Required|
|--------|-----------|---------|
|maxTaskCount |The max number of tasks the supervisor can submit simultaneously.	|no(default=1)|

##  Materialized-view-selection

In materialized-view-selection, we implement a new query type `view`. When we request a view query, Druid will try its best to optimize the query based on query dataSource and intervals.

A sample view query spec is shown below:

```json
   {
       "queryType": "view",
       "query": {
           "queryType": "groupBy",
           "dataSource": "wikiticker",
           "granularity": "all",
           "dimensions": [
               "user"
           ],
           "limitSpec": {
               "type": "default",
               "limit": 1,
               "columns": [
                   {
                       "dimension": "added",
                       "direction": "descending",
                       "dimensionOrder": "numeric"
                   }
               ]
           },
           "aggregations": [
               {
                   "type": "longSum",
                   "name": "added",
                   "fieldName": "added"
               }
           ],
           "intervals": [
               "2015-09-12/2015-09-13"
           ]
       }
   }
```

There are 2 parts in a view query:

|Field|Description|Required|
|--------|-----------|---------|
|queryType	|The query type. This should always be view	|yes|
|query	|The real query of this `view` query. The real query must be [groupBy](../../querying/groupbyquery.md), [topN](../../querying/topnquery.md), or [timeseries](../../querying/timeseriesquery.md) type.|yes|

**Note that Materialized View is currently designated as experimental. Please make sure the time of all processes are the same and increase monotonically. Otherwise, some unexpected errors may happen on query results.**
