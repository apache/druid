---
layout: doc_page
---

# Materialized View

To use this feature, make sure to only load materialized-view-selection on broker and load materialized-view-maintenance on overlord. In addtion, this feature currently requires a hadoop cluster.

This feature enables Druid to greatly improve the query performance, especially when the query dataSource has a very large number of dimensions but the query only required several dimensions. This feature includes two parts. One is `materialized-view-maintenance`, and the other is `materialized-view-selection`.

## Materialized-view-maintenance
In materialized-view-maintenance, dataSouces user ingested are called "base-dataSource". For each base-dataSource, we can submit `derivativeDataSource` supervisors to create and maintain other dataSources which we called  "derived-dataSource". The deminsions and metrics of derived-dataSources are the subset of base-dataSource's.
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
|Type	|The supervisor type. This should always be derivativeDataSource	|yes|
|baseDataSource	|The name of base dataSource. This dataSource data should be already stored inside Druid, and the dataSource will be used as input data. See [dataSource inputSpec](http://druid.io/docs/latest/ingestion/update-existing-data.html#datasource "dataSource inputSpec"). 	|yes|
|dimensionsSpec	|Specifies the dimensions of the data. These dimensions must be the subset of baseDataSource's dimensions.	|yes|
|metricsSpec	|A list of aggregators. These metrics must be the subset of baseDataSource's metrics. See [aggregations](http://druid.io/docs/latest/querying/aggregations.html "aggregations") 	|yes|
|tuningConfig	|TuningConfig must be HadoopTuningConfig. See [hadoop tuning config]( http://druid.io/docs/latest/ingestion/batch-ingestion.html#tuningconfig "hadoop tuning config")	|yes|
|dataSource	|The name of this derived dataSource. 	|no(default=baseDataSource-hashCode of supervisor)|
|hadoopDependencyCoordinates	|A JSON array of Hadoop dependency coordinates that Druid will use, this property will override the default Hadoop coordinates. Once specified, Druid will look for those Hadoop dependencies from the location specified by druid.extensions.hadoopDependenciesDir	|no|
|classpathPrefix	|Classpath that will be pre-appended for the peon process.	|no|
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
|query	|The real query of this `view` query. The real query must be [groupby](http://druid.io/docs/latest/querying/groupbyquery.html "groupby")/[topn](http://druid.io/docs/latest/querying/topnquery.html "topn")/[timeseries](http://druid.io/docs/latest/querying/timeseriesquery.html "timeseries") type.	|yes|

**Note that Materialized View is currently designated as experimental. Please make sure the time of all nodes are the same and increase monotonically. Otherwise, some unexpected errors may happen on query results.**