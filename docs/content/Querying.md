---
layout: doc_page
---
Querying
========

Queries are made using an HTTP REST style request to a [Broker](Broker.html), [Historical](Historical.html), or [Realtime](Realtime.html) node. The query is expressed in JSON and each of these node types expose the same REST query interface.

We start by describing an example query with additional comments that mention possible variations. Query operators are also summarized in a table below.

Example Query "rand"
--------------------

Here is the query in the examples/rand subproject (file is query.body), followed by a commented version of the same.

```javascript
{
  "queryType": "groupBy",
  "dataSource": "randSeq",
  "granularity": "all",
  "dimensions": [],
  "aggregations": [
    { "type": "count", "name": "rows" },
    { "type": "doubleSum", "fieldName": "events", "name": "e" },
    { "type": "doubleSum", "fieldName": "outColumn", "name": "randomNumberSum" }
  ],
  "postAggregations": [{
     "type": "arithmetic",
     "name": "avg_random",
     "fn": "/",
     "fields": [
       { "type": "fieldAccess", "fieldName": "randomNumberSum" },
       { "type": "fieldAccess", "fieldName": "rows" }
     ]
  }],
  "intervals": ["2012-10-01T00:00/2020-01-01T00"]
}
```

This query could be submitted via curl like so (assuming the query object is in a file "query.json").

```
curl -X POST "http://host:port/druid/v2/?pretty" -H 'content-type: application/json' -d @query.json
```

The "pretty" query parameter gets the results formatted a bit nicer.

Details of Example Query "rand"
-------------------------------

The queryType JSON field identifies which kind of query operator is to be used, in this case it is groupBy, the most frequently used kind (which corresponds to an internal implementation class GroupByQuery registered as "groupBy"), and it has a set of required fields that are also part of this query. The queryType can also be "search" or "timeBoundary" which have similar or different required fields summarized below:

```javascript
{
  "queryType": "groupBy",
```

The dataSource JSON field shown next identifies where to apply the query. In this case, randSeq corresponds to the examples/rand/rand_realtime.spec file schema:

```javascript
  "dataSource": "randSeq",
```

The granularity JSON field specifies the bucket size for values. It could be a built-in time interval like "second", "minute", "fifteen_minute", "thirty_minute", "hour" or "day". It can also be an expression like `{"type": "period", "period":"PT6m"}` meaning "6 minute buckets". See [Granularities](Granularities.html) for more information on the different options for this field. In this example, it is set to the special value "all" which means [bucket all data points together into the same time bucket]()

```javascript
  "granularity": "all",
```

The dimensions JSON field value is an array of zero or more fields as defined in the dataSource spec file or defined in the input records and carried forward. These are used to constrain the grouping. If empty, then one value per time granularity bucket is requested in the groupBy:

```javascript
  "dimensions": [],
```

A groupBy also requires the JSON field "aggregations" (See [Aggregations](Aggregations.html)), which are applied to the column specified by fieldName and the output of the aggregation will be named according to the value in the "name" field:

```javascript
  "aggregations": [
    { "type": "count", "name": "rows" },
    { "type": "doubleSum", "fieldName": "events", "name": "e" },
    { "type": "doubleSum", "fieldName": "outColumn", "name": "randomNumberSum" }
  ],
```

You can also specify postAggregations, which are applied after data has been aggregated for the current granularity and dimensions bucket. See [Post Aggregations](Post Aggregations.html) for a detailed description. In the rand example, an arithmetic type operation (division, as specified by "fn") is performed with the result "name" of "avg_random". The "fields" field specifies the inputs from the aggregation stage to this expression. Note that identifiers corresponding to "name" JSON field inside the type "fieldAccess" are required but not used outside this expression, so they are prefixed with "dummy" for clarity:

```javascript
  "postAggregations": [{
     "type": "arithmetic",
     "name": "avg_random",
     "fn": "/",
     "fields": [
       { "type": "fieldAccess", "fieldName": "randomNumberSum" },
       { "type": "fieldAccess", "fieldName": "rows" }
     ]
  }],
```
The time range(s) of the query; data outside the specified intervals will not be used; this example specifies from October 1, 2012 until January 1, 2020:

```javascript
  "intervals": ["2012-10-01T00:00/2020-01-01T00"]
}
```

Query Operators
---------------

The following table summarizes query properties.

|query types|property|description|required?|
|-----------|--------|-----------|---------|
|timeseries, groupBy, search, timeBoundary|dataSource|query is applied to this data source|yes|
|timeseries, groupBy, search|intervals|range of time series to include in query|yes|
|timeseries, groupBy, search, timeBoundary|context|This is a key-value map that can allow the query to alter some of the behavior of a query. It is primarily used for debugging, for example if you include `"bySegment":true` in the map, you will get results associated with the data segment they came from.|no|
|timeseries, groupBy, search|filter|Specifies the filter (the "WHERE" clause in SQL) for the query. See [Filters](Filters.html)|no|
|timeseries, groupBy, search|granularity|the timestamp granularity to bucket results into (i.e. "hour"). See [Granularities](Granularities.html) for more information.|no|
|groupBy|dimensions|constrains the groupings; if empty, then one value per time granularity bucket|yes|
|timeseries, groupBy|aggregations|aggregations that combine values in a bucket. See [Aggregations](Aggregations.html).|yes|
|timeseries, groupBy|postAggregations|aggregations of aggregations. See [Post Aggregations](Post Aggregations.html).|yes|
|search|limit|maximum number of results (default is 1000), a system-level maximum can also be set via `com.metamx.query.search.maxSearchLimit`|no|
|search|searchDimensions|Dimensions to apply the search query to. If not specified, it will search through all dimensions.|no|
|search|query|The query portion of the search query. This is essentially a predicate that specifies if something matches.|yes|

Additional Information about Query Types
----------------------------------------

[TimeseriesQuery](TimeseriesQuery.html)
