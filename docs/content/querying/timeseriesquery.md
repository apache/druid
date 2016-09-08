---
layout: doc_page
---
Timeseries queries
==================

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
|queryType|This String should always be "timeseries"; this is the first thing Druid looks at to figure out how to interpret the query|yes|
|dataSource|A String or Object defining the data source to query, very similar to a table in a relational database. See [DataSource](../querying/datasource.html) for more information.|yes|
|descending|Whether to make descending ordered result. Default is `false`(ascending).|no|
|intervals|A JSON Object representing ISO-8601 Intervals. This defines the time ranges to run the query over.|yes|
|granularity|Defines the granularity to bucket query results. See [Granularities](../querying/granularities.html)|yes|
|filter|See [Filters](../querying/filters.html)|no|
|aggregations|See [Aggregations](../querying/aggregations.html)|no|
|postAggregations|See [Post Aggregations](../querying/post-aggregations.html)|no|
|context|See [Context](../querying/query-context.html)|no|

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

#### Zero-filling

Timeseries queries normally fill empty interior time buckets with zeroes. For example, if you issue a "day" granularity
timeseries query for the interval 2012-01-01/2012-01-04, and no data exists for 2012-01-02, you will receive:

```json
[
  {
    "timestamp": "2012-01-01T00:00:00.000Z",
    "result": { "sample_name1": <some_value> }
  },
  {
   "timestamp": "2012-01-02T00:00:00.000Z",
   "result": { "sample_name1": 0 }
  },
  {
    "timestamp": "2012-01-03T00:00:00.000Z",
    "result": { "sample_name1": <some_value> }
  }
]
```

Time buckets that lie completely outside the data interval are not zero-filled.

You can disable all zero-filling with the context flag "skipEmptyBuckets". In this mode, the data point for 2012-01-02
would be omitted from the results.

A query with this context flag set would look like:

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
