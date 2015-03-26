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
        { "type": "fieldAccess", "name": "sample_name1", "fieldName": "sample_fieldName1" },
        { "type": "fieldAccess", "name": "sample_name2", "fieldName": "sample_fieldName2" }
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
|dataSource|A String or Object defining the data source to query, very similar to a table in a relational database. See [DataSource](DataSource.html) for more information.|yes|
|intervals|A JSON Object representing ISO-8601 Intervals. This defines the time ranges to run the query over.|yes|
|granularity|Defines the granularity to bucket query results. See [Granularities](Granularities.html)|yes|
|filter|See [Filters](Filters.html)|no|
|aggregations|See [Aggregations](Aggregations.html)|yes|
|postAggregations|See [Post Aggregations](Post-aggregations.html)|no|
|context|See [Context](Context.html)|no|

To pull it all together, the above query would return 2 data points, one for each day between 2012-01-01 and 2012-01-03, from the "sample\_datasource" table. Each data point would be the (long) sum of sample\_fieldName1, the (double) sum of sample\_fieldName2 and the (double) the result of sample\_fieldName1 divided by sample\_fieldName2 for the filter set. The output looks like this:

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
