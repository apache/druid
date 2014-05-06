---
layout: doc_page
---
# groupBy Queries
These types of queries take a groupBy query object and return an array of JSON objects where each object represents a grouping asked for by the query. Note: If you only want to do straight aggregates for some time range, we highly recommend using [TimeseriesQueries](TimeseriesQuery.html) instead. The performance will be substantially better.
An example groupBy query object is shown below:

``` json
{
  "queryType": "groupBy",
  "dataSource": "sample_datasource",
  "granularity": "day",
  "dimensions": ["dim1", "dim2"],
  "limitSpec": { "type": "default", "limit": 5000, "columns": ["dim1", "metric1"] },
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
  "intervals": [ "2012-01-01T00:00:00.000/2012-01-03T00:00:00.000" ],
  "having": { "type": "greaterThan", "aggregation": "sample_name1", "value": 0 }
}
```

There are 9 main parts to a groupBy query:

|property|description|required?|
|--------|-----------|---------|
|queryType|This String should always be "groupBy"; this is the first thing Druid looks at to figure out how to interpret the query|yes|
|dataSource|A String defining the data source to query, very similar to a table in a relational database, or a [DataSource](DataSource.html) structure.|yes|
|dimensions|A JSON list of dimensions to do the groupBy over|yes|
|orderBy|See [OrderBy](OrderBy.html).|no|
|having|See [Having](Having.html).|no|
|granularity|Defines the granularity of the query. See [Granularities](Granularities.html)|yes|
|filter|See [Filters](Filters.html)|no|
|aggregations|See [Aggregations](Aggregations.html)|yes|
|postAggregations|See [Post Aggregations](Post-Aggregations.html)|no|
|intervals|A JSON Object representing ISO-8601 Intervals. This defines the time ranges to run the query over.|yes|
|context|An additional JSON Object which can be used to specify certain flags.|no|

To pull it all together, the above query would return *n\*m* data points, up to a maximum of 5000 points, where n is the cardinality of the "dim1" dimension, m is the cardinality of the "dim2" dimension, each day between 2012-01-01 and 2012-01-03, from the "sample_datasource" table. Each data point contains the (long) sum of sample_fieldName1 if the value of the data point is greater than 0, the (double) sum of sample_fieldName2 and the (double) the result of sample_fieldName1 divided by sample_fieldName2 for the filter set for a particular grouping of "dim1" and "dim2". The output looks like this:

```json
[ 
  {
    "version" : "v1",
    "timestamp" : "2012-01-01T00:00:00.000Z",
    "event" : {
      "dim1" : <some_dim_value_one>,
      "dim2" : <some_dim_value_two>,
      "sample_name1" : <some_sample_name_value_one>,
      "sample_name2" :<some_sample_name_value_two>,
      "sample_divide" : <some_sample_divide_value>
    }
  }, 
  {
    "version" : "v1",
    "timestamp" : "2012-01-01T00:00:00.000Z",
    "event" : {
      "dim1" : <some_other_dim_value_one>,
      "dim2" : <some_other_dim_value_two>,
      "sample_name1" : <some_other_sample_name_value_one>,
      "sample_name2" :<some_other_sample_name_value_two>,
      "sample_divide" : <some_other_sample_divide_value>
    }
  },
...
]
```