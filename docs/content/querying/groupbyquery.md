---
layout: doc_page
---
# groupBy Queries
These types of queries take a groupBy query object and return an array of JSON objects where each object represents a
grouping asked for by the query. Note: If you only want to do straight aggregates for some time range, we highly recommend
using [TimeseriesQueries](../querying/timeseriesquery.html) instead. The performance will be substantially better. If you want to
do an ordered groupBy over a single dimension, please look at [TopN](../querying/topnquery.html) queries. The performance for that use case is also substantially better.
An example groupBy query object is shown below:

``` json
{
  "queryType": "groupBy",
  "dataSource": "sample_datasource",
  "granularity": "day",
  "dimensions": ["country", "device"],
  "limitSpec": { "type": "default", "limit": 5000, "columns": ["country", "data_transfer"] },
  "filter": {
    "type": "and",
    "fields": [
      { "type": "selector", "dimension": "carrier", "value": "AT&T" },
      { "type": "or", 
        "fields": [
          { "type": "selector", "dimension": "make", "value": "Apple" },
          { "type": "selector", "dimension": "make", "value": "Samsung" }
        ]
      }
    ]
  },
  "aggregations": [
    { "type": "longSum", "name": "total_usage", "fieldName": "user_count" },
    { "type": "doubleSum", "name": "data_transfer", "fieldName": "data_transfer" }
  ],
  "postAggregations": [
    { "type": "arithmetic",
      "name": "avg_usage",
      "fn": "/",
      "fields": [
        { "type": "fieldAccess", "fieldName": "data_transfer" },
        { "type": "fieldAccess", "fieldName": "total_usage" }
      ]
    }
  ],
  "intervals": [ "2012-01-01T00:00:00.000/2012-01-03T00:00:00.000" ],
  "having": {
  	"type": "greaterThan",
  	"aggregation": "total_usage",
  	"value": 100
  }
}
```

There are 11 main parts to a groupBy query:

|property|description|required?|
|--------|-----------|---------|
|queryType|This String should always be "groupBy"; this is the first thing Druid looks at to figure out how to interpret the query|yes|
|dataSource|A String or Object defining the data source to query, very similar to a table in a relational database. See [DataSource](../querying/datasource.html) for more information.|yes|
|dimensions|A JSON list of dimensions to do the groupBy over; or see [DimensionSpec](../querying/dimensionspecs.html) for ways to extract dimensions. |yes|
|limitSpec|See [LimitSpec](../querying/limitspec.html).|no|
|having|See [Having](../querying/having.html).|no|
|granularity|Defines the granularity of the query. See [Granularities](../querying/granularities.html)|yes|
|filter|See [Filters](../querying/filters.html)|no|
|aggregations|See [Aggregations](../querying/aggregations.html)|yes|
|postAggregations|See [Post Aggregations](../querying/post-aggregations.html)|no|
|intervals|A JSON Object representing ISO-8601 Intervals. This defines the time ranges to run the query over.|yes|
|context|An additional JSON Object which can be used to specify certain flags.|no|

To pull it all together, the above query would return *n\*m* data points, up to a maximum of 5000 points, where n is the cardinality of the `country` dimension, m is the cardinality of the `device` dimension, each day between 2012-01-01 and 2012-01-03, from the `sample_datasource` table. Each data point contains the (long) sum of `total_usage` if the value of the data point is greater than 100, the (double) sum of `data_transfer` and the (double) result of `total_usage` divided by `data_transfer` for the filter set for a particular grouping of `country` and `device`. The output looks like this:

```json
[ 
  {
    "version" : "v1",
    "timestamp" : "2012-01-01T00:00:00.000Z",
    "event" : {
      "country" : <some_dim_value_one>,
      "device" : <some_dim_value_two>,
      "total_usage" : <some_value_one>,
      "data_transfer" :<some_value_two>,
      "avg_usage" : <some_avg_usage_value>
    }
  }, 
  {
    "version" : "v1",
    "timestamp" : "2012-01-01T00:00:12.000Z",
    "event" : {
      "dim1" : <some_other_dim_value_one>,
      "dim2" : <some_other_dim_value_two>,
      "sample_name1" : <some_other_value_one>,
      "sample_name2" :<some_other_value_two>,
      "avg_usage" : <some_other_avg_usage_value>
    }
  },
...
]
```

### Behavior on multi-value dimensions

groupBy queries can group on multi-value dimensions. When grouping on a multi-value dimension, _all_ values
from matching rows will be used to generate one group per value. It's possible for a query to return more groups than
there are rows. For example, a groupBy on the dimension `tags` with filter `"t1" OR "t3"` would match only row1, and
generate a result with three groups: `t1`, `t2`, and `t3`. If you only need to include values that match
your filter, you can use a [filtered dimensionSpec](dimensionspecs.html#filtered-dimensionspecs). This can also
improve performance.

See [Multi-value dimensions](multi-value-dimensions.html) for more details.
