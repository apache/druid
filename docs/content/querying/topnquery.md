---
layout: doc_page
---
TopN queries
==================

TopN queries return a sorted set of results for the values in a given dimension according to some criteria. Conceptually, they can be thought of as an approximate [GroupByQuery](../querying/groupbyquery.html) over a single dimension with an [Ordering](../querying/limitspec.html) spec. TopNs are much faster and resource efficient than GroupBys for this use case. These types of queries take a topN query object and return an array of JSON objects where each object represents a value asked for by the topN query.

TopNs are approximate in that each node will rank their top K results and only return those top K results to the broker. K, by default in Druid, is `max(1000, threshold)`. In practice, this means that if you ask for the top 1000 items ordered, the correctness of the first ~900 items will be 100%, and the ordering of the results after that is not guaranteed. TopNs can be made more accurate by increasing the threshold.

A topN query object looks like:

```json
{
  "queryType": "topN",
  "dataSource": "sample_data",
  "dimension": "sample_dim",
  "threshold": 5,
  "metric": "count",
  "granularity": "all",
  "filter": {
    "type": "and",
    "fields": [
      {
        "type": "selector",
        "dimension": "dim1",
        "value": "some_value"
      },
      {
        "type": "selector",
        "dimension": "dim2",
        "value": "some_other_val"
      }
    ]
  },
  "aggregations": [
    {
      "type": "longSum",
      "name": "count",
      "fieldName": "count"
    },
    {
      "type": "doubleSum",
      "name": "some_metric",
      "fieldName": "some_metric"
    }
  ],
  "postAggregations": [
    {
      "type": "arithmetic",
      "name": "average",
      "fn": "/",
      "fields": [
        {
          "type": "fieldAccess",
          "name": "some_metric",
          "fieldName": "some_metric"
        },
        {
          "type": "fieldAccess",
          "name": "count",
          "fieldName": "count"
        }
      ]
    }
  ],
  "intervals": [
    "2013-08-31T00:00:00.000/2013-09-03T00:00:00.000"
  ]
}
```

There are 11 parts to a topN query.

|property|description|required?|
|--------|-----------|---------|
|queryType|This String should always be "topN"; this is the first thing Druid looks at to figure out how to interpret the query|yes|
|dataSource|A String or Object defining the data source to query, very similar to a table in a relational database. See [DataSource](../querying/datasource.html) for more information.|yes|
|intervals|A JSON Object representing ISO-8601 Intervals. This defines the time ranges to run the query over.|yes|
|granularity|Defines the granularity to bucket query results. See [Granularities](../querying/granularities.html)|yes|
|filter|See [Filters](../querying/filters.html)|no|
|aggregations|See [Aggregations](../querying/aggregations.html)|no|
|postAggregations|See [Post Aggregations](../querying/post-aggregations.html)|no|
|dimension|A String or JSON object defining the dimension that you want the top taken for. For more info, see [DimensionSpecs](../querying/dimensionspecs.html)|yes|
|threshold|An integer defining the N in the topN (i.e. how many results you want in the top list)|yes|
|metric|A String or JSON object specifying the metric to sort by for the top list. For more info, see [TopNMetricSpec](../querying/topnmetricspec.html).|yes|
|context|See [Context](../querying/query-context.html)|no|

Please note the context JSON object is also available for topN queries and should be used with the same caution as the timeseries case.
The format of the results would look like so:

```json
[
  {
    "timestamp": "2013-08-31T00:00:00.000Z",
    "result": [
      {
        "dim1": "dim1_val",
        "count": 111,
        "some_metrics": 10669,
        "average": 96.11711711711712
      },
      {
        "dim1": "another_dim1_val",
        "count": 88,
        "some_metrics": 28344,
        "average": 322.09090909090907
      },
      {
        "dim1": "dim1_val3",
        "count": 70,
        "some_metrics": 871,
        "average": 12.442857142857143
      },
      {
        "dim1": "dim1_val4",
        "count": 62,
        "some_metrics": 815,
        "average": 13.14516129032258
      },
      {
        "dim1": "dim1_val5",
        "count": 60,
        "some_metrics": 2787,
        "average": 46.45
      }
    ]
  }
]
```

### Behavior on multi-value dimensions

topN queries can group on multi-value dimensions. When grouping on a multi-value dimension, _all_ values
from matching rows will be used to generate one group per value. It's possible for a query to return more groups than
there are rows. For example, a topN on the dimension `tags` with filter `"t1" AND "t3"` would match only row1, and
generate a result with three groups: `t1`, `t2`, and `t3`. If you only need to include values that match
your filter, you can use a [filtered dimensionSpec](dimensionspecs.html#filtered-dimensionspecs). This can also
improve performance.

See [Multi-value dimensions](multi-value-dimensions.html) for more details.

### Aliasing

The current TopN algorithm is an approximate algorithm. The top 1000 local results from each segment are returned for merging to determine the global topN. As such, the topN algorithm is approximate in both rank and results. Approximate results *ONLY APPLY WHEN THERE ARE MORE THAN 1000 DIM VALUES*. A topN over a dimension with fewer than 1000 unique dimension values can be considered accurate in rank and accurate in aggregates.

The threshold can be modified from it's default 1000 via the server parameter `druid.query.topN.minTopNThreshold` which need to restart servers to take effect or set `minTopNThreshold` in query context which take effect per query. 

If you are wanting the top 100 of a high cardinality, uniformly distributed dimension ordered by some low-cardinality, uniformly distributed dimension, you are potentially going to get aggregates back that are missing data.

To put it another way, the best use cases for topN are when you can have confidence that the overall results are uniformly in the top. For example, if a particular site ID is in the top 10 for some metric for every hour of every day, then it will probably be accurate in the topN over multiple days. But if a site barely in the top 1000 for any given hour, but over the whole query granularity is in the top 500 (example: a site which gets highly uniform traffic co-mingling in the dataset with sites with highly periodic data), then a top500 query may not have that particular site a the exact rank, and may not be accurate for that particular site's aggregates.

Before continuing in this section, please consider if you really need exact results. Getting exact results is a very resource intensive process. For the vast majority of "useful" data results, an approximate topN algorithm supplies plenty of accuracy.

Users wishing to get an *exact rank and exact aggregates* topN over a dimension with greater than 1000 unique values should issue a groupBy query and sort the results themselves. This is very computationally expensive for high-cardinality dimensions.

Users who can tolerate *approximate rank* topN over a dimension with greater than 1000 unique values, but require *exact aggregates* can issue two queries. One to get the approximate topN dimension values, and another topN with dimension selection filters which only use the topN results of the first.

#### Example First query:

```json
{
    "aggregations": [
             {
                 "fieldName": "L_QUANTITY_longSum",
                 "name": "L_QUANTITY_",
                 "type": "longSum"
             }
    ],
    "dataSource": "tpch_year",
    "dimension":"l_orderkey",
    "granularity": "all",
    "intervals": [
        "1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z"
    ],
    "metric": "L_QUANTITY_",
    "queryType": "topN",
    "threshold": 2
}
```

#### Example second query:

```json
{
    "aggregations": [
             {
                 "fieldName": "L_TAX_doubleSum",
                 "name": "L_TAX_",
                 "type": "doubleSum"
             },
             {
                 "fieldName": "L_DISCOUNT_doubleSum",
                 "name": "L_DISCOUNT_",
                 "type": "doubleSum"
             },
             {
                 "fieldName": "L_EXTENDEDPRICE_doubleSum",
                 "name": "L_EXTENDEDPRICE_",
                 "type": "doubleSum"
             },
             {
                 "fieldName": "L_QUANTITY_longSum",
                 "name": "L_QUANTITY_",
                 "type": "longSum"
             },
             {
                 "name": "count",
                 "type": "count"
             }
    ],
    "dataSource": "tpch_year",
    "dimension":"l_orderkey",
    "filter": {
        "fields": [
            {
                "dimension": "l_orderkey",
                "type": "selector",
                "value": "103136"
            },
            {
                "dimension": "l_orderkey",
                "type": "selector",
                "value": "1648672"
            }
        ],
        "type": "or"
    },
    "granularity": "all",
    "intervals": [
        "1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z"
    ],
    "metric": "L_QUANTITY_",
    "queryType": "topN",
    "threshold": 2
}
```
