---
layout: doc_page
---
# Select Queries
Select queries return raw Druid rows and support pagination.

```json
 {
   "queryType": "select",
   "dataSource": "wikipedia",
   "descending": "false",
   "dimensions":[],
   "metrics":[],
   "granularity": "all",
   "intervals": [
     "2013-01-01/2013-01-02"
   ],
   "pagingSpec":{"pagingIdentifiers": {}, "threshold":5}
 }
```

There are several main parts to a select query:

|property|description|required?|
|--------|-----------|---------|
|queryType|This String should always be "select"; this is the first thing Druid looks at to figure out how to interpret the query|yes|
|dataSource|A String or Object defining the data source to query, very similar to a table in a relational database. See [DataSource](../querying/datasource.html) for more information.|yes|
|intervals|A JSON Object representing ISO-8601 Intervals. This defines the time ranges to run the query over.|yes|
|descending|Whether to make descending ordered result. Default is `false`(ascending). When this is `true`, page identifier and offsets will be negative value.|no|
|filter|See [Filters](../querying/filters.html)|no|
|dimensions|A JSON list of dimensions to select; or see [DimensionSpec](../querying/dimensionspecs.html) for ways to extract dimensions. If left empty, all dimensions are returned.|no|
|metrics|A String array of metrics to select. If left empty, all metrics are returned.|no|
|pagingSpec|A JSON object indicating offsets into different scanned segments. Query results will return a `pagingIdentifiers` value that can be reused in the next query for pagination.|yes|
|context|An additional JSON Object which can be used to specify certain flags.|no|

The format of the result is:

```json
 [{
  "timestamp" : "2013-01-01T00:00:00.000Z",
  "result" : {
    "pagingIdentifiers" : {
      "wikipedia_2012-12-29T00:00:00.000Z_2013-01-10T08:00:00.000Z_2013-01-10T08:13:47.830Z_v9" : 4
    },
    "events" : [ {
      "segmentId" : "wikipedia_editstream_2012-12-29T00:00:00.000Z_2013-01-10T08:00:00.000Z_2013-01-10T08:13:47.830Z_v9",
      "offset" : 0,
      "event" : {
        "timestamp" : "2013-01-01T00:00:00.000Z",
        "robot" : "1",
        "namespace" : "article",
        "anonymous" : "0",
        "unpatrolled" : "0",
        "page" : "11._korpus_(NOVJ)",
        "language" : "sl",
        "newpage" : "0",
        "user" : "EmausBot",
        "count" : 1.0,
        "added" : 39.0,
        "delta" : 39.0,
        "variation" : 39.0,
        "deleted" : 0.0
      }
    }, {
      "segmentId" : "wikipedia_2012-12-29T00:00:00.000Z_2013-01-10T08:00:00.000Z_2013-01-10T08:13:47.830Z_v9",
      "offset" : 1,
      "event" : {
        "timestamp" : "2013-01-01T00:00:00.000Z",
        "robot" : "0",
        "namespace" : "article",
        "anonymous" : "0",
        "unpatrolled" : "0",
        "page" : "112_U.S._580",
        "language" : "en",
        "newpage" : "1",
        "user" : "MZMcBride",
        "count" : 1.0,
        "added" : 70.0,
        "delta" : 70.0,
        "variation" : 70.0,
        "deleted" : 0.0
      }
    }, {
      "segmentId" : "wikipedia_2012-12-29T00:00:00.000Z_2013-01-10T08:00:00.000Z_2013-01-10T08:13:47.830Z_v9",
      "offset" : 2,
      "event" : {
        "timestamp" : "2013-01-01T00:00:00.000Z",
        "robot" : "0",
        "namespace" : "article",
        "anonymous" : "0",
        "unpatrolled" : "0",
        "page" : "113_U.S._243",
        "language" : "en",
        "newpage" : "1",
        "user" : "MZMcBride",
        "count" : 1.0,
        "added" : 77.0,
        "delta" : 77.0,
        "variation" : 77.0,
        "deleted" : 0.0
      }
    }, {
      "segmentId" : "wikipedia_2012-12-29T00:00:00.000Z_2013-01-10T08:00:00.000Z_2013-01-10T08:13:47.830Z_v9",
      "offset" : 3,
      "event" : {
        "timestamp" : "2013-01-01T00:00:00.000Z",
        "robot" : "0",
        "namespace" : "article",
        "anonymous" : "0",
        "unpatrolled" : "0",
        "page" : "113_U.S._73",
        "language" : "en",
        "newpage" : "1",
        "user" : "MZMcBride",
        "count" : 1.0,
        "added" : 70.0,
        "delta" : 70.0,
        "variation" : 70.0,
        "deleted" : 0.0
      }
    }, {
      "segmentId" : "wikipedia_2012-12-29T00:00:00.000Z_2013-01-10T08:00:00.000Z_2013-01-10T08:13:47.830Z_v9",
      "offset" : 4,
      "event" : {
        "timestamp" : "2013-01-01T00:00:00.000Z",
        "robot" : "0",
        "namespace" : "article",
        "anonymous" : "0",
        "unpatrolled" : "0",
        "page" : "113_U.S._756",
        "language" : "en",
        "newpage" : "1",
        "user" : "MZMcBride",
        "count" : 1.0,
        "added" : 68.0,
        "delta" : 68.0,
        "variation" : 68.0,
        "deleted" : 0.0
      }
    } ]
  }
} ]
```

The `threshold` determines how many hits are returned, with each hit indexed by an offset. When `descending` is true, the offset will be negative value.

The results above include:

```json 
    "pagingIdentifiers" : {
      "wikipedia_2012-12-29T00:00:00.000Z_2013-01-10T08:00:00.000Z_2013-01-10T08:13:47.830Z_v9" : 4
    },
```

This can be used with the next query's pagingSpec:

```json
 {
   "queryType": "select",
   "dataSource": "wikipedia",
   "dimensions":[],
   "metrics":[],
   "granularity": "all",
   "intervals": [
     "2013-01-01/2013-01-02"
   ],
   "pagingSpec":{"pagingIdentifiers": {"wikipedia_2012-12-29T00:00:00.000Z_2013-01-10T08:00:00.000Z_2013-01-10T08:13:47.830Z_v9" : 5}, "threshold":5}
      
 }

Note that in the second query, an offset is specified and that it is 1 greater than the largest offset found in the initial results. To return the next "page", this offset must be incremented by 1 (should be decremented by 1 for descending query), with each new query. When an empty results set is received, the very last page has been returned.
