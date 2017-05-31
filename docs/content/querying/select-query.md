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
|granularity|Defines the granularity of the query. See [Granularities](../querying/granularities.html)|yes|
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

### Result Pagination

The PagingSpec allows the user to specify that the results of a select query should be returned as a paginated set.

The `threshold` option controls how many rows are returned in each block of paginated results.

To initiate a paginated query, the user should specify a PagingSpec with a `threshold` set and a blank `pagingIdentifiers` field, e.g.:

```json
"pagingSpec":{"pagingIdentifiers": {}, "threshold":5}
```

When the query returns, the results will contain a `pagingIndentifers` field indicating the current pagination point in the result set (an identifier and an offset).

To retrieve the next part of the result set, the user should issue the same select query, but replace the `pagingIdentifiers` field of the query with the `pagingIdentifiers` from the previous result.

When an empty result set is received, all rows have been returned.

#### fromNext Backwards Compatibility

In older versions of Druid, when using paginated select queries, it was necessary for the user to manually increment the paging offset by 1 in each `pagingIdentifiers` before submitting the next query to retrieve the next set of results. This offset increment happens automatically in the current version of Druid by default, the user does not need to modify the `pagingIdentifiers` offset to retrieve the next result set.

Setting the `fromNext` field of the PagingSpec to false instructs Druid to operate in the older mode where the user must manually increment the offset (or decrement for descending queries).

For example, suppose the user issues the following initial paginated query, with `fromNext` false:

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
   "pagingSpec":{"fromNext": "false", "pagingIdentifiers": {}, "threshold":5}
 }
```


The paginated query with `fromNext` set to false returns a result set with the following `pagingIdentifiers`:

```json
    "pagingIdentifiers" : {
      "wikipedia_2012-12-29T00:00:00.000Z_2013-01-10T08:00:00.000Z_2013-01-10T08:13:47.830Z_v9" : 4
    },
```

To retrieve the next result set, the next query must be sent with the paging offset (4) incremented by 1.

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
   "pagingSpec":{"fromNext": "false", "pagingIdentifiers": {"wikipedia_2012-12-29T00:00:00.000Z_2013-01-10T08:00:00.000Z_2013-01-10T08:13:47.830Z_v9" : 5}, "threshold":5}
 }
```

Note that specifying the `fromNext` option in the `pagingSpec` overrides the default value set by `druid.query.select.enableFromNextDefault` in the server configuration. See [Server configuration](#server-configuration) for more details.

### Server configuration

The following runtime properties apply to select queries:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.query.select.enableFromNextDefault`|If the `fromNext` property in a query's `pagingSpec` is left unspecified, the system will use the value of this property as the default value for `fromNext`. This option is true by default: the option of setting `fromNext` to false by default is intended to support backwards compatibility for deployments where some users may still expect behavior from older versions of Druid.|true|