---
layout: doc_page
---
Hello! This tutorial is meant to provide a more in-depth look into Druid queries. The tutorial is somewhat incomplete right now but we hope to add more content to it in the near future.

Setup
-----

Before we start digging into how to query Druid, make sure you've gone through the other tutorials and are comfortable with spinning up a local cluster and loading data into Druid.

#### Booting a Druid Cluster

Let's start up a simple Druid cluster so we can query all the things.

To start a Coordinator node:

```bash
java -Xmx256m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -classpath lib/*:config/coordinator io.druid.cli.Main server coordinator
```

To start a Historical node:

```bash
java -Xmx256m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -classpath lib/*:config/historical io.druid.cli.Main server historical
```

To start a Broker node:

```bash
java -Xmx256m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -classpath lib/*:config/broker io.druid.cli.Main server broker
```

Querying Your Data
------------------

Make sure you've completed [Loading Your Data](Loading-Your-Data-Part-1.html) so we have some data to query. Having done that, it's time to query our data! For a complete specification of queries, see [Querying](Querying.html).

#### Construct a Query
```json
{
    "queryType": "groupBy",
    "dataSource": "wikipedia",
    "granularity": "all",
    "dimensions": [],
    "aggregations": [
        {"type": "count", "name": "rows"},
        {"type": "longSum", "name": "edit_count", "fieldName": "count"},
        {"type": "doubleSum", "name": "chars_added", "fieldName": "added"}
    ],
    "intervals": ["2010-01-01T00:00/2020-01-01T00"]
}
```

#### Query That Data
Run the query against your broker:

```bash
curl -X POST "http://localhost:8080/druid/v2/?pretty" -H 'Content-type: application/json' -d @query.body
```

And get:

```json
[ {
  "version" : "v1",
  "timestamp" : "2010-01-01T00:00:00.000Z",
  "event" : {
    "chars_added" : 1545.0,
    "edit_count" : 5,
    "rows" : 5
  }
} ]
```

This result tells us that our query has 5 edits, and we have 5 rows of data as well. In those 5 edits, we have 1545 characters added.

#### What can I query for?

How are we to know what queries we can run? Although [Querying](Querying.html) is a helpful index, to get a handle on querying our data we need to look at our ingestion schema. There are a few particular fields we care about in the ingestion schema. All of these fields should in present in the real-time ingestion schema and the batch ingestion schema.

Datasource:

```json
"dataSource":"wikipedia"
```

Our dataSource tells us the name of the relation/table, or 'source of data'. What we decide to name our data source must match the data source we are going to be querying.

Granularity:

```json
"indexGranularity": "none",
```

Druid will roll up data at ingestion time unless the index/rollup granularity is specified as "none". Your query granularity cannot be lower than your index granularity.

Aggregators:

```json
"aggregators" : [{
   "type" : "count",
   "name" : "count"
  }, {
   "type" : "doubleSum",
   "name" : "added",
   "fieldName" : "added"
  }, {
   "type" : "doubleSum",
   "name" : "deleted",
   "fieldName" : "deleted"
  }, {
   "type" : "doubleSum",
   "name" : "delta",
   "fieldName" : "delta"
}]
```

The [Aggregations](Aggregations.html) specified at ingestion time correlated directly to the metrics that can be queried.

Dimensions:

```json
"dimensions" : ["page","language","user","unpatrolled","newPage","robot","anonymous","namespace","continent","country","region","city"]
```

These specify the dimensions that we can filter our data on. If we added a dimension to our groupBy query, we get:

```json
{
    "queryType": "groupBy",
    "dataSource": "wikipedia",
    "granularity": "all",
    "dimensions": ["namespace"],
    "aggregations": [
        {"type": "longSum", "name": "edit_count", "fieldName": "count"},
        {"type": "doubleSum", "name": "chars_added", "fieldName": "added"}
    ],
    "intervals": ["2010-01-01T00:00/2020-01-01T00"]
}
```

Which gets us data grouped over the namespace dimension in return!

```json
[ {
  "version" : "v1",
  "timestamp" : "2010-01-01T00:00:00.000Z",
  "event" : {
    "chars_added" : 180.0,
    "edit_count" : 2,
    "namespace" : "article"
  }
}, {
  "version" : "v1",
  "timestamp" : "2010-01-01T00:00:00.000Z",
  "event" : {
    "chars_added" : 1365.0,
    "edit_count" : 3,
    "namespace" : "wikipedia"
  }
} ]
```

Additionally,, we can also filter our query to narrow down our metric values:

```json
{
    "queryType": "groupBy",
    "dataSource": "wikipedia",
    "granularity": "all",
    "filter": { "type": "selector", "dimension": "namespace", "value": "article" },
    "aggregations": [
        {"type": "longSum", "name": "edit_count", "fieldName": "count"},
        {"type": "doubleSum", "name": "chars_added", "fieldName": "added"}
    ],
    "intervals": ["2010-01-01T00:00/2020-01-01T00"]
}
```

Which gets us metrics about only those edits where the namespace is 'article':

```json
[ {
  "version" : "v1",
  "timestamp" : "2010-01-01T00:00:00.000Z",
  "event" : {
    "chars_added" : 180.0,
    "edit_count" : 2
  }
} ]
```

Check out [Filters](Filters.html) for more information.

## Learn More ##

You can learn more about querying at [Querying](Querying.html)! If you are ready to evaluate Druid more in depth, check out [Booting a production cluster](Booting-a-production-cluster.html)!
