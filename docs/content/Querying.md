---
layout: doc_page
---

Querying
========

Queries are made using an HTTP REST style request to queryable nodes ([Broker](Broker.html),
[Historical](Historical.html), or [Realtime](Realtime.html)). The
query is expressed in JSON and each of these node types expose the same
REST query interface. For normal Druid operations, queries should be issued to the broker nodes.

Although Druid's native query language is JSON over HTTP, many members of the community have contributed different [client libraries](./Libraries.html) in other languages to query Druid.

Available Queries
-----------------

Druid has numerous query types for various use cases. Queries are composed of various JSON properties and Druid has different types of queries for different use cases. The documentation for the various query types describe all the JSON properties that can be set.

### Aggregation Queries

* [Timeseries](./TimeseriesQuery.html)
* [TopN](./TopNQuery.html)
* [GroupBy](./GroupByQuery.html)

### Metadata Queries

* [Time Boundary](./TimeBoundaryQuery.html)
* [Segment Metadata](./SegmentMetadataQuery.html)
* [Datasource Metadata](./DatasourceMetadataQuery.html)

### Search Queries

* [Search](./SearchQuery.html)

Which Query Should I Use?
-------------------------

Where possible, we recommend using [Timeseries]() and [TopN]() queries instead of [GroupBy](). GroupBy is the most flexible Druid query, but also has the poorest performance.
 Timeseries are significantly faster than groupBy queries for aggregations that don't require grouping over dimensions. For grouping and sorting over a single dimension,
 topN queries are much more optimized than groupBys.

Query Cancellation
------------------

Queries can be cancelled explicitly using their unique identifier.  If the
query identifier is set at the time of query, or is otherwise known, the following
endpoint can be used on the broker or router to cancel the query.

```sh
DELETE /druid/v2/{queryId}
```

For example, if the query ID is `abc123`, the query can be cancelled as follows:

```sh
curl -X DELETE "http://host:port/druid/v2/abc123"
```
