---
id: querying
title: "Native queries"
sidebar_label: "Making native queries"
---

<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->


> Apache Druid (incubating) supports two query languages: [Druid SQL](sql.md) and native queries. Druid SQL
> queries are planned into native queries. This document describes the native query language.

Native queries in Druid are JSON objects and are typically issued to the Broker or Router processes. Queries can be
posted like this:

```bash
curl -X POST '<queryable_host>:<port>/druid/v2/?pretty' -H 'Content-Type:application/json' -H 'Accept:application/json' -d @<query_json_file>
```

Druid's native query language is JSON over HTTP, although many members of the community have contributed different
[client libraries](/libraries.html) in other languages to query Druid.

The Content-Type/Accept Headers can also take 'application/x-jackson-smile'.

```bash
curl -X POST '<queryable_host>:<port>/druid/v2/?pretty' -H 'Content-Type:application/json' -H 'Accept:application/x-jackson-smile' -d @<query_json_file>
```

Note: If Accept header is not provided, it defaults to value of 'Content-Type' header.

Druid's native query is relatively low level, mapping closely to how computations are performed internally. Druid queries
are designed to be lightweight and complete very quickly. This means that for more complex analysis, or to build
more complex visualizations, multiple Druid queries may be required.

Even though queries are typically made to Brokers or Routers, they can also be accepted by
[Historical](../design/historical.md) processes and by [Peons (task JVMs)](../design/peons.md)) that are running
stream ingestion tasks. This may be valuable if you want to query results for specific segments that are served by
specific processes.

## Available queries

Druid has numerous query types for various use cases. Queries are composed of various JSON properties and Druid has different types of queries for different use cases. The documentation for the various query types describe all the JSON properties that can be set.

### Aggregation queries

* [Timeseries](../querying/timeseriesquery.md)
* [TopN](../querying/topnquery.md)
* [GroupBy](../querying/groupbyquery.md)

### Metadata queries

* [TimeBoundary](../querying/timeboundaryquery.md)
* [SegmentMetadata](../querying/segmentmetadataquery.md)
* [DatasourceMetadata](../querying/datasourcemetadataquery.md)

### Search queries

* [Search](../querying/searchquery.md)

## Which query should I use?

Where possible, we recommend using [Timeseries]() and [TopN]() queries instead of [GroupBy](). GroupBy is the most flexible Druid query, but also has the poorest performance.
 Timeseries are significantly faster than groupBy queries for aggregations that don't require grouping over dimensions. For grouping and sorting over a single dimension,
 topN queries are much more optimized than groupBys.

## Query cancellation

Queries can be cancelled explicitly using their unique identifier.  If the
query identifier is set at the time of query, or is otherwise known, the following
endpoint can be used on the Broker or Router to cancel the query.

```sh
DELETE /druid/v2/{queryId}
```

For example, if the query ID is `abc123`, the query can be cancelled as follows:

```sh
curl -X DELETE "http://host:port/druid/v2/abc123"
```

## Query errors

If a query fails, you will get an HTTP 500 response containing a JSON object with the following structure:

```json
{
  "error" : "Query timeout",
  "errorMessage" : "Timeout waiting for task.",
  "errorClass" : "java.util.concurrent.TimeoutException",
  "host" : "druid1.example.com:8083"
}
```

The fields in the response are:

|field|description|
|-----|-----------|
|error|A well-defined error code (see below).|
|errorMessage|A free-form message with more information about the error. May be null.|
|errorClass|The class of the exception that caused this error. May be null.|
|host|The host on which this error occurred. May be null.|

Possible codes for the *error* field include:

|code|description|
|----|-----------|
|`Query timeout`|The query timed out.|
|`Query interrupted`|The query was interrupted, possibly due to JVM shutdown.|
|`Query cancelled`|The query was cancelled through the query cancellation API.|
|`Resource limit exceeded`|The query exceeded a configured resource limit (e.g. groupBy maxResults).|
|`Unauthorized request.`|The query was denied due to security policy. Either the user was not recognized, or the user was recognized but does not have access to the requested resource.|
|`Unsupported operation`|The query attempted to perform an unsupported operation. This may occur when using undocumented features or when using an incompletely implemented extension.|
|`Unknown exception`|Some other exception occurred. Check errorMessage and errorClass for details, although keep in mind that the contents of those fields are free-form and may change from release to release.|
