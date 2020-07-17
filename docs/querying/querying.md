---
id: querying
title: "Native queries"
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


> Apache Druid supports two query languages: [Druid SQL](sql.md) and [native queries](querying.md).
> This document describes the
> native query language. For information about how Druid SQL chooses which native query types to use when
> it runs a SQL query, refer to the [SQL documentation](sql.md#query-types).

Native queries in Druid are JSON objects and are typically issued to the Broker or Router processes. Queries can be
posted like this:

```bash
curl -X POST '<queryable_host>:<port>/druid/v2/?pretty' -H 'Content-Type:application/json' -H 'Accept:application/json' -d @<query_json_file>
```

> Replace `<queryable_host>:<port>` with the appropriate address and port for your system. For example, if running the quickstart configuration, replace `<queryable_host>:<port>` with localhost:8888. 

You can also enter them directly in the Druid console's Query view. Simply pasting a native query into the console switches the editor into JSON mode.

![Native query](../assets/native-queries-01.png "Native query")


Druid's native query language is JSON over HTTP, although many members of the community have contributed different
[client libraries](/libraries.html) in other languages to query Druid.

The Content-Type/Accept Headers can also take 'application/x-jackson-smile'.

```bash
curl -X POST '<queryable_host>:<port>/druid/v2/?pretty' -H 'Content-Type:application/json' -H 'Accept:application/x-jackson-smile' -d @<query_json_file>
```

> If the Accept header is not provided, it defaults to the value of 'Content-Type' header.

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

### Other queries

* [Scan](../querying/scan-query.md)
* [Search](../querying/searchquery.md)

## Which query type should I use?

For aggregation queries, if more than one would satisfy your needs, we generally recommend using Timeseries or TopN
whenever possible, as they are specifically optimized for their use cases. If neither is a good fit, you should use
the GroupBy query, which is the most flexible.

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

If a query request fails due to being limited by the [query scheduler laning configuration](../configuration/index.md#broker), an HTTP 429 response with the same JSON object schema as an error response, but with `errorMessage` of the form: "Total query capacity exceeded" or "Query capacity exceeded for lane 'low'".

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
|`Truncated response context`|An intermediate response context for the query exceeded the built-in limit of 7KB.<br/><br/>The response context is an internal data structure that Druid servers use to share out-of-band information when sending query results to each other. It is serialized in an HTTP header with a maximum length of 7KB. This error occurs when an intermediate response context sent from a data server (like a Historical) to the Broker exceeds this limit.<br/><br/>The response context is used for a variety of purposes, but the one most likely to generate a large context is sharing details about segments that move during a query. That means this error can potentially indicate that a very large number of segments moved in between the time a Broker issued a query and the time it was processed on Historicals. This should rarely, if ever, occur during normal operation.|
|`Unknown exception`|Some other exception occurred. Check errorMessage and errorClass for details, although keep in mind that the contents of those fields are free-form and may change from release to release.|
