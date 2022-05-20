---
id: sql-api
title: "Druid SQL API"
sidebar_label: "Druid SQL API"
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

> Apache Druid supports two query languages: Druid SQL and [native queries](querying.md).
> This document describes the SQL language.

You can submit and cancel [Druid SQL](./sql.md) queries using the Druid SQL API.
The Druid SQL API is available at `https://ROUTER:8888/druid/v2/sql`, where `ROUTER` is the IP address of the Druid Router.

## Submit a query

To use the SQL API to make Druid SQL queries, send your query to the Router using the POST method:
```
POST https://ROUTER:8888/druid/v2/sql/
```  

Submit your query as the value of a "query" field in the JSON object within the request payload. For example:
```json
{"query" : "SELECT COUNT(*) FROM data_source WHERE foo = 'bar'"}
```

### Request body
      
|Property|Description|Default|
|--------|----|-----------|
|`query`|SQL query string.| none (required)|
|`resultFormat`|Format of query results. See [Responses](#responses) for details.|`"object"`|
|`header`|Whether or not to include a header row for the query result. See [Responses](#responses) for details.|`false`|
|`typesHeader`|Whether or not to include type information in the header. Can only be set when `header` is also `true`. See [Responses](#responses) for details.|`false`|
|`sqlTypesHeader`|Whether or not to include SQL type information in the header. Can only be set when `header` is also `true`. See [Responses](#responses) for details.|`false`|
|`context`|JSON object containing [SQL query context parameters](sql-query-context.md).|`{}` (empty)|
|`parameters`|List of query parameters for parameterized queries. Each parameter in the list should be a JSON object like `{"type": "VARCHAR", "value": "foo"}`. The type should be a SQL type; see [Data types](sql-data-types.md) for a list of supported SQL types.|`[]` (empty)|

You can use _curl_ to send SQL queries from the command-line:

```bash
$ cat query.json
{"query":"SELECT COUNT(*) AS TheCount FROM data_source"}

$ curl -XPOST -H'Content-Type: application/json' http://ROUTER:8888/druid/v2/sql/ -d @query.json
[{"TheCount":24433}]
```

There are a variety of [SQL query context parameters](sql-query-context.md) you can provide by adding a "context" map,
like:

```json
{
  "query" : "SELECT COUNT(*) FROM data_source WHERE foo = 'bar' AND __time > TIMESTAMP '2000-01-01 00:00:00'",
  "context" : {
    "sqlTimeZone" : "America/Los_Angeles"
  }
}
```

Parameterized SQL queries are also supported:

```json
{
  "query" : "SELECT COUNT(*) FROM data_source WHERE foo = ? AND __time > ?",
  "parameters": [
    { "type": "VARCHAR", "value": "bar"},
    { "type": "TIMESTAMP", "value": "2000-01-01 00:00:00" }
  ]
}
```

Metadata is available over HTTP POST by querying [metadata tables](sql-metadata-tables.md).

### Responses

#### Result formats

Druid SQL's HTTP POST API supports a variety of result formats. You can specify these by adding a "resultFormat"
parameter, like:

```json
{
  "query" : "SELECT COUNT(*) FROM data_source WHERE foo = 'bar' AND __time > TIMESTAMP '2000-01-01 00:00:00'",
  "resultFormat" : "array"
}
```

To request a header with information about column names, set `header` to true in your request.
When you set `header` to true, you can optionally include `typesHeader` and `sqlTypesHeader` as well, which gives
you information about [Druid runtime and SQL types](sql-data-types.md) respectively. You can request all these headers
with a request like:

```json
{
  "query" : "SELECT COUNT(*) FROM data_source WHERE foo = 'bar' AND __time > TIMESTAMP '2000-01-01 00:00:00'",
  "resultFormat" : "array",
  "header" : true,
  "typesHeader" : true,
  "sqlTypesHeader" : true
}
```

The following table shows supported result formats:

|Format|Description|Header description|Content-Type|
|------|-----------|------------------|------------|
|`object`|The default, a JSON array of JSON objects. Each object's field names match the columns returned by the SQL query, and are provided in the same order as the SQL query.|If `header` is true, the first row is an object where the fields are column names. Each field's value is either null (if `typesHeader` and `sqlTypesHeader` are false) or an object that contains the Druid type as `type` (if `typesHeader` is true) and the SQL type as `sqlType` (if `sqlTypesHeader` is true).|application/json|
|`array`|JSON array of JSON arrays. Each inner array has elements matching the columns returned by the SQL query, in order.|If `header` is true, the first row is an array of column names. If `typesHeader` is true, the next row is an array of Druid types. If `sqlTypesHeader` is true, the next row is an array of SQL types.|application/json|
|`objectLines`|Like `object`, but the JSON objects are separated by newlines instead of being wrapped in a JSON array. This can make it easier to parse the entire response set as a stream, if you do not have ready access to a streaming JSON parser. To make it possible to detect a truncated response, this format includes a trailer of one blank line.|Same as `object`.|text/plain|
|`arrayLines`|Like `array`, but the JSON arrays are separated by newlines instead of being wrapped in a JSON array. This can make it easier to parse the entire response set as a stream, if you do not have ready access to a streaming JSON parser. To make it possible to detect a truncated response, this format includes a trailer of one blank line.|Same as `array`, except the rows are separated by newlines.|text/plain|
|`csv`|Comma-separated values, with one row per line. Individual field values may be escaped by being surrounded in double quotes. If double quotes appear in a field value, they will be escaped by replacing them with double-double-quotes like `""this""`. To make it possible to detect a truncated response, this format includes a trailer of one blank line.|Same as `array`, except the lists are in CSV format.|text/csv|

If `typesHeader` is set to true, [Druid type](sql-data-types.md) information is included in the response. Complex types,
like sketches, will be reported as `COMPLEX<typeName>` if a particular complex type name is known for that field,
or as `COMPLEX` if the particular type name is unknown or mixed. If `sqlTypesHeader` is set to true,
[SQL type](sql-data-types.md) information is included in the response. It is possible to set both `typesHeader` and
`sqlTypesHeader` at once. Both parameters require that `header` is also set.

To aid in building clients that are compatible with older Druid versions, Druid returns the HTTP header
`X-Druid-SQL-Header-Included: yes` if `header` was set to true and if the version of Druid the client is connected to
understands the `typesHeader` and `sqlTypesHeader` parameters. This HTTP response header is present irrespective of
whether `typesHeader` or `sqlTypesHeader` are set or not.

Druid returns the SQL query identifier in the `X-Druid-SQL-Query-Id` HTTP header.
This query id will be assigned the value of `sqlQueryId` from the [query context parameters](sql-query-context.md)
if specified, else Druid will generate a SQL query id for you.

#### Errors

Errors that occur before the response body is sent will be reported in JSON, with an HTTP 500 status code, in the
same format as [native Druid query errors](../querying/querying.md#query-errors). If an error occurs while the response body is
being sent, at that point it is too late to change the HTTP status code or report a JSON error, so the response will
simply end midstream and an error will be logged by the Druid server that was handling your request.

As a caller, it is important that you properly handle response truncation. This is easy for the `object` and `array`
formats, since truncated responses will be invalid JSON. For the line-oriented formats, you should check the
trailer they all include: one blank line at the end of the result set. If you detect a truncated response, either
through a JSON parsing error or through a missing trailing newline, you should assume the response was not fully
delivered due to an error.

## Cancel a query

You can use the HTTP DELETE method to cancel a SQL query on either the Router or the Broker. When you cancel a query, Druid handles the cancellation in a best-effort manner. It marks the query canceled immediately and aborts the query execution as soon as possible. However, your query may run for a short time after your cancellation request.

Druid SQL's HTTP DELETE method uses the following syntax:
```
DELETE https://ROUTER:8888/druid/v2/sql/{sqlQueryId}
```

The DELETE method requires the `sqlQueryId` path parameter. To predict the query id you must set it in the query context. Druid does not enforce unique `sqlQueryId` in the query context. If you issue a cancel request for a `sqlQueryId` active in more than one query context, Druid cancels all requests that use the query id.

For example if you issue the following query:
```bash
curl --request POST 'https://ROUTER:8888/druid/v2/sql' \
--header 'Content-Type: application/json' \
--data-raw '{"query" : "SELECT sleep(CASE WHEN sum_added > 0 THEN 1 ELSE 0 END) FROM wikiticker WHERE sum_added > 0 LIMIT 15",
"context" : {"sqlQueryId" : "myQuery01"}}'
```
You can cancel the query using the query id `myQuery01` as follows:
```bash
curl --request DELETE 'https://ROUTER:8888/druid/v2/sql/myQuery01' \
```

Cancellation requests require READ permission on all resources used in the sql query. 

Druid returns an HTTP 202 response for successful deletion requests.

Druid returns an HTTP 404 response in the following cases:
  - `sqlQueryId` is incorrect.
  - The query completes before your cancellation request is processed.
  
Druid returns an HTTP 403 response for authorization failure.
