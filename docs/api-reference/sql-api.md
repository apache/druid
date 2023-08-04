---
id: sql-api
title: Druid SQL API
sidebar_label: Druid SQL
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';


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

:::info
 Apache Druid supports two query languages: Druid SQL and [native queries](../querying/querying.md).
 This document describes the SQL language.
:::

You can submit and cancel [Druid SQL](../querying/sql.md) queries using the Druid SQL API.
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
|`context`|JSON object containing [SQL query context parameters](../querying/sql-query-context.md).|`{}` (empty)|
|`parameters`|List of query parameters for parameterized queries. Each parameter in the list should be a JSON object like `{"type": "VARCHAR", "value": "foo"}`. The type should be a SQL type; see [Data types](../querying/sql-data-types.md) for a list of supported SQL types.|`[]` (empty)|

You can use _curl_ to send SQL queries from the command-line:

```bash
$ cat query.json
{"query":"SELECT COUNT(*) AS TheCount FROM data_source"}

$ curl -XPOST -H'Content-Type: application/json' http://ROUTER:8888/druid/v2/sql/ -d @query.json
[{"TheCount":24433}]
```

There are a variety of [SQL query context parameters](../querying/sql-query-context.md) you can provide by adding a "context" map,
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

Metadata is available over HTTP POST by querying [metadata tables](../querying/sql-metadata-tables.md).

### Responses

#### Result formats

Druid SQL's HTTP POST API supports a variety of result formats. You can specify these by adding a `resultFormat` parameter, like:

```json
{
  "query" : "SELECT COUNT(*) FROM data_source WHERE foo = 'bar' AND __time > TIMESTAMP '2000-01-01 00:00:00'",
  "resultFormat" : "array"
}
```

To request a header with information about column names, set `header` to true in your request.
When you set `header` to true, you can optionally include `typesHeader` and `sqlTypesHeader` as well, which gives
you information about [Druid runtime and SQL types](../querying/sql-data-types.md) respectively. You can request all these headers
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

If `typesHeader` is set to true, [Druid type](../querying/sql-data-types.md) information is included in the response. Complex types,
like sketches, will be reported as `COMPLEX<typeName>` if a particular complex type name is known for that field,
or as `COMPLEX` if the particular type name is unknown or mixed. If `sqlTypesHeader` is set to true,
[SQL type](../querying/sql-data-types.md) information is included in the response. It is possible to set both `typesHeader` and
`sqlTypesHeader` at once. Both parameters require that `header` is also set.

To aid in building clients that are compatible with older Druid versions, Druid returns the HTTP header
`X-Druid-SQL-Header-Included: yes` if `header` was set to true and if the version of Druid the client is connected to
understands the `typesHeader` and `sqlTypesHeader` parameters. This HTTP response header is present irrespective of
whether `typesHeader` or `sqlTypesHeader` are set or not.

Druid returns the SQL query identifier in the `X-Druid-SQL-Query-Id` HTTP header.
This query id will be assigned the value of `sqlQueryId` from the [query context parameters](../querying/sql-query-context.md)
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

Cancellation requests require READ permission on all resources used in the SQL query.

Druid returns an HTTP 202 response for successful deletion requests.

Druid returns an HTTP 404 response in the following cases:
  - `sqlQueryId` is incorrect.
  - The query completes before your cancellation request is processed.

Druid returns an HTTP 403 response for authorization failure.

## Query from deep storage

> Query from deep storage is an [experimental feature](../development/experimental.md).

You can use the `sql/statements` endpoint to query segments that exist only in deep storage and are not loaded onto your Historical processes as determined by your load rules.

Note that at least one segment of a datasource must be available on a Historical process so that the Broker can plan your query. A quick way to check if this is true is whether or not a datasource is visible in the Druid console.


For more information, see [Query from deep storage](../querying/query-from-deep-storage.md).

### Submit a query

Submit a query for data stored in deep storage. Any data ingested into Druid is placed into deep storage. The query is contained in the "query" field in the JSON object within the request payload.

Note that at least part of a datasource must be available on a Historical process so that Druid can plan your query and only the user who submits a query can see the results.

#### URL

<code class="postAPI">POST</code> <code>/druid/v2/sql/statements</code>

#### Request body

Generally, the `sql` and `sql/statements` endpoints support the same response body fields with minor differences. For general information about the available fields, see [Submit a query to the `sql` endpoint](#submit-a-query).

Keep the following in mind when submitting queries to the `sql/statements` endpoint:

- There are additional context parameters  for `sql/statements`:

   - `executionMode`  determines how query results are fetched. Druid currently only supports `ASYNC`. You must manually retrieve your results after the query completes.
   - `selectDestination` determines where final results get written. By default, results are written to task reports. Set this parameter to `durableStorage` to instruct Druid to write the results from SELECT queries to durable storage, which allows you to fetch larger result sets. Note that this requires you to have [durable storage for MSQ enabled](../operations/durable-storage.md).

- The only supported value for `resultFormat` is JSON LINES.

#### Responses

<Tabs>

<TabItem value="1" label="200 SUCCESS">


*Successfully queried from deep storage*

</TabItem>
<TabItem value="2" label="400 BAD REQUEST">


*Error thrown due to bad query. Returns a JSON object detailing the error with the following format:*

```json
{
    "error": "Summary of the encountered error.",
    "errorClass": "Class of exception that caused this error.",
    "host": "The host on which the error occurred.",
    "errorCode": "Well-defined error code.",
    "persona": "Role or persona associated with the error.",
    "category": "Classification of the error.",
    "errorMessage": "Summary of the encountered issue with expanded information.",
    "context": "Additional context about the error."
}
```

</TabItem>
</Tabs>

---

#### Sample request

<Tabs>

<TabItem value="3" label="cURL">


```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/v2/sql/statements" \
--header 'Content-Type: application/json' \
--data '{
    "query": "SELECT * FROM wikipedia WHERE user='\''BlueMoon2662'\''",
    "context": {
        "executionMode":"ASYNC"
    }
}'
```

</TabItem>
<TabItem value="4" label="HTTP">


```HTTP
POST /druid/v2/sql/statements HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
Content-Type: application/json
Content-Length: 134

{
    "query": "SELECT * FROM wikipedia WHERE user='BlueMoon2662'",
    "context": {
        "executionMode":"ASYNC"
    }
}
```

</TabItem>
</Tabs>

#### Sample response

<details>
  <summary>Click to show sample response</summary>

  ```json
{
    "queryId": "query-b82a7049-b94f-41f2-a230-7fef94768745",
    "state": "ACCEPTED",
    "createdAt": "2023-07-26T21:16:25.324Z",
    "schema": [
        {
            "name": "__time",
            "type": "TIMESTAMP",
            "nativeType": "LONG"
        },
        {
            "name": "channel",
            "type": "VARCHAR",
            "nativeType": "STRING"
        },
        {
            "name": "cityName",
            "type": "VARCHAR",
            "nativeType": "STRING"
        },
        {
            "name": "comment",
            "type": "VARCHAR",
            "nativeType": "STRING"
        },
        {
            "name": "countryIsoCode",
            "type": "VARCHAR",
            "nativeType": "STRING"
        },
        {
            "name": "countryName",
            "type": "VARCHAR",
            "nativeType": "STRING"
        },
        {
            "name": "isAnonymous",
            "type": "BIGINT",
            "nativeType": "LONG"
        },
        {
            "name": "isMinor",
            "type": "BIGINT",
            "nativeType": "LONG"
        },
        {
            "name": "isNew",
            "type": "BIGINT",
            "nativeType": "LONG"
        },
        {
            "name": "isRobot",
            "type": "BIGINT",
            "nativeType": "LONG"
        },
        {
            "name": "isUnpatrolled",
            "type": "BIGINT",
            "nativeType": "LONG"
        },
        {
            "name": "metroCode",
            "type": "BIGINT",
            "nativeType": "LONG"
        },
        {
            "name": "namespace",
            "type": "VARCHAR",
            "nativeType": "STRING"
        },
        {
            "name": "page",
            "type": "VARCHAR",
            "nativeType": "STRING"
        },
        {
            "name": "regionIsoCode",
            "type": "VARCHAR",
            "nativeType": "STRING"
        },
        {
            "name": "regionName",
            "type": "VARCHAR",
            "nativeType": "STRING"
        },
        {
            "name": "user",
            "type": "VARCHAR",
            "nativeType": "STRING"
        },
        {
            "name": "delta",
            "type": "BIGINT",
            "nativeType": "LONG"
        },
        {
            "name": "added",
            "type": "BIGINT",
            "nativeType": "LONG"
        },
        {
            "name": "deleted",
            "type": "BIGINT",
            "nativeType": "LONG"
        }
    ],
    "durationMs": -1
}
  ```
</details>

### Get query status

Retrieves information about the query associated with the given query ID. The response matches the response from the POST API if the query is accepted or running and the execution mode is  `ASYNC`. In addition to the fields that this endpoint shares with `POST /sql/statements`, a completed query's status includes the following:

- A `result` object that summarizes information about your results, such as the total number of rows and sample records.
- A `pages` object that includes the following information for each page of results:
  -  `numRows`: the number of rows in that page of results.
  - `sizeInBytes`: the size of the page.
  - `id`: the page number that you can use to reference a specific page when you get query results.

#### URL

<code class="getAPI">GET</code> <code>/druid/v2/sql/statements/:queryId</code>

#### Responses

<Tabs>

<TabItem value="5" label="200 SUCCESS">


*Successfully retrieved query status*

</TabItem>
<TabItem value="6" label="400 BAD REQUEST">


*Error thrown due to bad query. Returns a JSON object detailing the error with the following format:*

```json
{
    "error": "Summary of the encountered error.",
    "errorCode": "Well-defined error code.",
    "persona": "Role or persona associated with the error.",
    "category": "Classification of the error.",
    "errorMessage": "Summary of the encountered issue with expanded information.",
    "context": "Additional context about the error."
}
```

</TabItem>
</Tabs>

#### Sample request

The following example retrieves the status of a query with specified ID `query-9b93f6f7-ab0e-48f5-986a-3520f84f0804`.

<Tabs>

<TabItem value="7" label="cURL">


```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/v2/sql/statements/query-9b93f6f7-ab0e-48f5-986a-3520f84f0804"
```

</TabItem>
<TabItem value="8" label="HTTP">


```HTTP
GET /druid/v2/sql/statements/query-9b93f6f7-ab0e-48f5-986a-3520f84f0804 HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

</TabItem>
</Tabs>

#### Sample response

<details>
  <summary>Click to show sample response</summary>

  ```json
{
    "queryId": "query-9b93f6f7-ab0e-48f5-986a-3520f84f0804",
    "state": "SUCCESS",
    "createdAt": "2023-07-26T22:57:46.620Z",
    "schema": [
        {
            "name": "__time",
            "type": "TIMESTAMP",
            "nativeType": "LONG"
        },
        {
            "name": "channel",
            "type": "VARCHAR",
            "nativeType": "STRING"
        },
        {
            "name": "cityName",
            "type": "VARCHAR",
            "nativeType": "STRING"
        },
        {
            "name": "comment",
            "type": "VARCHAR",
            "nativeType": "STRING"
        },
        {
            "name": "countryIsoCode",
            "type": "VARCHAR",
            "nativeType": "STRING"
        },
        {
            "name": "countryName",
            "type": "VARCHAR",
            "nativeType": "STRING"
        },
        {
            "name": "isAnonymous",
            "type": "BIGINT",
            "nativeType": "LONG"
        },
        {
            "name": "isMinor",
            "type": "BIGINT",
            "nativeType": "LONG"
        },
        {
            "name": "isNew",
            "type": "BIGINT",
            "nativeType": "LONG"
        },
        {
            "name": "isRobot",
            "type": "BIGINT",
            "nativeType": "LONG"
        },
        {
            "name": "isUnpatrolled",
            "type": "BIGINT",
            "nativeType": "LONG"
        },
        {
            "name": "metroCode",
            "type": "BIGINT",
            "nativeType": "LONG"
        },
        {
            "name": "namespace",
            "type": "VARCHAR",
            "nativeType": "STRING"
        },
        {
            "name": "page",
            "type": "VARCHAR",
            "nativeType": "STRING"
        },
        {
            "name": "regionIsoCode",
            "type": "VARCHAR",
            "nativeType": "STRING"
        },
        {
            "name": "regionName",
            "type": "VARCHAR",
            "nativeType": "STRING"
        },
        {
            "name": "user",
            "type": "VARCHAR",
            "nativeType": "STRING"
        },
        {
            "name": "delta",
            "type": "BIGINT",
            "nativeType": "LONG"
        },
        {
            "name": "added",
            "type": "BIGINT",
            "nativeType": "LONG"
        },
        {
            "name": "deleted",
            "type": "BIGINT",
            "nativeType": "LONG"
        }
    ],
    "durationMs": 25591,
    "result": {
        "numTotalRows": 1,
        "totalSizeInBytes": 375,
        "dataSource": "__query_select",
        "sampleRecords": [
            [
                1442018873259,
                "#ja.wikipedia",
                "",
                "/* 対戦通算成績と得失点 */",
                "",
                "",
                0,
                1,
                0,
                0,
                0,
                0,
                "Main",
                "アルビレックス新潟の年度別成績一覧",
                "",
                "",
                "BlueMoon2662",
                14,
                14,
                0
            ]
        ],
        "pages": [
            {
                "id": 0,
                "numRows": 1,
                "sizeInBytes": 375
            }
        ]
    }
}
  ```
</details>


### Get query results

Retrieves results for completed queries. Results are separated into pages, so you can use the optional `page` parameter to refine the results you get. Druid returns information about the composition of each page and its page number (`id`). For information about pages, see [Get query status](#get-query-status).

If a page number isn't passed, all results are returned sequentially in the same response. If you have large result sets, you may encounter timeouts based on the value configured for `druid.router.http.readTimeout`.

When getting query results, keep the following in mind:

- JSON Lines is the only supported result format.
- Getting the query results for an ingestion query returns an empty response.

#### URL

<code class="getAPI">GET</code> <code>/druid/v2/sql/statements/:queryId/results</code>

#### Query parameters
* `page`
    * Int (optional)
    * Refine paginated results

#### Responses

<Tabs>

<TabItem value="9" label="200 SUCCESS">


*Successfully retrieved query results*

</TabItem>
<TabItem value="10" label="400 BAD REQUEST">


*Query in progress. Returns a JSON object detailing the error with the following format:*

```json
{
    "error": "Summary of the encountered error.",
    "errorCode": "Well-defined error code.",
    "persona": "Role or persona associated with the error.",
    "category": "Classification of the error.",
    "errorMessage": "Summary of the encountered issue with expanded information.",
    "context": "Additional context about the error."
}
```

</TabItem>
<TabItem value="11" label="404 NOT FOUND">


*Query not found, failed or canceled*

</TabItem>
<TabItem value="12" label="500 SERVER ERROR">


*Error thrown due to bad query. Returns a JSON object detailing the error with the following format:*

```json
{
    "error": "Summary of the encountered error.",
    "errorCode": "Well-defined error code.",
    "persona": "Role or persona associated with the error.",
    "category": "Classification of the error.",
    "errorMessage": "Summary of the encountered issue with expanded information.",
    "context": "Additional context about the error."
}
```

</TabItem>
</Tabs>

---

#### Sample request

The following example retrieves the status of a query with specified ID `query-f3bca219-173d-44d4-bdc7-5002e910352f`.

<Tabs>

<TabItem value="13" label="cURL">


```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/v2/sql/statements/query-f3bca219-173d-44d4-bdc7-5002e910352f/results"
```

</TabItem>
<TabItem value="14" label="HTTP">


```HTTP
GET /druid/v2/sql/statements/query-f3bca219-173d-44d4-bdc7-5002e910352f/results HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

</TabItem>
</Tabs>

#### Sample response

<details>
  <summary>Click to show sample response</summary>

  ```json
[
    {
        "__time": 1442018818771,
        "channel": "#en.wikipedia",
        "cityName": "",
        "comment": "added project",
        "countryIsoCode": "",
        "countryName": "",
        "isAnonymous": 0,
        "isMinor": 0,
        "isNew": 0,
        "isRobot": 0,
        "isUnpatrolled": 0,
        "metroCode": 0,
        "namespace": "Talk",
        "page": "Talk:Oswald Tilghman",
        "regionIsoCode": "",
        "regionName": "",
        "user": "GELongstreet",
        "delta": 36,
        "added": 36,
        "deleted": 0
    },
    {
        "__time": 1442018820496,
        "channel": "#ca.wikipedia",
        "cityName": "",
        "comment": "Robot inserta {{Commonscat}} que enllaça amb [[commons:category:Rallicula]]",
        "countryIsoCode": "",
        "countryName": "",
        "isAnonymous": 0,
        "isMinor": 1,
        "isNew": 0,
        "isRobot": 1,
        "isUnpatrolled": 0,
        "metroCode": 0,
        "namespace": "Main",
        "page": "Rallicula",
        "regionIsoCode": "",
        "regionName": "",
        "user": "PereBot",
        "delta": 17,
        "added": 17,
        "deleted": 0
    },
    {
        "__time": 1442018825474,
        "channel": "#en.wikipedia",
        "cityName": "Auburn",
        "comment": "/* Status of peremptory norms under international law */ fixed spelling of 'Wimbledon'",
        "countryIsoCode": "AU",
        "countryName": "Australia",
        "isAnonymous": 1,
        "isMinor": 0,
        "isNew": 0,
        "isRobot": 0,
        "isUnpatrolled": 0,
        "metroCode": 0,
        "namespace": "Main",
        "page": "Peremptory norm",
        "regionIsoCode": "NSW",
        "regionName": "New South Wales",
        "user": "60.225.66.142",
        "delta": 0,
        "added": 0,
        "deleted": 0
    },
    {
        "__time": 1442018828770,
        "channel": "#vi.wikipedia",
        "cityName": "",
        "comment": "fix Lỗi CS1: ngày tháng",
        "countryIsoCode": "",
        "countryName": "",
        "isAnonymous": 0,
        "isMinor": 1,
        "isNew": 0,
        "isRobot": 1,
        "isUnpatrolled": 0,
        "metroCode": 0,
        "namespace": "Main",
        "page": "Apamea abruzzorum",
        "regionIsoCode": "",
        "regionName": "",
        "user": "Cheers!-bot",
        "delta": 18,
        "added": 18,
        "deleted": 0
    },
    {
        "__time": 1442018831862,
        "channel": "#vi.wikipedia",
        "cityName": "",
        "comment": "clean up using [[Project:AWB|AWB]]",
        "countryIsoCode": "",
        "countryName": "",
        "isAnonymous": 0,
        "isMinor": 0,
        "isNew": 0,
        "isRobot": 1,
        "isUnpatrolled": 0,
        "metroCode": 0,
        "namespace": "Main",
        "page": "Atractus flammigerus",
        "regionIsoCode": "",
        "regionName": "",
        "user": "ThitxongkhoiAWB",
        "delta": 18,
        "added": 18,
        "deleted": 0
    },
    {
        "__time": 1442018833987,
        "channel": "#vi.wikipedia",
        "cityName": "",
        "comment": "clean up using [[Project:AWB|AWB]]",
        "countryIsoCode": "",
        "countryName": "",
        "isAnonymous": 0,
        "isMinor": 0,
        "isNew": 0,
        "isRobot": 1,
        "isUnpatrolled": 0,
        "metroCode": 0,
        "namespace": "Main",
        "page": "Agama mossambica",
        "regionIsoCode": "",
        "regionName": "",
        "user": "ThitxongkhoiAWB",
        "delta": 18,
        "added": 18,
        "deleted": 0
    },
    {
        "__time": 1442018837009,
        "channel": "#ca.wikipedia",
        "cityName": "",
        "comment": "/* Imperi Austrohongarès */",
        "countryIsoCode": "",
        "countryName": "",
        "isAnonymous": 0,
        "isMinor": 0,
        "isNew": 0,
        "isRobot": 0,
        "isUnpatrolled": 0,
        "metroCode": 0,
        "namespace": "Main",
        "page": "Campanya dels Balcans (1914-1918)",
        "regionIsoCode": "",
        "regionName": "",
        "user": "Jaumellecha",
        "delta": -20,
        "added": 0,
        "deleted": 20
    },
    {
        "__time": 1442018839591,
        "channel": "#en.wikipedia",
        "cityName": "",
        "comment": "adding comment on notability and possible COI",
        "countryIsoCode": "",
        "countryName": "",
        "isAnonymous": 0,
        "isMinor": 0,
        "isNew": 1,
        "isRobot": 0,
        "isUnpatrolled": 1,
        "metroCode": 0,
        "namespace": "Talk",
        "page": "Talk:Dani Ploeger",
        "regionIsoCode": "",
        "regionName": "",
        "user": "New Media Theorist",
        "delta": 345,
        "added": 345,
        "deleted": 0
    },
    {
        "__time": 1442018841578,
        "channel": "#en.wikipedia",
        "cityName": "",
        "comment": "Copying assessment table to wiki",
        "countryIsoCode": "",
        "countryName": "",
        "isAnonymous": 0,
        "isMinor": 0,
        "isNew": 0,
        "isRobot": 1,
        "isUnpatrolled": 0,
        "metroCode": 0,
        "namespace": "User",
        "page": "User:WP 1.0 bot/Tables/Project/Pubs",
        "regionIsoCode": "",
        "regionName": "",
        "user": "WP 1.0 bot",
        "delta": 121,
        "added": 121,
        "deleted": 0
    },
    {
        "__time": 1442018845821,
        "channel": "#vi.wikipedia",
        "cityName": "",
        "comment": "clean up using [[Project:AWB|AWB]]",
        "countryIsoCode": "",
        "countryName": "",
        "isAnonymous": 0,
        "isMinor": 0,
        "isNew": 0,
        "isRobot": 1,
        "isUnpatrolled": 0,
        "metroCode": 0,
        "namespace": "Main",
        "page": "Agama persimilis",
        "regionIsoCode": "",
        "regionName": "",
        "user": "ThitxongkhoiAWB",
        "delta": 18,
        "added": 18,
        "deleted": 0
    }
]
  ```
</details>

### Cancel a query

Cancels a running or accepted query.

#### URL

<code class="deleteAPI">DELETE</code> <code>/druid/v2/sql/statements/:queryId</code>

#### Responses

<Tabs>

<TabItem value="15" label="200 OK">


*A no op operation since the query is not in a state to be cancelled*

</TabItem>
<TabItem value="16" label="202 ACCEPTED">


*Successfully accepted query for cancellation*

</TabItem>
<TabItem value="17" label="404 SERVER ERROR">


*Invalid query ID. Returns a JSON object detailing the error with the following format:*

```json
{
    "error": "Summary of the encountered error.",
    "errorCode": "Well-defined error code.",
    "persona": "Role or persona associated with the error.",
    "category": "Classification of the error.",
    "errorMessage": "Summary of the encountered issue with expanded information.",
    "context": "Additional context about the error."
}
```

</TabItem>
</Tabs>

---

#### Sample request

The following example cancels a query with specified ID `query-945c9633-2fa2-49ab-80ae-8221c38c024da`.

<Tabs>

<TabItem value="18" label="cURL">


```shell
curl --request DELETE "http://ROUTER_IP:ROUTER_PORT/druid/v2/sql/statements/query-945c9633-2fa2-49ab-80ae-8221c38c024da"
```

</TabItem>
<TabItem value="19" label="HTTP">


```HTTP
DELETE /druid/v2/sql/statements/query-945c9633-2fa2-49ab-80ae-8221c38c024da HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

</TabItem>
</Tabs>

#### Sample response

A successful request returns a `202 ACCEPTED` response and an empty response.
