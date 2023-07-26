---
id: sql-api
title: Druid SQL API
sidebar_label: Druid SQL
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

Apache Druid supports two query languages: Druid SQL and [native queries](../querying/querying.md). This topic describes the SQL language.

You can submit and cancel [Druid SQL](../querying/sql.md) queries using the Druid SQL API.

In this topic, `http://ROUTER_IP:ROUTER_PORT` is a place holder for your Router service address and port. Replace it with the information for your deployment. For example, use `http://localhost:8888` for quickstart deployments. 

## Submit a query

Submit a SQL-based query in the JSON request body. Returns a JSON object with the database results and a set of header metadata associated with the query. 

This endpoint also supports querying metadata by querying [metadata tables](../querying/sql-metadata-tables.md).
### URL

<code class="postAPI">POST</code> <code>/druid/v2/sql</code>

### Request body

* `query`: SQL query string.
* `resultFormat`: Format of query results.
  * `object`: Returns a JSON array of JSON objects with the HTTP header `Content-Type: application/json`.
  * `array`: Returns a JSON array of JSON arrays with the HTTP header `Content-Type: application/json`.
  * `objectLines`: Returns newline-delimited JSON objects with a trailing blank line. Sent with the HTTP header `Content-Type: text/plain`.
  * `arrayLines`: Returns newline-delimited JSON arrays with a trailing blank line. Sent with the HTTP header `Content-Type: text/plain`. 
  * `csv`: Returns a comma-separated values with one row per line and a trailing blank line. Sent with the HTTP header `Content-Type: text/csv`. 
* `header`: Adds a header row with information on column names for the query result when set to `true`. You can optionally include `typesHeader` and `sqlTypesHeader`.<br/><br/> Complex types, like sketches, will be reported as `COMPLEX<typeName>` if a particular complex type name is known for that field, or as `COMPLEX` if the particular type name is unknown or mixed.<br/><br/>If working with an older version of Druid, set `header` to `true` to verify compatibility. Druid returns the HTTP header `X-Druid-SQL-Header-Included: yes` when the client connects to an older Druid version with support for the `typesHeader` and `sqlTypesHeader` parameters. Additionally, Druid returns a `X-Druid-SQL-Query-Id` HTTP header with the value of `sqlQueryId` from the [query context parameters](../querying/sql-query-context.md) if specified, else Druid will generate a SQL query ID.
* `typesHeader`: Adds Druid runtime type information in the header. Can only be set when `header` is also `true`.
* `sqlTypesHeader`: Adds SQL type information in the header. Can only be set when `header` is also `true`.
* `context`: JSON object containing optional [SQL query context parameters](../querying/sql-query-context.md), such as to set the query ID, time zone, and whether to use an approximation algorithm for distinct count.
* `parameters`: List of query parameters for parameterized queries. Each parameter in the list should be a JSON object like `{"type": "VARCHAR", "value": "foo"}`. The type should be a SQL type; see [Data types](../querying/sql-data-types.md) for a list of supported SQL types.


### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->

*Successfully submitted query* 

<!--400 BAD REQUEST-->

*Error thrown due to bad query. Returns a JSON object detailing the error with the following format:* 

```json
{
    "error": "A well-defined error code.",
    "errorMessage": "A message with additional details about the error.",
    "errorClass": "Class of exception that caused this error.",
    "host": "The host on which the error occurred."
}
```
<!--500 INTERNAL SERVER ERROR-->

*Request not sent due to unexpected conditions. Returns a JSON object detailing the error with the following format:* 

```json
{
    "error": "A well-defined error code.",
    "errorMessage": "A message with additional details about the error.",
    "errorClass": "Class of exception that caused this error.",
    "host": "The host on which the error occurred."
}
```

<!--END_DOCUSAURUS_CODE_TABS-->

---


### Sample request

The following example retrieves all rows in the `wikipedia` datasource where the `user` is `BlueMoon2662`.

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->

```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/v2/sql" \
--header 'Content-Type: application/json' \
--data '{
    "query": "SELECT * FROM wikipedia WHERE user='\''BlueMoon2662'\''",
    "context" : {"sqlQueryId" : "request01"},
    "header" : true,
    "typesHeader" : true,
      "sqlTypesHeader" : true
}'
```

<!--HTTP-->

```HTTP
POST /druid/v2/sql HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
Content-Type: application/json
Content-Length: 192

{
    "query": "SELECT * FROM wikipedia WHERE user='BlueMoon2662'",
    "context" : {"sqlQueryId" : "request01"},
    "header" : true,
    "typesHeader" : true,
      "sqlTypesHeader" : true
}
```

<!--END_DOCUSAURUS_CODE_TABS-->

### Sample response

<details>
  <summary>Click to show sample response</summary>

  ```json
[
    {
        "__time": {
            "type": "LONG",
            "sqlType": "TIMESTAMP"
        },
        "channel": {
            "type": "STRING",
            "sqlType": "VARCHAR"
        },
        "cityName": {
            "type": "STRING",
            "sqlType": "VARCHAR"
        },
        "comment": {
            "type": "STRING",
            "sqlType": "VARCHAR"
        },
        "countryIsoCode": {
            "type": "STRING",
            "sqlType": "VARCHAR"
        },
        "countryName": {
            "type": "STRING",
            "sqlType": "VARCHAR"
        },
        "isAnonymous": {
            "type": "LONG",
            "sqlType": "BIGINT"
        },
        "isMinor": {
            "type": "LONG",
            "sqlType": "BIGINT"
        },
        "isNew": {
            "type": "LONG",
            "sqlType": "BIGINT"
        },
        "isRobot": {
            "type": "LONG",
            "sqlType": "BIGINT"
        },
        "isUnpatrolled": {
            "type": "LONG",
            "sqlType": "BIGINT"
        },
        "metroCode": {
            "type": "LONG",
            "sqlType": "BIGINT"
        },
        "namespace": {
            "type": "STRING",
            "sqlType": "VARCHAR"
        },
        "page": {
            "type": "STRING",
            "sqlType": "VARCHAR"
        },
        "regionIsoCode": {
            "type": "STRING",
            "sqlType": "VARCHAR"
        },
        "regionName": {
            "type": "STRING",
            "sqlType": "VARCHAR"
        },
        "user": {
            "type": "STRING",
            "sqlType": "VARCHAR"
        },
        "delta": {
            "type": "LONG",
            "sqlType": "BIGINT"
        },
        "added": {
            "type": "LONG",
            "sqlType": "BIGINT"
        },
        "deleted": {
            "type": "LONG",
            "sqlType": "BIGINT"
        }
    },
    {
        "__time": "2015-09-12T00:47:53.259Z",
        "channel": "#ja.wikipedia",
        "cityName": "",
        "comment": "/* 対戦通算成績と得失点 */",
        "countryIsoCode": "",
        "countryName": "",
        "isAnonymous": 0,
        "isMinor": 1,
        "isNew": 0,
        "isRobot": 0,
        "isUnpatrolled": 0,
        "metroCode": 0,
        "namespace": "Main",
        "page": "アルビレックス新潟の年度別成績一覧",
        "regionIsoCode": "",
        "regionName": "",
        "user": "BlueMoon2662",
        "delta": 14,
        "added": 14,
        "deleted": 0
    }
]
  ```
</details>

## Cancel a query

Cancels a query on the Router or the Broker with the associated `sqlQueryId`. Queries can only be canceled with a valid `sqlQueryId`. It must be set in the query context when the query is submitted. Note that Druid does not enforce unique `sqlQueryId` in the query context. Druid cancels all requests that use the same query id.

When you cancel a query, Druid handles the cancellation in a best-effort manner. It marks the query canceled immediately and aborts the query execution as soon as possible. However, your query may run for a short time after your cancellation request.

Cancellation requests require READ permission on all resources used in the SQL query. 


### URL

<code class="deleteAPI">DELETE</code> <code>/druid/v2/sql/:sqlQueryId</code>

### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--202 SUCCESS-->

*Successfully deleted query*

<!--403 FORBIDDEN-->

*Authorization failure* 

<!--404 NOT FOUND-->


*Invalid `sqlQueryId` or query was completed before cancellation request* 

<!--END_DOCUSAURUS_CODE_TABS-->

### Sample request

The following example cancels a request with specified query ID `request01`.

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->

```shell
curl --request DELETE "http://ROUTER_IP:ROUTER_PORT/druid/v2/sql/request01"
```

<!--HTTP-->

```HTTP
DELETE /druid/v2/sql/request01 HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

<!--END_DOCUSAURUS_CODE_TABS-->

### Sample response

A successful response results in an `HTTP 202` and an empty response body.

## Submit a query for data in deep storage

Submit a query for data stored in deep storage. Any data ingested into Druid is placed into deep storage. The query is contained in the "query" field in the JSON object within the request payload.

Note that at least part of a datasource must be available on a Historical process so that Druid can plan your query and only the user who submits a query can see the results.

### URL
<code class="postAPI">POST</code> <code>/druid/v2/sql/statements</code>

### Request body

Generally, the `sql` and `sql/statements` endpoints support the same response body fields with minor differences. Refer to the [Submit a query](#request-body) endpoint for request body construction.

There are additional context parameters for `sql/statements` specifically: 
   - `executionMode`  determines how query results are fetched. The currently supported mode is `ASYNC`. 
   - `selectDestination` set to `DURABLE_STORAGE` instructs Druid to write the results from SELECT queries to durable storage. Note that this requires you to have [durable storage for MSQ enabled](../operations/durable-storage.md).
  
Note that the only supported value for `resultFormat` is JSON.

### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->

<br/>

*Successfully queried from deep storage* 

<!--400 BAD REQUEST-->

<br/>

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

<!--END_DOCUSAURUS_CODE_TABS-->

---

### Sample request

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->

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

<!--HTTP-->

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

<!--END_DOCUSAURUS_CODE_TABS-->

### Sample response

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

## Get query status

Retrieves information about the query associated with the given query ID.

### URL
<code name="getAPI">GET</code> <code>/druid/v2/sql/statements/:queryId</code>
