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

In this topic, `http://ROUTER_IP:ROUTER_PORT` is a placeholder for your Router service address and port. Replace it with the information for your deployment. For example, use `http://localhost:8888` for quickstart deployments.

## Query from Historicals

### Submit a query

Submits a SQL-based query in the JSON request body. Returns a JSON object with the query results and optional metadata for the results. You can also use this endpoint to query [metadata tables](../querying/sql-metadata-tables.md).

Each query has an associated SQL query ID. You can set this ID manually using the SQL context parameter `sqlQueryId`. If not set, Druid automatically generates `sqlQueryId` and returns it in the response header for `X-Druid-SQL-Query-Id`. Note that you need the `sqlQueryId` to [cancel a query](#cancel-a-query).

#### URL

<code class="postAPI">POST</code> <code>/druid/v2/sql</code>

#### Request body

The request body takes the following properties:

* `query`: SQL query string.

* `resultFormat`: String that indicates the format to return query results. Select one of the following formats:
  * `object`: Returns a JSON array of JSON objects with the HTTP response header `Content-Type: application/json`.
  * `array`: Returns a JSON array of JSON arrays with the HTTP response header `Content-Type: application/json`.
  * `objectLines`: Returns newline-delimited JSON objects with a trailing blank line. Returns the HTTP response header `Content-Type: text/plain`.
  * `arrayLines`: Returns newline-delimited JSON arrays with a trailing blank line. Returns the HTTP response header `Content-Type: text/plain`.
  * `csv`: Returns a comma-separated values with one row per line and a trailing blank line. Returns the HTTP response header `Content-Type: text/csv`.

* `header`: Boolean value that determines whether to return information on column names. When set to `true`, Druid returns the column names as the first row of the results. To also get information on the column types, set `typesHeader` or `sqlTypesHeader` to `true`. For a comparative overview of data formats and configurations for the header, see the [Query output format](#query-output-format) table.

* `typesHeader`: Adds Druid runtime type information in the header. Requires `header` to be set to `true`. Complex types, like sketches, will be reported as `COMPLEX<typeName>` if a particular complex type name is known for that field, or as `COMPLEX` if the particular type name is unknown or mixed.

* `sqlTypesHeader`: Adds SQL type information in the header. Requires `header` to be set to `true`.

   For compatibility, Druid returns the HTTP header `X-Druid-SQL-Header-Included: yes` when all of the following conditions are met:
   * The `header` property is set to true.
   * The version of Druid supports `typesHeader` and `sqlTypesHeader`, regardless of whether either property is set.

* `context`: JSON object containing optional [SQL query context parameters](../querying/sql-query-context.md), such as to set the query ID, time zone, and whether to use an approximation algorithm for distinct count.

* `parameters`: List of query parameters for parameterized queries. Each parameter in the array should be a JSON object containing the parameter's SQL data type and parameter value. For a list of supported SQL types, see [Data types](../querying/sql-data-types.md).

    For example:
    ```json
    "parameters": [
        {
            "type": "VARCHAR",
            "value": "bar"
        }
    ]
    ```

#### Responses

<Tabs>

<TabItem value="1" label="200 SUCCESS">


*Successfully submitted query*

</TabItem>
<TabItem value="2" label="400 BAD REQUEST">


*Error thrown due to bad query. Returns a JSON object detailing the error with the following format:*

```json
{
    "error": "A well-defined error code.",
    "errorMessage": "A message with additional details about the error.",
    "errorClass": "Class of exception that caused this error.",
    "host": "The host on which the error occurred."
}
```
</TabItem>
<TabItem value="3" label="500 INTERNAL SERVER ERROR">


*Request not sent due to unexpected conditions. Returns a JSON object detailing the error with the following format:*

```json
{
    "error": "A well-defined error code.",
    "errorMessage": "A message with additional details about the error.",
    "errorClass": "Class of exception that caused this error.",
    "host": "The host on which the error occurred."
}
```

</TabItem>
</Tabs>


---


#### Sample request

The following example retrieves all rows in the `wikipedia` datasource where the `user` is `BlueMoon2662`. The query is assigned the ID `request01` using the `sqlQueryId` context parameter. The optional properties `header`, `typesHeader`, and `sqlTypesHeader` are set to `true` to include type information to the response.

<Tabs>

<TabItem value="4" label="cURL">


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

</TabItem>
<TabItem value="5" label="HTTP">


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

</TabItem>
</Tabs>

#### Sample response

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

### Cancel a query

Cancels a query on the Router or the Broker with the associated `sqlQueryId`. The `sqlQueryId` can be manually set when the query is submitted in the query context parameter, or if not set, Druid will generate one and return it in the response header when the query is successfully submitted. Note that Druid does not enforce a unique `sqlQueryId` in the query context. If you've set the same `sqlQueryId` for multiple queries, Druid cancels all requests with that query ID.

When you cancel a query, Druid handles the cancellation in a best-effort manner. Druid immediately marks the query as canceled and aborts the query execution as soon as possible. However, the query may continue running for a short time after you make the cancellation request.

Cancellation requests require READ permission on all resources used in the SQL query.

#### URL

<code class="deleteAPI">DELETE</code> <code>/druid/v2/sql/:sqlQueryId</code>

#### Responses

<Tabs>

<TabItem value="6" label="202 SUCCESS">


*Successfully deleted query*

</TabItem>
<TabItem value="7" label="403 FORBIDDEN">


*Authorization failure*

</TabItem>
<TabItem value="8" label="404 NOT FOUND">


*Invalid `sqlQueryId` or query was completed before cancellation request*

</TabItem>
</Tabs>

---

#### Sample request

The following example cancels a request with the set query ID `request01`.

<Tabs>

<TabItem value="9" label="cURL">


```shell
curl --request DELETE "http://ROUTER_IP:ROUTER_PORT/druid/v2/sql/request01"
```

</TabItem>
<TabItem value="10" label="HTTP">


```HTTP
DELETE /druid/v2/sql/request01 HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

</TabItem>
</Tabs>

#### Sample response

A successful response results in an `HTTP 202` message code and an empty response body.

### Query output format

The following table shows examples of how Druid returns the column names and data types based on the result format and the type request.
In all cases, `header` is true.
The examples includes the first row of results, where the value of `user` is `BlueMoon2662`.

```
| Format | typesHeader | sqlTypesHeader | Example output                                                                             |
|--------|-------------|----------------|--------------------------------------------------------------------------------------------|
| object | true        | false          | [ { "user" : { "type" : "STRING" } }, { "user" : "BlueMoon2662" } ]                        |
| object | true        | true           | [ { "user" : { "type" : "STRING", "sqlType" : "VARCHAR" } }, { "user" : "BlueMoon2662" } ] |
| object | false       | true           | [ { "user" : { "sqlType" : "VARCHAR" } }, { "user" : "BlueMoon2662" } ]                    |
| object | false       | false          | [ { "user" : null }, { "user" : "BlueMoon2662" } ]                                         |
| array  | true        | false          | [ [ "user" ], [ "STRING" ], [ "BlueMoon2662" ] ]                                           |
| array  | true        | true           | [ [ "user" ], [ "STRING" ], [ "VARCHAR" ], [ "BlueMoon2662" ] ]                            |
| array  | false       | true           | [ [ "user" ], [ "VARCHAR" ], [ "BlueMoon2662" ] ]                                          |
| array  | false       | false          | [ [ "user" ], [ "BlueMoon2662" ] ]                                                         |
| csv    | true        | false          | user STRING BlueMoon2662                                                                   |
| csv    | true        | true           | user STRING VARCHAR BlueMoon2662                                                           |
| csv    | false       | true           | user VARCHAR BlueMoon2662                                                                  |
| csv    | false       | false          | user BlueMoon2662                                                                          |
```

## Query from deep storage

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

- Apart from the context parameters mentioned [here](../multi-stage-query/reference.md#context-parameters) there are additional context parameters for `sql/statements` specifically:

   - `executionMode`  determines how query results are fetched. Druid currently only supports `ASYNC`. You must manually retrieve your results after the query completes.
   - `selectDestination` determines where final results get written. By default, results are written to task reports. Set this parameter to `durableStorage` to instruct Druid to write the results from SELECT queries to durable storage, which allows you to fetch larger result sets. For result sets with more than 3000 rows, it is highly recommended to use `durableStorage`. Note that this requires you to have [durable storage for MSQ](../operations/durable-storage.md) enabled.

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

Getting the query results for an ingestion query returns an empty response.

#### URL

<code class="getAPI">GET</code> <code>/druid/v2/sql/statements/:queryId/results</code>

#### Query parameters
* `page` (optional)
    * Type: Int
    * Fetch results based on page numbers. If not specified, all results are returned sequentially starting from page 0 to N in the same response.
* `resultFormat` (optional)
    * Type: String
    * Defines the format in which the results are presented. The following options are supported `arrayLines`,`objectLines`,`array`,`object`, and `csv`. The default is `object`.

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

A successful request returns an HTTP `202 ACCEPTED` message code and an empty response body.
