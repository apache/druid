---
id: json-querying-api
title: JSON querying API
sidebar_label: JSON querying
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

This document describes the API endpoints to submit JSON-based [native queries](../querying/querying.md) to Apache Druid.

## Queries

### Submit a query

#### URL
<code class="postAPI">POST</code> `/druid/v2/`

The endpoint for submitting queries. Accepts an option `?pretty` that pretty prints the results. 

Queries are composed of various JSON properties and Druid has different types of queries for different use cases. The possible types of queries are: `timeseries`, `topN`, `groupBy`, `timeBoundaries`, `segmentMetadata`, `datasourceMetadata`, `scan`, and `search`.

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully submitted query* 
<!--400 BAD REQUEST-->
<br/>
*Error thrown due to bad query* 
<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

The following example submits a JSON query of the `topN` type to retrieve a ranked list of users and their post views. 

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->
```shell
curl "http://<ROUTER_IP>:<ROUTER_PORT>/druid/v2?pretty=null" \
--header "Content-Type: application/json" \
--data "{
  \"queryType\": \"topN\",
  \"dataSource\": \"social_media\",
  \"dimension\": \"username\",
  \"threshold\": 5,
  \"metric\": \"views\",
  \"granularity\": \"all\",

  \"aggregations\": [
    {
      \"type\": \"longSum\",
      \"name\": \"views\",
      \"fieldName\": \"views\"
    }
  ],
  \"intervals\": [
    \"2022-01-01T00:00:00.000/2024-01-01T00:00:00.000\"
  ]
}"
```
<!--HTTP-->
```HTTP
POST /druid/v2?pretty=null HTTP/1.1
Host: http://<ROUTER_IP>:<ROUTER_PORT>
Content-Type: application/json
Content-Length: 336

{
  "queryType": "topN",
  "dataSource": "social_media",
  "dimension": "username",
  "threshold": 5,
  "metric": "views",
  "granularity": "all",

  "aggregations": [
    {
      "type": "longSum",
      "name": "views",
      "fieldName": "views"
    }
  ],
  "intervals": [
    "2022-01-01T00:00:00.000/2024-01-01T00:00:00.000"
  ]
}
```
<!--END_DOCUSAURUS_CODE_TABS-->

The following example submits a JSON query of the `groupBy` type to retrieve the `username` with the highest upvote-to-post ratio from the `social_media` datasource.

In this query:
* `upvoteSum` aggregation calculates the sum of `upvotes` for each user.
* `postCount` aggregation calculates the sum of posts for each user.
* `upvoteToPostRatio` is a post-aggregation of the `upvoteSum` and `postCount`, divided to calculate the ratio.
* The result is sorted based on `upvoteToPostRatio` and in descending order.

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->
```shell
curl "http://<ROUTER_IP>:<ROUTER_PORT>/druid/v2?pretty=null" \
--header "Content-Type: application/json" \
--data "{
  \"queryType\": \"groupBy\",
  \"dataSource\": \"social_media\",
  \"dimensions\": [\"username\"],
  \"granularity\": \"all\",
  \"aggregations\": [
    { \"type\": \"doubleSum\", \"name\": \"upvoteSum\", \"fieldName\": \"upvotes\" },
    { \"type\": \"count\", \"name\": \"postCount\", \"fieldName\": \"post_title\" }
  ],
  \"postAggregations\": [
    {
      \"type\": \"arithmetic\",
      \"name\": \"upvoteToPostRatio\",
      \"fn\": \"/\",
      \"fields\": [
        { \"type\": \"fieldAccess\", \"name\": \"upvoteSum\", \"fieldName\": \"upvoteSum\" },
        { \"type\": \"fieldAccess\", \"name\": \"postCount\", \"fieldName\": \"postCount\" }
      ]
    }
  ],
  \"intervals\": [\"2022-01-01T00:00:00.000/2024-01-01T00:00:00.000\"],
  \"limitSpec\": {
    \"type\": \"default\",
    \"limit\": 1,
    \"columns\": [
      { \"dimension\": \"upvoteToPostRatio\", \"direction\": \"descending\" }
    ]
  }
}"
```
<!--HTTP-->
```HTTP
POST /druid/v2?pretty=null HTTP/1.1
Host: http://<ROUTER_IP>:<ROUTER_PORT>
Content-Type: application/json
Content-Length: 817

{
  "queryType": "groupBy",
  "dataSource": "social_media",
  "dimensions": ["username"],
  "granularity": "all",
  "aggregations": [
    { "type": "doubleSum", "name": "upvoteSum", "fieldName": "upvotes" },
    { "type": "count", "name": "postCount", "fieldName": "post_title" }
  ],
  "postAggregations": [
    {
      "type": "arithmetic",
      "name": "upvoteToPostRatio",
      "fn": "/",
      "fields": [
        { "type": "fieldAccess", "name": "upvoteSum", "fieldName": "upvoteSum" },
        { "type": "fieldAccess", "name": "postCount", "fieldName": "postCount" }
      ]
    }
  ],
  "intervals": ["2022-01-01T00:00:00.000/2024-01-01T00:00:00.000"],
  "limitSpec": {
    "type": "default",
    "limit": 1,
    "columns": [
      { "dimension": "upvoteToPostRatio", "direction": "descending" }
    ]
  }
}
```
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response

<details>
  <summary>Click to show sample response</summary>
  The following is a sample response of the first query, retrieving a ranked list of users and post views.
  
  ```json
[
    {
        "timestamp": "2023-07-03T18:49:54.848Z",
        "result": [
            {
                "views": 11591218026,
                "username": "gus"
            },
            {
                "views": 11578638578,
                "username": "miette"
            },
            {
                "views": 11561618880,
                "username": "leon"
            },
            {
                "views": 11552609824,
                "username": "mia"
            },
            {
                "views": 11551537517,
                "username": "milton"
            }
        ]
    }
]
  ```


  The following is a sample response of the second query, retrieving the user with the highest post-to-upvotes ration.

  ```json
[
    {
        "version": "v1",
        "timestamp": "2022-01-01T00:00:00.000Z",
        "event": {
            "upvoteSum": 8.0419541E7,
            "upvoteToPostRatio": 69.53014661762697,
            "postCount": 1156614,
            "username": "miette"
        }
    }
]
  ```
</details>

### Get segment information for query

`POST /druid/v2/candidates/`

Returns segment information lists including server locations for the given query.