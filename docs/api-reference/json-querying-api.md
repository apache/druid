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

This topic describes the API endpoints to submit JSON-based [native queries](../querying/querying.md) to Apache Druid.

In this topic, `http://SERVICE_IP:SERVICE_PORT` is a placeholder for the server address of deployment and the service port. For example, on the quickstart configuration, replace `http://ROUTER_IP:ROUTER_PORT` with `http://localhost:8888`.


## Submit a query

Submits a JSON-based native query. The body of the request is the native query itself.

Druid supports different types of queries for different use cases. All queries require the following properties:
* `queryType`: A string representing the type of query. Druid supports the following native query types: `timeseries`, `topN`, `groupBy`, `timeBoundaries`, `segmentMetadata`, `datasourceMetadata`, `scan`, and `search`. 
* `dataSource`: A string or object defining the source of data to query. The most common value is the name of the datasource to query. For more information, see [Datasources](../querying/datasource.md).

For additional properties based on your query type or use case, see [available native queries](../querying/querying.md#available-queries).

### URL

<code class="postAPI">POST</code> <code>/druid/v2/</code>

### Query parameters

* `pretty` (optional)
  * Druid returns the response in a pretty-printed format using indentation and line breaks.

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
For more information on possible error messages, see [query execution failures](../querying/querying.md#query-execution-failures).

<!--END_DOCUSAURUS_CODE_TABS-->

---

### Example query: `topN`

The following example shows a `topN` query. The query analyzes the `social_media` datasource to return the top five users from the `username` dimension with the highest number of views from the `views` metric. 

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->

```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/v2?pretty=null" \
--header 'Content-Type: application/json' \
--data '{
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
}'
```
<!--HTTP-->

```HTTP
POST /druid/v2?pretty=null HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
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

#### Example response: `topN`

<details>
  <summary>Click to show sample response</summary>

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
</details>

### Example query: `groupBy`

The following example submits a JSON query of the `groupBy` type to retrieve the `username` with the highest votes to posts ratio from the `social_media` datasource.

In this query:
* The `upvoteSum` aggregation calculates the sum of the `upvotes` for each user.
* The `postCount` aggregation calculates the sum of posts for each user.
* The `upvoteToPostRatio` is a post-aggregation of the `upvoteSum` and the `postCount`, divided to calculate the ratio.
* The result is sorted based on the `upvoteToPostRatio` in descending order.

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->

```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/v2" \
--header 'Content-Type: application/json' \
--data '{
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
}'
```

<!--HTTP-->

```HTTP
POST /druid/v2?pretty=null HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
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

#### Example response: `groupBy`

<details>
  <summary>Click to show sample response</summary>
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

## Get segment information for query

Retrieves an array that contains objects with segment information, including the server locations associated with the query provided in the request body. 

### URL

<code class="postAPI">POST</code> <code>/druid/v2/candidates/</code>

### Query parameters
* `pretty` (optional)
  *  Druid returns the response in a pretty-printed format using indentation and line breaks.

### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->

*Successfully retrieved segment information* 

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
For more information on possible error messages, see [query execution failures](../querying/querying.md#query-execution-failures).

<!--END_DOCUSAURUS_CODE_TABS-->

---

### Sample request

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->

```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/v2/candidates" \
--header 'Content-Type: application/json' \
--data '{
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
}'
```

<!--HTTP-->

```HTTP
POST /druid/v2/candidates HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
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
    "2020-01-01T00:00:00.000/2024-01-01T00:00:00.000"
  ]
}
```

<!--END_DOCUSAURUS_CODE_TABS-->

### Sample response

<details>
  <summary>Click to show sample response</summary>

  ```json
[
    {
        "interval": "2023-07-03T18:00:00.000Z/2023-07-03T19:00:00.000Z",
        "version": "2023-07-03T18:51:18.905Z",
        "partitionNumber": 0,
        "size": 21563693,
        "locations": [
            {
                "name": "localhost:8083",
                "host": "localhost:8083",
                "hostAndTlsPort": null,
                "maxSize": 300000000000,
                "type": "historical",
                "tier": "_default_tier",
                "priority": 0
            }
        ]
    },
    {
        "interval": "2023-07-03T19:00:00.000Z/2023-07-03T20:00:00.000Z",
        "version": "2023-07-03T19:00:00.657Z",
        "partitionNumber": 0,
        "size": 6057236,
        "locations": [
            {
                "name": "localhost:8083",
                "host": "localhost:8083",
                "hostAndTlsPort": null,
                "maxSize": 300000000000,
                "type": "historical",
                "tier": "_default_tier",
                "priority": 0
            }
        ]
    },
    {
        "interval": "2023-07-05T21:00:00.000Z/2023-07-05T22:00:00.000Z",
        "version": "2023-07-05T21:09:58.102Z",
        "partitionNumber": 0,
        "size": 223926186,
        "locations": [
            {
                "name": "localhost:8083",
                "host": "localhost:8083",
                "hostAndTlsPort": null,
                "maxSize": 300000000000,
                "type": "historical",
                "tier": "_default_tier",
                "priority": 0
            }
        ]
    },
    {
        "interval": "2023-07-05T21:00:00.000Z/2023-07-05T22:00:00.000Z",
        "version": "2023-07-05T21:09:58.102Z",
        "partitionNumber": 1,
        "size": 20244827,
        "locations": [
            {
                "name": "localhost:8083",
                "host": "localhost:8083",
                "hostAndTlsPort": null,
                "maxSize": 300000000000,
                "type": "historical",
                "tier": "_default_tier",
                "priority": 0
            }
        ]
    },
    {
        "interval": "2023-07-05T22:00:00.000Z/2023-07-05T23:00:00.000Z",
        "version": "2023-07-05T22:00:00.524Z",
        "partitionNumber": 0,
        "size": 104628051,
        "locations": [
            {
                "name": "localhost:8083",
                "host": "localhost:8083",
                "hostAndTlsPort": null,
                "maxSize": 300000000000,
                "type": "historical",
                "tier": "_default_tier",
                "priority": 0
            }
        ]
    },
    {
        "interval": "2023-07-05T22:00:00.000Z/2023-07-05T23:00:00.000Z",
        "version": "2023-07-05T22:00:00.524Z",
        "partitionNumber": 1,
        "size": 1603995,
        "locations": [
            {
                "name": "localhost:8083",
                "host": "localhost:8083",
                "hostAndTlsPort": null,
                "maxSize": 300000000000,
                "type": "historical",
                "tier": "_default_tier",
                "priority": 0
            }
        ]
    },
    {
        "interval": "2023-07-05T23:00:00.000Z/2023-07-06T00:00:00.000Z",
        "version": "2023-07-05T23:21:55.242Z",
        "partitionNumber": 0,
        "size": 181506843,
        "locations": [
            {
                "name": "localhost:8083",
                "host": "localhost:8083",
                "hostAndTlsPort": null,
                "maxSize": 300000000000,
                "type": "historical",
                "tier": "_default_tier",
                "priority": 0
            }
        ]
    },
    {
        "interval": "2023-07-06T00:00:00.000Z/2023-07-06T01:00:00.000Z",
        "version": "2023-07-06T00:02:08.498Z",
        "partitionNumber": 0,
        "size": 9170974,
        "locations": [
            {
                "name": "localhost:8083",
                "host": "localhost:8083",
                "hostAndTlsPort": null,
                "maxSize": 300000000000,
                "type": "historical",
                "tier": "_default_tier",
                "priority": 0
            }
        ]
    },
    {
        "interval": "2023-07-06T00:00:00.000Z/2023-07-06T01:00:00.000Z",
        "version": "2023-07-06T00:02:08.498Z",
        "partitionNumber": 1,
        "size": 23969632,
        "locations": [
            {
                "name": "localhost:8083",
                "host": "localhost:8083",
                "hostAndTlsPort": null,
                "maxSize": 300000000000,
                "type": "historical",
                "tier": "_default_tier",
                "priority": 0
            }
        ]
    },
    {
        "interval": "2023-07-06T01:00:00.000Z/2023-07-06T02:00:00.000Z",
        "version": "2023-07-06T01:13:53.982Z",
        "partitionNumber": 0,
        "size": 599895,
        "locations": [
            {
                "name": "localhost:8083",
                "host": "localhost:8083",
                "hostAndTlsPort": null,
                "maxSize": 300000000000,
                "type": "historical",
                "tier": "_default_tier",
                "priority": 0
            }
        ]
    },
    {
        "interval": "2023-07-06T01:00:00.000Z/2023-07-06T02:00:00.000Z",
        "version": "2023-07-06T01:13:53.982Z",
        "partitionNumber": 1,
        "size": 1627041,
        "locations": [
            {
                "name": "localhost:8083",
                "host": "localhost:8083",
                "hostAndTlsPort": null,
                "maxSize": 300000000000,
                "type": "historical",
                "tier": "_default_tier",
                "priority": 0
            }
        ]
    },
    {
        "interval": "2023-07-06T02:00:00.000Z/2023-07-06T03:00:00.000Z",
        "version": "2023-07-06T02:55:50.701Z",
        "partitionNumber": 0,
        "size": 629753,
        "locations": [
            {
                "name": "localhost:8083",
                "host": "localhost:8083",
                "hostAndTlsPort": null,
                "maxSize": 300000000000,
                "type": "historical",
                "tier": "_default_tier",
                "priority": 0
            }
        ]
    },
    {
        "interval": "2023-07-06T02:00:00.000Z/2023-07-06T03:00:00.000Z",
        "version": "2023-07-06T02:55:50.701Z",
        "partitionNumber": 1,
        "size": 1342360,
        "locations": [
            {
                "name": "localhost:8083",
                "host": "localhost:8083",
                "hostAndTlsPort": null,
                "maxSize": 300000000000,
                "type": "historical",
                "tier": "_default_tier",
                "priority": 0
            }
        ]
    },
    {
        "interval": "2023-07-06T04:00:00.000Z/2023-07-06T05:00:00.000Z",
        "version": "2023-07-06T04:02:36.562Z",
        "partitionNumber": 0,
        "size": 2131434,
        "locations": [
            {
                "name": "localhost:8083",
                "host": "localhost:8083",
                "hostAndTlsPort": null,
                "maxSize": 300000000000,
                "type": "historical",
                "tier": "_default_tier",
                "priority": 0
            }
        ]
    },
    {
        "interval": "2023-07-06T05:00:00.000Z/2023-07-06T06:00:00.000Z",
        "version": "2023-07-06T05:23:27.856Z",
        "partitionNumber": 0,
        "size": 797161,
        "locations": [
            {
                "name": "localhost:8083",
                "host": "localhost:8083",
                "hostAndTlsPort": null,
                "maxSize": 300000000000,
                "type": "historical",
                "tier": "_default_tier",
                "priority": 0
            }
        ]
    },
    {
        "interval": "2023-07-06T05:00:00.000Z/2023-07-06T06:00:00.000Z",
        "version": "2023-07-06T05:23:27.856Z",
        "partitionNumber": 1,
        "size": 1176858,
        "locations": [
            {
                "name": "localhost:8083",
                "host": "localhost:8083",
                "hostAndTlsPort": null,
                "maxSize": 300000000000,
                "type": "historical",
                "tier": "_default_tier",
                "priority": 0
            }
        ]
    },
    {
        "interval": "2023-07-06T06:00:00.000Z/2023-07-06T07:00:00.000Z",
        "version": "2023-07-06T06:46:34.638Z",
        "partitionNumber": 0,
        "size": 2148760,
        "locations": [
            {
                "name": "localhost:8083",
                "host": "localhost:8083",
                "hostAndTlsPort": null,
                "maxSize": 300000000000,
                "type": "historical",
                "tier": "_default_tier",
                "priority": 0
            }
        ]
    },
    {
        "interval": "2023-07-06T07:00:00.000Z/2023-07-06T08:00:00.000Z",
        "version": "2023-07-06T07:38:28.050Z",
        "partitionNumber": 0,
        "size": 2040748,
        "locations": [
            {
                "name": "localhost:8083",
                "host": "localhost:8083",
                "hostAndTlsPort": null,
                "maxSize": 300000000000,
                "type": "historical",
                "tier": "_default_tier",
                "priority": 0
            }
        ]
    },
    {
        "interval": "2023-07-06T08:00:00.000Z/2023-07-06T09:00:00.000Z",
        "version": "2023-07-06T08:27:31.407Z",
        "partitionNumber": 0,
        "size": 678723,
        "locations": [
            {
                "name": "localhost:8083",
                "host": "localhost:8083",
                "hostAndTlsPort": null,
                "maxSize": 300000000000,
                "type": "historical",
                "tier": "_default_tier",
                "priority": 0
            }
        ]
    },
    {
        "interval": "2023-07-06T08:00:00.000Z/2023-07-06T09:00:00.000Z",
        "version": "2023-07-06T08:27:31.407Z",
        "partitionNumber": 1,
        "size": 1437866,
        "locations": [
            {
                "name": "localhost:8083",
                "host": "localhost:8083",
                "hostAndTlsPort": null,
                "maxSize": 300000000000,
                "type": "historical",
                "tier": "_default_tier",
                "priority": 0
            }
        ]
    },
    {
        "interval": "2023-07-06T10:00:00.000Z/2023-07-06T11:00:00.000Z",
        "version": "2023-07-06T10:02:42.079Z",
        "partitionNumber": 0,
        "size": 1671296,
        "locations": [
            {
                "name": "localhost:8083",
                "host": "localhost:8083",
                "hostAndTlsPort": null,
                "maxSize": 300000000000,
                "type": "historical",
                "tier": "_default_tier",
                "priority": 0
            }
        ]
    },
    {
        "interval": "2023-07-06T11:00:00.000Z/2023-07-06T12:00:00.000Z",
        "version": "2023-07-06T11:27:23.902Z",
        "partitionNumber": 0,
        "size": 574893,
        "locations": [
            {
                "name": "localhost:8083",
                "host": "localhost:8083",
                "hostAndTlsPort": null,
                "maxSize": 300000000000,
                "type": "historical",
                "tier": "_default_tier",
                "priority": 0
            }
        ]
    },
    {
        "interval": "2023-07-06T11:00:00.000Z/2023-07-06T12:00:00.000Z",
        "version": "2023-07-06T11:27:23.902Z",
        "partitionNumber": 1,
        "size": 1427384,
        "locations": [
            {
                "name": "localhost:8083",
                "host": "localhost:8083",
                "hostAndTlsPort": null,
                "maxSize": 300000000000,
                "type": "historical",
                "tier": "_default_tier",
                "priority": 0
            }
        ]
    },
    {
        "interval": "2023-07-06T12:00:00.000Z/2023-07-06T13:00:00.000Z",
        "version": "2023-07-06T12:52:00.846Z",
        "partitionNumber": 0,
        "size": 2115172,
        "locations": [
            {
                "name": "localhost:8083",
                "host": "localhost:8083",
                "hostAndTlsPort": null,
                "maxSize": 300000000000,
                "type": "historical",
                "tier": "_default_tier",
                "priority": 0
            }
        ]
    },
    {
        "interval": "2023-07-06T14:00:00.000Z/2023-07-06T15:00:00.000Z",
        "version": "2023-07-06T14:32:33.926Z",
        "partitionNumber": 0,
        "size": 589108,
        "locations": [
            {
                "name": "localhost:8083",
                "host": "localhost:8083",
                "hostAndTlsPort": null,
                "maxSize": 300000000000,
                "type": "historical",
                "tier": "_default_tier",
                "priority": 0
            }
        ]
    },
    {
        "interval": "2023-07-06T14:00:00.000Z/2023-07-06T15:00:00.000Z",
        "version": "2023-07-06T14:32:33.926Z",
        "partitionNumber": 1,
        "size": 1392649,
        "locations": [
            {
                "name": "localhost:8083",
                "host": "localhost:8083",
                "hostAndTlsPort": null,
                "maxSize": 300000000000,
                "type": "historical",
                "tier": "_default_tier",
                "priority": 0
            }
        ]
    },
    {
        "interval": "2023-07-06T15:00:00.000Z/2023-07-06T16:00:00.000Z",
        "version": "2023-07-06T15:53:25.467Z",
        "partitionNumber": 0,
        "size": 2037851,
        "locations": [
            {
                "name": "localhost:8083",
                "host": "localhost:8083",
                "hostAndTlsPort": null,
                "maxSize": 300000000000,
                "type": "historical",
                "tier": "_default_tier",
                "priority": 0
            }
        ]
    },
    {
        "interval": "2023-07-06T16:00:00.000Z/2023-07-06T17:00:00.000Z",
        "version": "2023-07-06T16:02:26.568Z",
        "partitionNumber": 0,
        "size": 230400650,
        "locations": [
            {
                "name": "localhost:8083",
                "host": "localhost:8083",
                "hostAndTlsPort": null,
                "maxSize": 300000000000,
                "type": "historical",
                "tier": "_default_tier",
                "priority": 0
            }
        ]
    },
    {
        "interval": "2023-07-06T16:00:00.000Z/2023-07-06T17:00:00.000Z",
        "version": "2023-07-06T16:02:26.568Z",
        "partitionNumber": 1,
        "size": 38209056,
        "locations": [
            {
                "name": "localhost:8083",
                "host": "localhost:8083",
                "hostAndTlsPort": null,
                "maxSize": 300000000000,
                "type": "historical",
                "tier": "_default_tier",
                "priority": 0
            }
        ]
    },
    {
        "interval": "2023-07-06T17:00:00.000Z/2023-07-06T18:00:00.000Z",
        "version": "2023-07-06T17:00:02.391Z",
        "partitionNumber": 0,
        "size": 211099463,
        "locations": [
            {
                "name": "localhost:8083",
                "host": "localhost:8083",
                "hostAndTlsPort": null,
                "maxSize": 300000000000,
                "type": "historical",
                "tier": "_default_tier",
                "priority": 0
            }
        ]
    }
]
  ```
</details>