---
id: retention-rules-api
title: Retention rules API
sidebar_label: Retention rules
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

This document describes the API endpoints for managing retention rules in Apache Druid.

Druid uses retention rules to determine what data should be retained in the cluster. Druid supports load, drop, and broadcast rules. See [using rules to drop and retain data](../operations/rule-configuration.md) for more information. 

In this document, `http://SERVICE_IP:SERVICE_PORT` is a placeholder for the server address of deployment and the service port. For example, on the quickstart configuration, replace `http://ROUTER_IP:ROUTER_PORT` with `http://localhost:8888`.

## Update retention rules for a datasource

Update one or more retention rules for a datasource. Retention rules can be submitted as an array of rule objects in the request body and overwrites any existing rules for the datasource. Rules are read in the order in which they appear, see [rule structure](../operations/rule-configuration.md) for more information.

Note that this endpoint returns an `HTTP 200 Success` code message even if the `datasource` does not exist.

### URL
<code class="postAPI">POST</code> `/druid/coordinator/v1/rules/{dataSource}`

### Header parameters

The endpoint supports a set of optional header parameters to populate the `author` and `comment` fields in the `auditInfo` property for audit history. 

* `X-Druid-Author`(optional)
  * Type: String
  * A string representing the author making the configuration change.
* `X-Druid-Comment` (optional)
  * Type: String
  * A string describing the update.

### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully updated retention rules for specified datasource* 

<!--END_DOCUSAURUS_CODE_TABS-->

---

### Sample request

The following example updates the retention rules for a datasource with specified ID `kttm1`.

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->
```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/coordinator/v1/rules/kttm1" \
--header "X-Druid-Author: doc intern" \
--header "X-Druid-Comment: submitted via api" \
--header "Content-Type: application/json" \
--data "[
  {
    \"type\": \"broadcastForever\"
  },
  {
    \"type\": \"loadForever\",
    \"tieredReplicants\": {
      \"_default_tier\": 2
    }
  },
  {
    \"type\": \"dropByPeriod\",
    \"period\": \"P1M\"
  }
]"
```
<!--HTTP-->
```HTTP
POST /druid/coordinator/v1/rules/kttm1 HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
X-Druid-Author: doc intern
X-Druid-Comment: submitted via api
Content-Type: application/json
Content-Length: 192

[
  {
    "type": "broadcastForever"
  },
  {
    "type": "loadForever",
    "tieredReplicants": {
      "_default_tier": 2
    }
  },
  {
    "type": "dropByPeriod",
    "period": "P1M"
  }
]
```
<!--END_DOCUSAURUS_CODE_TABS-->

### Sample response

A successful request returns an HTTP `200 OK` and empty response body.

## Update default retention rules for all datasources

Update one or more default retention rules for all datasources. Retention rules can be submitted as an array of rule objects in the request body and overwrites any existing rules for the datasource. To remove default retention rules for all datasources, submit an empty rule array in the request body. Rules are read in the order in which they appear, see [rule structure](../operations/rule-configuration.md) for more information.

### URL

<code class="postAPI">POST</code> `/druid/coordinator/v1/rules/_default`

### Header parameters

The endpoint supports a set of optional header parameters to populate the `author` and `comment` fields in the `auditInfo` property for audit history.  

* `X-Druid-Author` (optional)
  * Type: String
  * A string representing the author making the configuration change.
* `X-Druid-Comment` (optional)
  * Type: String
  * A string describing the update.

### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully updated default retention rules* 
<!--500 SERVER ERROR-->
<br/>
*Error with request body* 

<!--END_DOCUSAURUS_CODE_TABS-->

---

### Sample request
The following example updates the default retention rule for all datasources with two new rules, `dropByPeriod` and `broadcastByPeriod`.

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->
```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/coordinator/v1/rules/_default" \
--header "Content-Type: application/json" \
--data "[
    {
        \"type\": \"dropByPeriod\",
        \"period\": \"P1M\",
        \"includeFuture\": true
    },
    {
        \"type\": \"broadcastByPeriod\",
        \"period\": \"P1M\",
        \"includeFuture\": true
    }
]"
```
<!--HTTP-->
```HTTP
POST /druid/coordinator/v1/rules/_default HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
Content-Type: application/json
Content-Length: 207

[
    {
        "type": "dropByPeriod",
        "period": "P1M",
        "includeFuture": true
    },
    {
        "type": "broadcastByPeriod",
        "period": "P1M",
        "includeFuture": true
    }
]
```
<!--END_DOCUSAURUS_CODE_TABS-->

### Sample response

A successful request returns an HTTP `200 OK` and empty response body.


## Get an array of all retention rules

Retrieves all current retention rules in the cluster including the default retention rule. The response object is an array of objects for each datasource and their associated retention rule.

### URL

<code class="getAPI">GET</code> `/druid/coordinator/v1/rules`

### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully retrieved retention rules* 

<!--END_DOCUSAURUS_CODE_TABS-->

---

### Sample request

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->
```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/coordinator/v1/rules"
```
<!--HTTP-->
```HTTP
GET /druid/coordinator/v1/rules HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```
<!--END_DOCUSAURUS_CODE_TABS-->

### Sample response

<details>
  <summary>Click to show sample response</summary>

  ```json
{
    "_default": [
        {
            "tieredReplicants": {
                "_default_tier": 2
            },
            "type": "loadForever"
        }
    ],
    "social_media": [
        {
            "interval": "2023-01-01T00:00:00.000Z/2023-02-01T00:00:00.000Z",
            "type": "dropByInterval"
        }
    ],
    "wikipedia_api": [],
}
  ```
</details>

## Get an array of retention rules for a datasource

Retrieves an array of rule objects for a single datasource. If there are no retention rules, it returns an empty array. 

Note that this endpoint returns an `HTTP 200 Success` code message even if the `datasource` does not exist.

### URL

<code class="getAPI">GET</code> `/druid/coordinator/v1/rules/{dataSource}`

### Query parameters
* `full` (optional)
  * Include the default retention rule for the datasource in the response.

### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully retrieved retention rules* 

<!--END_DOCUSAURUS_CODE_TABS-->

---

### Sample request

The following example retrieves the custom retention rules and default retention rules for datasource with specified ID `social_media`.

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->
```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/coordinator/v1/rules/social_media?full=null"
```
<!--HTTP-->
```HTTP
GET /druid/coordinator/v1/rules/social_media?full=null HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```
<!--END_DOCUSAURUS_CODE_TABS-->

### Sample response

<details>
  <summary>Click to show sample response</summary>

  ```json
[
    {
        "interval": "2020-01-01T00:00:00.000Z/2022-02-01T00:00:00.000Z",
        "type": "dropByInterval"
    },
    {
        "interval": "2010-01-01T00:00:00.000Z/2020-01-01T00:00:00.000Z",
        "tieredReplicants": {
            "_default_tier": 2
        },
        "type": "loadByInterval"
    },
    {
        "tieredReplicants": {
            "_default_tier": 2
        },
        "type": "loadForever"
    }
]
  ```
</details>

## Get audit history for all datasources

Retrieves the audit history of rules for all datasources. 

### URL

<code class="getAPI">GET</code> `/druid/coordinator/v1/rules/history`

### Query parameters

Note that the following query parameters cannot be chained.

* `inverval` (optional)
  * Type: ISO 8601 delimited with `/`. For example, `2023-07-13/2023-07-19`.
  * Limit the number of results to the specified time interval. Default value of interval can be specified by setting `druid.audit.manager.auditHistoryMillis` (1 week if not configured) in Coordinator `runtime.properties`.
* `count` (optional)
  * Type: Int
  * Limit the number of results to the last `n` entries.

### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully retrieved audit history* 
<!--400 BAD REQUEST-->
<br/>
*Request in incorrect format* 
<!--404 NOT FOUND-->
<br/>
*`count` query parameter too large* 

<!--END_DOCUSAURUS_CODE_TABS-->

---

### Sample request
The following example retrieves the audit history for all datasources from `2023-07-13` to `2023-07-19`.

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->
```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/coordinator/v1/rules/history?interval=2023-07-13%2F2023-07-19"
```
<!--HTTP-->
```HTTP
GET /druid/coordinator/v1/rules/history?interval=2023-07-13/2023-07-19 HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```
<!--END_DOCUSAURUS_CODE_TABS-->

### Sample response

<details>
  <summary>Click to show sample response</summary>

  ```json
[
    {
        "key": "social_media",
        "type": "rules",
        "auditInfo": {
            "author": "console",
            "comment": "test",
            "ip": "127.0.0.1"
        },
        "payload": "[{\"interval\":\"2023-01-01T00:00:00.000Z/2023-02-01T00:00:00.000Z\",\"type\":\"dropByInterval\"}]",
        "auditTime": "2023-07-13T18:05:33.066Z"
    },
    {
        "key": "social_media",
        "type": "rules",
        "auditInfo": {
            "author": "console",
            "comment": "test",
            "ip": "127.0.0.1"
        },
        "payload": "[]",
        "auditTime": "2023-07-18T18:10:21.203Z"
    },
    {
        "key": "wikipedia_api",
        "type": "rules",
        "auditInfo": {
            "author": "console",
            "comment": "test",
            "ip": "127.0.0.1"
        },
        "payload": "[{\"tieredReplicants\":{\"_default_tier\":2},\"type\":\"loadForever\"}]",
        "auditTime": "2023-07-18T18:10:44.519Z"
    },
    {
        "key": "wikipedia_api",
        "type": "rules",
        "auditInfo": {
            "author": "console",
            "comment": "test",
            "ip": "127.0.0.1"
        },
        "payload": "[]",
        "auditTime": "2023-07-18T18:11:02.110Z"
    },
    {
        "key": "social_media",
        "type": "rules",
        "auditInfo": {
            "author": "console",
            "comment": "test",
            "ip": "127.0.0.1"
        },
        "payload": "[{\"interval\":\"2023-07-03T18:49:54.848Z/2023-07-03T18:49:55.861Z\",\"type\":\"dropByInterval\"}]",
        "auditTime": "2023-07-18T18:32:50.060Z"
    },
    {
        "key": "social_media",
        "type": "rules",
        "auditInfo": {
            "author": "console",
            "comment": "test",
            "ip": "127.0.0.1"
        },
        "payload": "[{\"interval\":\"2020-01-01T00:00:00.000Z/2022-02-01T00:00:00.000Z\",\"type\":\"dropByInterval\"}]",
        "auditTime": "2023-07-18T18:34:09.657Z"
    },
    {
        "key": "social_media",
        "type": "rules",
        "auditInfo": {
            "author": "console",
            "comment": "test",
            "ip": "127.0.0.1"
        },
        "payload": "[{\"interval\":\"2020-01-01T00:00:00.000Z/2022-02-01T00:00:00.000Z\",\"type\":\"dropByInterval\"},{\"tieredReplicants\":{\"_default_tier\":2},\"type\":\"loadForever\"}]",
        "auditTime": "2023-07-18T18:38:37.223Z"
    },
    {
        "key": "social_media",
        "type": "rules",
        "auditInfo": {
            "author": "console",
            "comment": "test",
            "ip": "127.0.0.1"
        },
        "payload": "[{\"interval\":\"2020-01-01T00:00:00.000Z/2022-02-01T00:00:00.000Z\",\"type\":\"dropByInterval\"},{\"interval\":\"2010-01-01T00:00:00.000Z/2020-01-01T00:00:00.000Z\",\"tieredReplicants\":{\"_default_tier\":2},\"type\":\"loadByInterval\"}]",
        "auditTime": "2023-07-18T18:49:43.964Z"
    }
]
  ```
</details>