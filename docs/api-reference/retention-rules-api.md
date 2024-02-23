---
id: retention-rules-api
title: Retention rules API
sidebar_label: Retention rules
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

This topic describes the API endpoints for managing retention rules in Apache Druid. You can configure retention rules in the Druid web console or API.

Druid uses retention rules to determine what data is retained in the cluster. Druid supports load, drop, and broadcast rules. For more information, see [Using rules to drop and retain data](../operations/rule-configuration.md).

In this topic, `http://ROUTER_IP:ROUTER_PORT` is a placeholder for your Router service address and port. Replace it with the information for your deployment. For example, use `http://localhost:8888` for quickstart deployments.

## Update retention rules for a datasource

Updates one or more retention rules for a datasource. The request body takes an array of retention rule objects. For details on defining retention rules, see the following sources:

* [Load rules](../operations/rule-configuration.md#load-rules)
* [Drop rules](../operations/rule-configuration.md#drop-rules)
* [Broadcast rules](../operations/rule-configuration.md#broadcast-rules)

This request overwrites any existing rules for the datasource.
Druid reads rules in the order in which they appear; for more information, see [rule structure](../operations/rule-configuration.md).

Note that this endpoint returns an HTTP `200 OK` even if the datasource does not exist.

### URL

<code class="postAPI">POST</code> <code>/druid/coordinator/v1/rules/:dataSource</code>

### Header parameters

The endpoint supports a set of optional header parameters to populate the `author` and `comment` fields in the `auditInfo` property for audit history.

* `X-Druid-Author` (optional)
  * Type: String
  * A string representing the author making the configuration change.
* `X-Druid-Comment` (optional)
  * Type: String
  * A string describing the update.

### Responses

<Tabs>

<TabItem value="1" label="200 SUCCESS">


*Successfully updated retention rules for specified datasource*

</TabItem>
</Tabs>

---

### Sample request

The following example sets a set of broadcast, load, and drop retention rules for the `kttm1` datasource.

<Tabs>

<TabItem value="2" label="cURL">


```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/coordinator/v1/rules/kttm1" \
--header 'X-Druid-Author: doc intern' \
--header 'X-Druid-Comment: submitted via api' \
--header 'Content-Type: application/json' \
--data '[
    {
        "type": "broadcastForever"
    },
    {
        "type": "loadForever",
        "tieredReplicants": {
            "_default_tier": 2
        },
        "useDefaultTierForNull": true
    },
    {
        "type": "dropByPeriod",
        "period": "P1M"
    }
]'
```

</TabItem>
<TabItem value="3" label="HTTP">


```HTTP
POST /druid/coordinator/v1/rules/kttm1 HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
X-Druid-Author: doc intern
X-Druid-Comment: submitted via api
Content-Type: application/json
Content-Length: 273

[
    {
        "type": "broadcastForever"
    },
    {
        "type": "loadForever",
        "tieredReplicants": {
            "_default_tier": 1
        },
        "useDefaultTierForNull": true
    },
    {
        "type": "dropByPeriod",
        "period": "P1M"
    }
]
```

</TabItem>
</Tabs>

### Sample response

A successful request returns an HTTP `200 OK` message code and an empty response body.

## Update default retention rules for all datasources

Updates one or more default retention rules for all datasources. Submit retention rules as an array of objects in the request body. For details on defining retention rules, see the following sources:

* [Load rules](../operations/rule-configuration.md#load-rules)
* [Drop rules](../operations/rule-configuration.md#drop-rules)
* [Broadcast rules](../operations/rule-configuration.md#broadcast-rules)

This request overwrites any existing rules for all datasources. To remove default retention rules for all datasources, submit an empty rule array in the request body. Rules are read in the order in which they appear; for more information, see [rule structure](../operations/rule-configuration.md).

### URL

<code class="postAPI">POST</code> <code>/druid/coordinator/v1/rules/_default</code>

### Header parameters

The endpoint supports a set of optional header parameters to populate the `author` and `comment` fields in the `auditInfo` property for audit history.

* `X-Druid-Author` (optional)
  * Type: String
  * A string representing the author making the configuration change.
* `X-Druid-Comment` (optional)
  * Type: String
  * A string describing the update.

### Responses

<Tabs>

<TabItem value="4" label="200 SUCCESS">


*Successfully updated default retention rules*

</TabItem>
<TabItem value="5" label="500 SERVER ERROR">


*Error with request body*

</TabItem>
</Tabs>

---

### Sample request

The following example updates the default retention rule for all datasources with a `loadByInterval` rule.

<Tabs>

<TabItem value="6" label="cURL">


```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/coordinator/v1/rules/_default" \
--header 'Content-Type: application/json' \
--data '[
    {
        "type": "loadByInterval",
        "tieredReplicants": {},
        "useDefaultTierForNull": false,
        "interval": "2010-01-01/2020-01-01"
    }
]'
```

</TabItem>
<TabItem value="7" label="HTTP">


```HTTP
POST /druid/coordinator/v1/rules/_default HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
Content-Type: application/json
Content-Length: 205

[
    {
        "type": "loadByInterval",
        "tieredReplicants": {},
        "useDefaultTierForNull": false,
        "interval": "2010-01-01/2020-01-01"
    }
]
```

</TabItem>
</Tabs>

### Sample response

A successful request returns an HTTP `200 OK` message code and an empty response body.

## Get an array of all retention rules

Retrieves all current retention rules in the cluster including the default retention rule. Returns an array of objects for each datasource and their associated retention rules.

### URL

<code class="getAPI">GET</code> <code>/druid/coordinator/v1/rules</code>

### Responses

<Tabs>

<TabItem value="8" label="200 SUCCESS">


*Successfully retrieved retention rules*

</TabItem>
</Tabs>

---

### Sample request

<Tabs>

<TabItem value="9" label="cURL">


```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/coordinator/v1/rules"
```

</TabItem>
<TabItem value="10" label="HTTP">


```HTTP
GET /druid/coordinator/v1/rules HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

</TabItem>
</Tabs>

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

Retrieves an array of rule objects for a single datasource. Returns an empty array if there are no retention rules.

Note that this endpoint returns an HTTP `200 OK` message code even if the datasource does not exist.

### URL

<code class="getAPI">GET</code> <code>/druid/coordinator/v1/rules/:dataSource</code>

### Query parameters

* `full` (optional)
  * Includes the default retention rule for the datasource in the response.

### Responses

<Tabs>

<TabItem value="11" label="200 SUCCESS">


*Successfully retrieved retention rules*

</TabItem>
</Tabs>

---

### Sample request

The following example retrieves the custom retention rules and default retention rules for datasource with the name `social_media`.

<Tabs>

<TabItem value="12" label="cURL">


```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/coordinator/v1/rules/social_media?full=null"
```

</TabItem>
<TabItem value="13" label="HTTP">


```HTTP
GET /druid/coordinator/v1/rules/social_media?full=null HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

</TabItem>
</Tabs>

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

Retrieves the audit history of rules for all datasources over an interval of time. The default interval is 1 week. You can change this period by setting `druid.audit.manager.auditHistoryMillis` in the `runtime.properties` file for the Coordinator.

### URL

<code class="getAPI">GET</code> <code>/druid/coordinator/v1/rules/history</code>

### Query parameters

Note that the following query parameters cannot be chained.

* `interval` (optional)
  * Type: ISO 8601.
  * Limits the number of results to the specified time interval. Delimit with `/`. For example, `2023-07-13/2023-07-19`.
* `count` (optional)
  * Type: Int
  * Limits the number of results to the last `n` entries.

### Responses

<Tabs>

<TabItem value="14" label="200 SUCCESS">


*Successfully retrieved audit history*

</TabItem>
<TabItem value="15" label="400 BAD REQUEST">


*Request in the incorrect format*

</TabItem>
<TabItem value="16" label="404 NOT FOUND">


*`count` query parameter too large*

</TabItem>
</Tabs>

---

### Sample request

The following example retrieves the audit history for all datasources from `2023-07-13` to `2023-07-19`.

<Tabs>

<TabItem value="17" label="cURL">


```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/coordinator/v1/rules/history?interval=2023-07-13%2F2023-07-19"
```

</TabItem>
<TabItem value="18" label="HTTP">


```HTTP
GET /druid/coordinator/v1/rules/history?interval=2023-07-13/2023-07-19 HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

</TabItem>
</Tabs>

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
