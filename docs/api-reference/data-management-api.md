---
id: data-management-api
title: Data management API
sidebar_label: Data management
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

This topic describes the data management API endpoints for Apache Druid. This includes information on how to mark segments as `used` or `unused` and delete them from Druid.

Note that while segments may be enabled (set to `used`) by issuing POST requests for the datasources, the Coordinator may again disable segments (set to `unused`) if they match any configured [drop rules](../operations/rule-configuration.md#drop-rules). Even if segments are enabled by these APIs, you must configure a [load rule](../operations/rule-configuration.md#load-rules) to load them onto Historical processes. If an indexing or kill task runs at the same time these APIs are invoked, the behavior is undefined. Some segments might be killed and others might be enabled. It's also possible that all segments might be disabled, but the indexing task can still read data from those segments and succeed.

In this topic, `http://ROUTER_IP:ROUTER_PORT` is a placeholder for your Router service address and port. Replace it with the information for your deployment. For example, use `http://localhost:8888` for quickstart deployments.

> Avoid using indexing or kill tasks and these APIs at the same time for the same datasource and time chunk.

### Update all datasource segments as `used`

Updates the state of all `unused` segments of a datasource to `used`. If there are no segments eligible to be marked as `used`, this endpoint will return the property `"numChangedSegments"` and the value `0`. 

Note that this endpoint returns an HTTP `200 OK` message code even if the datasource name does not exist.

#### URL

<code class="postAPI">POST</code> <code>/druid/coordinator/v1/datasources/:dataSource</code>

#### Header 

The following header is required to make a call to this endpoint.

```json
Content-Type: application/json
Accept: application/json, text/plain
```
#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->

*Successfully updated segments* 

<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

The following example updates all `unused` segments of `wikipedia_hour` to `used`. `wikipedia_hour` contains one `unused` segment eligible to be marked as `used`.

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->

```shell
curl --request POST 'http://ROUTER_IP:ROUTER_PORT/druid/coordinator/v1/datasources/wikipedia_hour' \
--header 'Content-Type: application/json' \
--header 'Accept: application/json, text/plain'
```

<!--HTTP-->

```HTTP
POST /druid/coordinator/v1/datasources/wikipedia_hour HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
Content-Type: application/json
Accept: application/json, text/plain
```

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response

<details>
  <summary>Click to show sample response</summary>

```json
{
    "numChangedSegments": 1
}
```
</details>

### Update a segment as `used`

Updates the state a segment as `used` using the segment's ID. 

#### URL

<code class="postAPI">POST</code> <code>/druid/coordinator/v1/datasources/segments/:segmentId</code>

#### Header 

The following header is required to make a call to this endpoint.

```json
Content-Type: application/json
Accept: application/json, text/plain
```

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->

*Successfully updated segments* 

<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

The following example updates the segment with ID `wikipedia_hour_2015-09-12T18:00:00.000Z_2015-09-12T19:00:00.000Z_2023-08-10T04:12:03.860Z` to `used`.

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->

```shell
curl --request POST 'http://ROUTER_IP:ROUTER_PORT/druid/coordinator/v1/datasources/wikipedia_hour/segments/wikipedia_hour_2015-09-12T18:00:00.000Z_2015-09-12T19:00:00.000Z_2023-08-10T04:12:03.860Z' \
--header 'Content-Type: application/json' \
--header 'Accept: application/json, text/plain'
```

<!--HTTP-->

```HTTP
POST /druid/coordinator/v1/datasources/wikipedia_hour/segments/wikipedia_hour_2015-09-12T18:00:00.000Z_2015-09-12T19:00:00.000Z_2023-08-10T04:12:03.860Z HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
Content-Type: application/json
Accept: application/json, text/plain
```

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response

<details>
  <summary>Click to show sample response</summary>

```json
{
    "segmentStateChanged": true
}
```
</details>

### Update a group of segments as `used`

Updates the state a group of segments as `used` using the an array of segment IDs or an interval. Pass the array of segment IDs or interval as a JSON object in the request body.

Interval specifies the start and end times as ISO 8601 strings. `interval=(start/end)` where start is inclusive and end is non-inclusive. Only the segments completely contained within the specified interval will be updated, partially overlapping segments will not be affected.

#### URL 

<code class="postAPI">POST</code> <code>/druid/coordinator/v1/datasources/:dataSource/markUsed</code>

#### Request body

The group of segments is sent as a JSON request payload with the following properties:

|Property|Type|Description|Example|
|----------|---|-------------|---------|
|`interval`|ISO-8601| The interval of segments.|`"2015-09-12T03:00:00.000Z/2015-09-12T05:00:00.000Z"`|
|`segmentIds`|String|Array of segment IDs.|`["segmentId1", "segmentId2"]`|

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->

*Successfully updated segments* 

<!--204 NO CONTENT-->

*Invalid dataSource name*

<!--400 BAD REQUEST-->

*Invalid request payload*

<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

The following example updates two segments with IDs `wikipedia_hour_2015-09-12T14:00:00.000Z_2015-09-12T15:00:00.000Z_2023-08-10T04:12:03.860Z` and `wikipedia_hour_2015-09-12T04:00:00.000Z_2015-09-12T05:00:00.000Z_2023-08-10T04:12:03.860Z` as `used`.

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->

```shell
curl 'http://ROUTER_IP:ROUTER_PORT/druid/coordinator/v1/datasources/wikipedia_hour/markUsed' \
--header 'Content-Type: application/json' \
--data '{
    "segmentIds": [
        "wikipedia_hour_2015-09-12T14:00:00.000Z_2015-09-12T15:00:00.000Z_2023-08-10T04:12:03.860Z",
        "wikipedia_hour_2015-09-12T04:00:00.000Z_2015-09-12T05:00:00.000Z_2023-08-10T04:12:03.860Z"
    ]
}'
```

<!--HTTP-->

```HTTP
POST /druid/coordinator/v1/datasources/wikipedia_hour/markUsed HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
Content-Type: application/json
Content-Length: 230

{
    "segmentIds": [
        "wikipedia_hour_2015-09-12T14:00:00.000Z_2015-09-12T15:00:00.000Z_2023-08-10T04:12:03.860Z",
        "wikipedia_hour_2015-09-12T04:00:00.000Z_2015-09-12T05:00:00.000Z_2023-08-10T04:12:03.860Z"
    ]
}
```

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response

<details>
  <summary>Click to show sample response</summary>

```json
{
    "numChangedSegments": 2
}
```
</details>

### Update a group of segments as `unused`

Updates the state a group of segments as `unused` using the an array of segment IDs or an interval. Pass the array of segment IDs or interval as a JSON object in the request body.

Interval specifies the start and end times as IS0 8601 strings. `interval=(start/end)` where start is inclusive and end is non-inclusive. Only the segments completely contained within the specified interval will be updated, partially overlapping segments will not be affected.

#### URL 

<code class="postAPI">POST</code> <code>/druid/coordinator/v1/datasources/:dataSource/markUnused</code>

#### Request body

The group of segments is sent as a JSON request payload with the following properties:

|Property|Description|Example|
|----------|-------------|---------|
|`interval`|The interval of segments.|`"2015-09-12T03:00:00.000Z/2015-09-12T05:00:00.000Z"`|
|`segmentIds`|Array of segment IDs.|`["segmentId1", "segmentId2"]`|

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->

*Successfully updated segments* 

<!--204 NO CONTENT-->

*Invalid dataSource name*

<!--400 BAD REQUEST-->

*Invalid request payload*

<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

The following example updates two segments with IDs `wikipedia_hour_2015-09-12T14:00:00.000Z_2015-09-12T15:00:00.000Z_2023-08-10T04:12:03.860Z` and `wikipedia_hour_2015-09-12T04:00:00.000Z_2015-09-12T05:00:00.000Z_2023-08-10T04:12:03.860Z` as `unused`.

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->

```shell
curl 'http://ROUTER_IP:ROUTER_PORT/druid/coordinator/v1/datasources/wikipedia_hour/markUnused' \
--header 'Content-Type: application/json' \
--data '{
    "segmentIds": [
        "wikipedia_hour_2015-09-12T14:00:00.000Z_2015-09-12T15:00:00.000Z_2023-08-10T04:12:03.860Z",
        "wikipedia_hour_2015-09-12T04:00:00.000Z_2015-09-12T05:00:00.000Z_2023-08-10T04:12:03.860Z"
    ]
}'
```

<!--HTTP-->

```HTTP
POST /druid/coordinator/v1/datasources/wikipedia_hour/markUnused HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
Content-Type: application/json
Content-Length: 230

{
    "segmentIds": [
        "wikipedia_hour_2015-09-12T14:00:00.000Z_2015-09-12T15:00:00.000Z_2023-08-10T04:12:03.860Z",
        "wikipedia_hour_2015-09-12T04:00:00.000Z_2015-09-12T05:00:00.000Z_2023-08-10T04:12:03.860Z"
    ]
}
```

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response

<details>
  <summary>Click to show sample response</summary>

```json
{
    "numChangedSegments": 2
}
```
</details>

### Mark all datasource segments as `unused`

Updates the state of all segments of a datasource to `unused`. This is a "soft delete" of the segments from Historicals.

Note that this endpoint returns an HTTP `200 OK` message code even if the datasource name does not exist.

#### URL 

<code class="deleteAPI">DELETE</code> <code>/druid/coordinator/v1/datasources/:dataSource</code>

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->

*Successfully updated segments* 

<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->

```shell
curl --request DELETE 'http://ROUTER_IP:ROUTER_PORT/druid/coordinator/v1/datasources/wikipedia_hour'
```

<!--HTTP-->

```HTTP
DELETE /druid/coordinator/v1/datasources/wikipedia_hour HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response

<details>
  <summary>Click to show sample response</summary>

```json
{
    "numChangedSegments": 24
}
```
</details>

### Update a segment as `unused`

Updates the state a segment as `unused` using the segment's ID. This is a "soft delete" of the segment from Historicals. To undo this delete, mark the segments as `used`.

Note that this endpoint returns an HTTP `200 OK` message code even if the segment ID or datasource name does not exist.

#### URL

<code class="deleteAPI">DELETE</code> <code>/druid/coordinator/v1/datasources/:dataSource/segments/:segmentId</code>

#### Header 

The following header is required to make a call to this endpoint.

```json
Content-Type: application/json
Accept: application/json, text/plain
```

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->

*Successfully updated segment* 

<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

The following example updates the segment `wikipedia_hour_2015-09-12T16:00:00.000Z_2015-09-12T17:00:00.000Z_2023-08-10T04:12:03.860Z` from datasource `wikipedia_hour` as `unused`.

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->

```shell
curl --request DELETE 'http://ROUTER_IP:ROUTER_PORT/druid/coordinator/v1/datasources/wikipedia_hour/segments/wikipedia_hour_2015-09-12T16:00:00.000Z_2015-09-12T17:00:00.000Z_2023-08-10T04:12:03.860Z' \
--header 'Content-Type: application/json' \
--header 'Accept: application/json, text/plain'
```

<!--HTTP-->

```HTTP
DELETE /druid/coordinator/v1/datasources/wikipedia_hour/segments/wikipedia_hour_2015-09-12T16:00:00.000Z_2015-09-12T17:00:00.000Z_2023-08-10T04:12:03.860Z HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
Content-Type: application/json
Accept: application/json, text/plain
```

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response

<details>
  <summary>Click to show sample response</summary>

```json
{
    "segmentStateChanged": true
}
```
</details>


### Permanently delete segments with a kill task

Runs a [Kill task](../ingestion/tasks.md) for a given interval and datasource. The interval value is an ISO 8601 string delimited by `_`. This endpoint permanently deletes all metadata about `unused` segments and removes them from deep storage.

Note that this endpoint returns an HTTP `200 OK` message code even if the datasource name does not exist.

This succeeds the depreciated endpoint: `DELETE /druid/coordinator/v1/datasources/:dataSource?kill=true&interval=:interval`

#### URL

<code class="deleteAPI">DELETE</code> <code>/druid/coordinator/v1/datasources/:dataSource/intervals/:interval</code>

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->

*Successfully sent kill task* 

<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

The following example sends a kill task to permanently delete segments in the datasource `wikipedia_hour` from the interval `2015-09-12` to `2015-09-13`.

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->

```shell
curl --request DELETE 'http://ROUTER_IP:ROUTER_PORT/druid/coordinator/v1/datasources/wikipedia_hour/intervals/2015-09-12_2015-09-13'
```

<!--HTTP-->

```HTTP
DELETE /druid/coordinator/v1/datasources/wikipedia_hour/intervals/2015-09-12_2015-09-13 HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response

A successful request returns an HTTP `200 OK` and an empty response body.