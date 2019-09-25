---
id: influxdb-emitter
title: "InfluxDB Emitter"
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


To use this Apache Druid (incubating) extension, make sure to [include](../../development/extensions.md#loading-extensions) `druid-influxdb-emitter` extension.

## Introduction

This extension emits druid metrics to [InfluxDB](https://www.influxdata.com/time-series-platform/influxdb/) over HTTP. Currently this emitter only emits service metric events to InfluxDB (See [Druid metrics](../../operations/metrics.md) for a list of metrics).
When a metric event is fired it is added to a queue of events. After a configurable amount of time, the events on the queue are transformed to InfluxDB's line protocol
and POSTed to the InfluxDB HTTP API. The entire queue is flushed at this point. The queue is also flushed as the emitter is shutdown.

Note that authentication and authorization must be [enabled](https://docs.influxdata.com/influxdb/v1.7/administration/authentication_and_authorization/) on the InfluxDB server.

## Configuration

All the configuration parameters for the influxdb emitter are under `druid.emitter.influxdb`.

|Property|Description|Required?|Default|
|--------|-----------|---------|-------|
|`druid.emitter.influxdb.hostname`|The hostname of the InfluxDB server.|Yes|N/A|
|`druid.emitter.influxdb.port`|The port of the InfluxDB server.|No|8086|
|`druid.emitter.influxdb.databaseName`|The name of the database in InfluxDB.|Yes|N/A|
|`druid.emitter.influxdb.maxQueueSize`|The size of the queue that holds events.|No|Integer.MAX_VALUE(=2^31-1)|
|`druid.emitter.influxdb.flushPeriod`|How often (in milliseconds) the events queue is parsed into Line Protocol and POSTed to InfluxDB.|No|60000|
|`druid.emitter.influxdb.flushDelay`|How long (in milliseconds) the scheduled method will wait until it first runs.|No|60000|
|`druid.emitter.influxdb.influxdbUserName`|The username for authenticating with the InfluxDB database.|Yes|N/A|
|`druid.emitter.influxdb.influxdbPassword`|The password of the database authorized user|Yes|N/A|
|`druid.emitter.influxdb.dimensionWhitelist`|A whitelist of metric dimensions to include as tags|No|`["dataSource","type","numMetrics","numDimensions","threshold","dimension","taskType","taskStatus","tier"]`|

## InfluxDB Line Protocol

An example of how this emitter parses a Druid metric event into InfluxDB's [line protocol](https://docs.influxdata.com/influxdb/v1.7/write_protocols/line_protocol_reference/) is given here:

The syntax of the line protocol is :

`<measurement>[,<tag_key>=<tag_value>[,<tag_key>=<tag_value>]] <field_key>=<field_value>[,<field_key>=<field_value>] [<timestamp>]`

where timestamp is in nanoseconds since epoch.

A typical service metric event as recorded by Druid's logging emitter is: `Event [{"feed":"metrics","timestamp":"2017-10-31T09:09:06.857Z","service":"druid/historical","host":"historical001:8083","version":"0.11.0-SNAPSHOT","metric":"query/cache/total/hits","value":34787256}]`.

This event is parsed into line protocol according to these rules:

* The measurement becomes druid_query since query is the first part of the metric.
* The tags are service=druid/historical, hostname=historical001, metric=druid_cache_total. (The metric tag is the middle part of the druid metric separated with _ and preceded by druid_. Another example would be if an event has metric=query/time then there is no middle part and hence no metric tag)
* The field is druid_hits since this is the last part of the metric.

This gives the following String which can be POSTed to InfluxDB: `"druid_query,service=druid/historical,hostname=historical001,metric=druid_cache_total druid_hits=34787256 1509440946857000000"`

The InfluxDB emitter has a white list of dimensions
which will be added as a tag to the line protocol string if the metric has a dimension from the white list.
The value of the dimension is sanitized such that every occurrence of a dot or whitespace is replaced with a `_` .
