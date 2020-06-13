---
id: rule-configuration
title: "Retaining or automatically dropping data"
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


In Apache Druid, Coordinator processes use rules to determine what data should be loaded to or dropped from the cluster. Rules are used for data retention and query execution, and are set on the Coordinator console (http://coordinator_ip:port).

There are three types of rules, i.e., load rules, drop rules, and broadcast rules. Load rules indicate how segments should be assigned to different historical process tiers and how many replicas of a segment should exist in each tier.
Drop rules indicate when segments should be dropped entirely from the cluster. Finally, broadcast rules indicate how segments of different datasources should be co-located in Historical processes.

The Coordinator loads a set of rules from the metadata storage. Rules may be specific to a certain datasource and/or a
default set of rules can be configured. Rules are read in order and hence the ordering of rules is important. The
Coordinator will cycle through all used segments and match each segment with the first rule that applies. Each segment
may only match a single rule.

Note: It is recommended that the Coordinator console is used to configure rules. However, the Coordinator process does have HTTP endpoints to programmatically configure rules.

## Load rules

Load rules indicate how many replicas of a segment should exist in a server tier. **Please note**: If a Load rule is used to retain only data from a certain interval or period, it must be accompanied by a Drop rule. If a Drop rule is not included, data not within the specified interval or period will be retained by the default rule (loadForever).

### Forever Load Rule

Forever load rules are of the form:

```json
{
  "type" : "loadForever",
  "tieredReplicants": {
    "hot": 1,
    "_default_tier" : 1
  }
}
```

* `type` - this should always be "loadForever"
* `tieredReplicants` - A JSON Object where the keys are the tier names and values are the number of replicas for that tier.


### Interval Load Rule

Interval load rules are of the form:

```json
{
  "type" : "loadByInterval",
  "interval": "2012-01-01/2013-01-01",
  "tieredReplicants": {
    "hot": 1,
    "_default_tier" : 1
  }
}
```

* `type` - this should always be "loadByInterval"
* `interval` - A JSON Object representing ISO-8601 Intervals
* `tieredReplicants` - A JSON Object where the keys are the tier names and values are the number of replicas for that tier.

### Period Load Rule

Period load rules are of the form:

```json
{
  "type" : "loadByPeriod",
  "period" : "P1M",
  "includeFuture" : true,
  "tieredReplicants": {
      "hot": 1,
      "_default_tier" : 1
  }
}
```

* `type` - this should always be "loadByPeriod"
* `period` - A JSON Object representing ISO-8601 Periods
* `includeFuture` - A JSON Boolean indicating whether the load period should include the future. This property is optional, Default is true.
* `tieredReplicants` - A JSON Object where the keys are the tier names and values are the number of replicas for that tier.

The interval of a segment will be compared against the specified period. The period is from some time in the past to the future or to the current time, which depends on `includeFuture` is true or false. The rule matches if the period *overlaps* the interval.

## Drop Rules

Drop rules indicate when segments should be dropped from the cluster.

### Forever Drop Rule

Forever drop rules are of the form:

```json
{
  "type" : "dropForever"
}
```

* `type` - this should always be "dropForever"

All segments that match this rule are dropped from the cluster.


### Interval Drop Rule

Interval drop rules are of the form:

```json
{
  "type" : "dropByInterval",
  "interval" : "2012-01-01/2013-01-01"
}
```

* `type` - this should always be "dropByInterval"
* `interval` - A JSON Object representing ISO-8601 Periods

A segment is dropped if the interval contains the interval of the segment.

### Period Drop Rule

Period drop rules are of the form:

```json
{
  "type" : "dropByPeriod",
  "period" : "P1M",
  "includeFuture" : true
}
```

* `type` - this should always be "dropByPeriod"
* `period` - A JSON Object representing ISO-8601 Periods
* `includeFuture` - A JSON Boolean indicating whether the load period should include the future. This property is optional, Default is true.

The interval of a segment will be compared against the specified period. The period is from some time in the past to the future or to the current time, which depends on `includeFuture` is true or false. The rule matches if the period *contains* the interval. This drop rule always dropping recent data.

### Period Drop Before Rule

Period drop before rules are of the form:

```json
{
  "type" : "dropBeforeByPeriod",
  "period" : "P1M"
}
```

* `type` - this should always be "dropBeforeByPeriod"
* `period` - A JSON Object representing ISO-8601 Periods

The interval of a segment will be compared against the specified period. The period is from some time in the past to the current time. The rule matches if the interval before the period. If you just want to retain recent data, you can use this rule to drop the old data that before a specified period and add a `loadForever` rule to follow it. Notes, `dropBeforeByPeriod + loadForever` is equivalent to `loadByPeriod(includeFuture = true) + dropForever`.

## Broadcast Rules

Broadcast rules indicate that segments of a data source should be loaded by all servers of a cluster of the following types: historicals, brokers, tasks, and indexers.

Note that the broadcast segments are only directly queryable through the historicals, but they are currently loaded on other server types to support join queries.

### Forever Broadcast Rule

Forever broadcast rules are of the form:

```json
{
  "type" : "broadcastForever"
}
```

* `type` - this should always be "broadcastForever"

This rule applies to all segments of a datasource, covering all intervals.

### Interval Broadcast Rule

Interval broadcast rules are of the form:

```json
{
  "type" : "broadcastByInterval",
  "interval" : "2012-01-01/2013-01-01"
}
```

* `type` - this should always be "broadcastByInterval"
* `interval` - A JSON Object representing ISO-8601 Periods. Only the segments of the interval will be broadcasted.

### Period Broadcast Rule

Period broadcast rules are of the form:

```json
{
  "type" : "broadcastByPeriod",
  "period" : "P1M",
  "includeFuture" : true
}
```

* `type` - this should always be "broadcastByPeriod"
* `period` - A JSON Object representing ISO-8601 Periods
* `includeFuture` - A JSON Boolean indicating whether the load period should include the future. This property is optional, Default is true.

The interval of a segment will be compared against the specified period. The period is from some time in the past to the future or to the current time, which depends on `includeFuture` is true or false. The rule matches if the period *overlaps* the interval.

## Permanently deleting data

Druid can fully drop data from the cluster, wipe the metadata store entry, and remove the data from deep storage for any
segments that are marked as unused (segments dropped from the cluster via rules are always marked as unused). You can
submit a [kill task](../ingestion/tasks.md) to the [Overlord](../design/overlord.md) to do this.

## Reloading dropped data

Data that has been dropped from a Druid cluster cannot be reloaded using only rules. To reload dropped data in Druid,
you must first set your retention period (i.e. changing the retention period from 1 month to 2 months), and then mark as
used all segments belonging to the datasource in the Druid Coordinator console, or through the Druid Coordinator
endpoints.
