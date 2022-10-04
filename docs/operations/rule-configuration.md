---
id: rule-configuration
title: "Using rules to drop and retain data"
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


In Apache Druid, [Coordinator processes](../design/coordinator.md) use rules to determine what data to retain or drop from the cluster. 

There are three types of rules: [load](#load-rules), [drop](#drop-rules), and [broadcast](#broadcast-rules). See the sections below for more information on each type.

The Coordinator loads a set of rules from the metadata storage. You can configure a default set of rules to apply to all data sources, and/or you can set specific rules for specific data sources. 

## Set a rule

To set a default retention rule for all data sources, send a POST request to the following API:

`/druid/coordinator/v1/rules/_default`

To set a retention rule for a specific data source, send a POST request to the following API:

`/druid/coordinator/v1/rules/{dataSourceName}`

The rules API accepts a list of rules. The payload you send in the API request for each rule is specific to the rules types outlined below.

You can also set rules using the [web console](./web-console.md). Go into a data source and select **Actions** > **Edit retention rules**.

### Rule order

The order of rules is important. The Coordinator reads rules in the order in which they appear in the rules list. For example, in the following screenshot the Coordinator evaluates data against rule 1, then rule 2, then rule 3:

![retention rules](../assets/retention-rules.png)

In the web console you can use the up and down arrows on the right side of the interface to change the order of the rules.

The Coordinator cycles through all used segments and matches each segment with the first rule that applies. Each segment can only match a single rule.

## Load rules

Load rules define how Druid assigns segments to historical process tiers, and how many replicas of a segment exist in each tier.

If you want to use a load rule to retain only data from a defined period of time, you must also define a drop rule. If you don't define a drop rule, Druid retains data that doesn't lie within your defined period according to the default rule, `loadForever`.

### Forever load rule

Forever load rules have type `loadForever` and the following example API payload:

```json
{
  "type": "loadForever",
  "tieredReplicants": {
    "hot": 1,
    "_default_tier": 1
  }
}
```
Set the following property:
- `tieredReplicants`: a JSON object containing tier names and the number of replicas for each tier.

The forever load rule is the default rule Druid applies to data sources.

### Interval load rule

Interval load rules have type `loadByInterval` and the following example API payload:


```json
{
  "type": "loadByInterval",
  "interval": "2012-01-01/2013-01-01",
  "tieredReplicants": {
    "hot": 1,
    "_default_tier": 1
  }
}
```

Set the following properties:
- `interval`: a JSON object representing [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) intervals.
- `tieredReplicants`: a JSON object containing tier names and the number of replicas for each tier.

### Period load rule

Period load rules have type `loadByPeriod` and the following example API payload:

```json
{
  "type": "loadByPeriod",
  "period": "P1M",
  "includeFuture": true,
  "tieredReplicants": {
      "hot": 1,
      "_default_tier": 1
  }
}
```

Set the following properties:
- `period`: a JSON object representing [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) periods. The period is from some time in the past to the future or to the current time, depending on the `includeFuture` flag.
- `includeFuture`: a boolean flag to indicate whether the load period includes the future. Defaults to `true`.
- `tieredReplicants`: a JSON object containing tier names and the number of replicas for each tier.

Druid compares a segment's interval to the period you specify in the rule. The rule matches if the period overlaps the segment interval. 

## Drop rules

Drop rules define when Druid drops segments from the cluster.

### Forever drop rule

Forever drop rules have type `dropForever`:

```json
{
  "type": "dropForever",
}
```

Druid drops all segments with this rule from the cluster. 

### Interval drop rule

Interval drop rules have type `dropByInterval` and the following example API payload:

```json
{
  "type": "dropByInterval",
  "interval": "2012-01-01/2013-01-01"
}
```

Set the following property:
- `interval`: a JSON object representing [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) intervals.

Druid drops all segments that match the defined interval.

### Period drop rule

Period drop rules have type `dropByPeriod` and the following example API payload:

```json
{
  "type": "dropByPeriod",
  "period": "P1M",
  "includeFuture": true,
}
```

Set the following properties:
- `period`: a JSON object representing [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) periods. The period is from some time in the past to the future or to the current time, depending on the `includeFuture` flag.
- `includeFuture`: a boolean flag to indicate whether the drop period includes the future. Defaults to `true`.

Druid compares a segment's interval to the period you specify in the rule. The rule matches if the period contains the segment interval. This rule always drops recent data.

### Period drop before rule

Period drop rules have type `dropBeforeByPeriod` and the following example API payload:

```json
{
  "type": "dropBeforeByPeriod",
  "period": "P1M"
}
```

Set the following property:
- `period`: a JSON object representing [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) periods.

Druid compares a segment's interval to the period you specify in the rule. The rule matches if the segment interval is before the specified period. 

If you only want to retain recent data, you can use this rule to drop old data before a specified period, and add a `loadForever` rule to retain the data that follows it. Note that the rule combination `dropBeforeByPeriod` + `loadForever` is equivalent to `loadByPeriod(includeFuture = true)` + `dropForever`.

## Broadcast rules

Broadcast rules instruct Druid to load segments of a data source in all brokers, historicals, tasks, and indexers in the cluster.

Note that the broadcast segments are only directly queryable through the historicals, but Druid loads them on other server types to support join queries.

### Forever broadcast rule

Forever broadcast rules have type `broadcastForever`:

```json
{
  "type": "broadcastForever",
}
```

This rule applies to all segments of a datasource, covering all intervals. 

### Interval broadcast rule

Interval broadcast rules have type `broadcastByInterval` and the following example API payload:

```json
{
  "type": "broadcastByInterval",
  "interval": "2012-01-01/2013-01-01"
}
```

Set the following property:

- `interval`: a JSON object representing [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) intervals.

Druid broadcasts all segments that match the defined interval.

### Period broadcast rule

Period broadcast rules have type `broadcastByPeriod` and the following example API payload:

```json
{
  "type": "broadcastByPeriod",
  "period": "P1M",
  "includeFuture": true,
}
```

Set the following properties:

- `period`: a JSON object representing [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) periods. The period is from some time in the past to the future or to the current time, depending on the `includeFuture` flag.
- `includeFuture`: a boolean flag to indicate whether the broadcast period includes the future. Defaults to `true`.

Druid compares a segment's interval to the period you specify in the rule. The rule matches if the period overlaps the segment interval.

## Permanently delete data

Druid can fully drop data from the cluster, wipe the metadata store entry, and remove the data from deep storage for any segments marked `unused`. Note that Druid always marks segments dropped from the cluster by rules as `unused`. You can submit a [kill task](./ingestion/tasks) to the [Overlord](./design/overlord) to do this.

## Reload dropped data

You can't use a single rule to reload data Druid has dropped from a cluster.

To reload dropped data:

1. Set your retention period&mdash;for example, change the retention period from one month to two months.
2. Use the web console or the API to mark all segments belonging to the data source as `used`.

## Learn more

For more information about using retention rules in Druid, see the following topics:

- [Tutorial: Configuring data retention](../tutorials/tutorial-retention.md)
- [Configure Druid for mixed workloads](../operations/mixed-workloads.md)
- [Router process](../design/router.md)
