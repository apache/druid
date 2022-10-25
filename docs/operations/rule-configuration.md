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

Data retention rules allow you to configure Apache Druid to conform to your data retention policies. Your data retention policies specify which data to retain and which data to drop from the cluster.

Druid supports [load](#load-rules), [drop](#drop-rules), and [broadcast](#broadcast-rules) rules. Each rule is a JSON object&mdash;see the  [retention rules reference](#retention-rules-reference) for more information on each type.

You can configure a default set of rules to apply to all datasources, and/or you can set specific rules for specific datasources. See [rule order](#rule-order) to see how rule order impacts the way the Coordinator applies retention rules.

Retention rules are persistent: they remain in effect until you change them. Druid stores retention rules in its [metadata store](../dependencies/metadata-storage.md).

## Set a retention rule

You can use the Druid [web console](./web-console.md) or the [Coordinator API](./api-reference.md#coordinator) to create and manage retention rules.

### Use the web console

To set retention rules in the Druid web console:

1. On the console home page, click **Datasources**. 
2. Click the name of your datasource to open the data window.
3. Select **Actions > Edit retention rules**.
4. Click **+New rule**.
5. Select a rule type and set properties according to the [rules reference]().
6. Click **Next** and enter a description for the rule.
7. Click **Save** to save and apply the rule to the datasource.

### Use the Coordinator API

To set one or more default retention rules for all datasources, send a POST request containing a JSON object for each rule to `/druid/coordinator/v1/rules/_default`.

The following example request sets a forever broadcast rule for all datasources:

```bash
curl --location --request POST 'http://localhost:8888/druid/coordinator/v1/rules/_default' \
--header 'Content-Type: application/json' \
--data-raw '[{
  "type": "broadcastForever"
}]'
```

To set one or more retention rules for a specific datasource, send a POST request containing a JSON object for each rule to `/druid/coordinator/v1/rules/{datasourceName}`.

The following example request sets a period drop rule for the `wikipedia` datasource:

```bash
curl --location --request POST 'http://localhost:8888/druid/coordinator/v1/rules/wikipedia' \
--header 'Content-Type: application/json' \
--data-raw '[{
   "type": "dropByPeriod",
   "period": "P1M",
   "includeFuture": true
}]'
```

The rules API accepts an array of rules as JSON objects. The JSON object you send in the API request for each rule is specific to the rules types outlined below.

> You must pass the entire array of rules with each API request. The request overwrites

### Rule order

The order of rules is important. The Coordinator reads rules in the order in which they appear in the rules list. For example, in the following screenshot the Coordinator evaluates data against rule 1, then rule 2, then rule 3:

![retention rules](../assets/retention-rules.png)

In the web console you can use the up and down arrows on the right side of the interface to change the order of the rules.

The Coordinator cycles through all used segments and matches each segment with the first rule that applies. Each segment can only match a single rule.

## Retention rules reference

### Load rules

Load rules define how Druid assigns segments to historical process tiers, and how many replicas of a segment exist in each tier.

If you have a single tier, Druid automatically names the tier `_default` and loads all segments onto it. If you define an additional tier, you must define a load rule to specify which segments to load on that tier. Until you define a load rule, your new tier remains empty.

If you want to use a load rule to retain only data from a defined period of time, you must also define a drop rule. If you don't define a drop rule, Druid retains data that doesn't lie within your defined period according to the default rule, `loadForever`.

#### Forever load rule

The forever load rule is the default rule Druid applies to datasources. Forever load rules have type `loadForever`. 

The following example places one replica of each segment on a custom tier named `hot`, and another single replica on the default tier.

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
- `tieredReplicants`: a map of tier names to the number of segment replicas for that tier.

#### Period load rule

Druid compares a segment's interval to the period you specify in the rule. The rule matches if the period overlaps the segment interval. 

Period load rules have type `loadByPeriod` and the following JSON structure:

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
- `tieredReplicants`: a map of tier names to the number of segment replicas for that tier.

#### Interval load rule

You can use an interval load rule to define an interval range to load onto a specified tier.

Interval load rules have type `loadByInterval` and the following JSON structure:

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
- `interval`: the load interval specified as an ISO-8601 [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) range encoded as a string.
- `tieredReplicants`: a map of tier names to the number of segment replicas for that tier.

### Drop rules

Drop rules define when Druid drops segments from the cluster.

#### Forever drop rule

Druid drops all segments with this rule from the cluster. 

Forever drop rules have type `dropForever`:

```json
{
  "type": "dropForever",
}
```

#### Period drop rule

Druid compares a segment's interval to the period you specify in the rule. The rule matches if the period contains the segment interval. This rule always drops recent data.

Period drop rules have type `dropByPeriod` and the following JSON structure:

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

#### Period drop before rule

Druid compares a segment's interval to the period you specify in the rule. The rule matches if the segment interval is before the specified period. 

If you only want to retain recent data, you can use this rule to drop old data before a specified period, and add a `loadForever` rule to retain the data that follows it. Note that the rule combination `dropBeforeByPeriod` + `loadForever` is equivalent to `loadByPeriod(includeFuture = true)` + `dropForever`.

Period drop rules have type `dropBeforeByPeriod` and the following JSON structure:

```json
{
  "type": "dropBeforeByPeriod",
  "period": "P1M"
}
```

Set the following property:
- `period`: a JSON object representing [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) periods.

#### Interval drop rule

Druid drops all segments that match the defined interval.

Interval drop rules have type `dropByInterval` and the following JSON structure:

```json
{
  "type": "dropByInterval",
  "interval": "2012-01-01/2013-01-01"
}
```

Set the following property:
- `interval`: the drop interval specified as an ISO-8601 [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) range encoded as a string.

### Broadcast rules

Broadcast rules instruct Druid to load segments of a datasource in all brokers, historicals, tasks, and indexers in the cluster.

Note that the broadcast segments are only directly queryable through the historicals, but Druid loads them on other server types to support join queries.

#### Forever broadcast rule

This rule applies to all segments of a datasource, covering all intervals.

Forever broadcast rules have type `broadcastForever`:

```json
{
  "type": "broadcastForever",
}
``` 

#### Period broadcast rule

Druid compares a segment's interval to the period you specify in the rule. The rule matches if the period overlaps the segment interval.

Period broadcast rules have type `broadcastByPeriod` and the following JSON structure:

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

#### Interval broadcast rule

Druid broadcasts all segments that match the defined interval.

Interval broadcast rules have type `broadcastByInterval` and the following JSON structure:

```json
{
  "type": "broadcastByInterval",
  "interval": "2012-01-01/2013-01-01"
}
```

Set the following property:

- `interval`: the broadcast interval specified as an ISO-8601 [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) range encoded as a string.

## Permanently delete data

Druid can fully drop data from the cluster, wipe the metadata store entry, and remove the data from deep storage for any segments marked `unused`. Note that Druid always marks segments dropped from the cluster by rules as `unused`. You can submit a [kill task](./ingestion/tasks) to the [Overlord](./design/overlord) to do this.

## Reload dropped data

You can't use a single rule to reload data Druid has dropped from a cluster.

To reload dropped data:

1. Set your retention period&mdash;for example, change the retention period from one month to two months.
2. Use the web console or the API to mark all segments belonging to the datasource as `used`.

## Learn more

For more information about using retention rules in Druid, see the following topics:

- [Tutorial: Configuring data retention](../tutorials/tutorial-retention.md)
- [Configure Druid for mixed workloads](../operations/mixed-workloads.md)
- [Router process](../design/router.md)
