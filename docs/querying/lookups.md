---
id: lookups
title: "Lookups"
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

Lookups are a concept in Apache Druid where dimension values are (optionally) replaced with new values, allowing join-like
functionality. Applying lookups in Druid is similar to joining a dimension table in a data warehouse. See
[dimension specs](../querying/dimensionspecs.md) for more information. For the purpose of these documents, a "key"
refers to a dimension value to match, and a "value" refers to its replacement. So if you wanted to map
`appid-12345` to `Super Mega Awesome App` then the key would be `appid-12345` and the value would be
`Super Mega Awesome App`.

It is worth noting that lookups support not just use cases where keys map one-to-one to unique values, such as country
code and country name, but also support use cases where multiple IDs map to the same value, e.g. multiple app-ids
mapping to a single account manager. When lookups are one-to-one, Druid is able to apply additional
[query rewrites](#query-rewrites); see below for more details.

Lookups do not have history. They always use the current data. This means that if the chief account manager for a
particular app-id changes, and you issue a query with a lookup to store the app-id to account manager relationship,
it will return the current account manager for that app-id REGARDLESS of the time range over which you query.

If you require data time range sensitive lookups, such a use case is not currently supported dynamically at query time,
and such data belongs in the raw denormalized data for use in Druid.

Lookups are generally preloaded in-memory on all servers. But very small lookups (on the order of a few dozen to a few
hundred entries) can also be passed inline in native queries time using the "map" lookup type. Refer to the
[dimension specs](dimensionspecs.md) documentation for details.

Other lookup types are available as extensions, including:

- Globally cached lookups from local files, remote URIs, or JDBC through [lookups-cached-global](../development/extensions-core/lookups-cached-global.md).
- Globally cached lookups from a Kafka topic through [kafka-extraction-namespace](../development/extensions-core/kafka-extraction-namespace.md).

Query Syntax
------------

In [Druid SQL](sql.md), lookups can be queried using the [`LOOKUP` function](sql-scalar.md#string-functions), for example:

```sql
SELECT
  LOOKUP(store, 'store_to_country') AS country,
  SUM(revenue)
FROM sales
GROUP BY 1
```

The `LOOKUP` function also accepts a third argument called `replaceMissingValueWith` as a constant string. If the lookup
does not contain a value for the provided key, then the `LOOKUP` function returns this `replaceMissingValueWith` value
rather than `NULL`, just like `COALESCE`. For example, `LOOKUP(store, 'store_to_country', 'NA')` is equivalent to
`COALESCE(LOOKUP(store, 'store_to_country'), 'NA')`.

Lookups can be queried using the [JOIN operator](datasource.md#join):

```sql
SELECT
  store_to_country.v AS country,
  SUM(sales.revenue) AS country_revenue
FROM
  sales
  INNER JOIN lookup.store_to_country ON sales.store = store_to_country.k
GROUP BY 1
```

:::info
The `LOOKUP` function has automatic [query rewrites](#query-rewrites) available that the `JOIN` approach does not,
including [reverse lookups](#reverse-lookup) and [pulling up through `GROUP BY`](#pull-up). If these rewrites are
important for you, consider using the `LOOKUP` function instead of `JOIN`.
:::

In native queries, lookups can be queried with [dimension specs or extraction functions](dimensionspecs.md).

Query Rewrites
--------------
Druid can perform two automatic query rewrites when using the `LOOKUP` function: [reverse lookups](#reverse-lookup) and
[pulling up through `GROUP BY`](#pull-up). These rewrites and their requirements are described in the following
sections.

### Reverse lookup

When `LOOKUP` function calls appear in the `WHERE` clause of a query, Druid reverses them [when possible](#table).
For example, if the lookup table `sku_to_name` contains the mapping `'WB00013' => 'WhizBang Sprocket'`, then Druid
automatically rewrites this query:

```sql
SELECT
  LOOKUP(sku, 'sku_to_name') AS name,
  SUM(revenue)
FROM sales
WHERE LOOKUP(sku, 'sku_to_name') = 'WhizBang Sprocket'
GROUP BY LOOKUP(sku, 'sku_to_name')
```

Into this:

```sql
SELECT
  LOOKUP(sku, 'sku_to_name') AS name,
  SUM(revenue)
FROM sales
WHERE sku = 'WB00013'
GROUP BY LOOKUP(sku, 'sku_to_name')
```

The difference is that in the latter case, data servers do not need to apply the `LOOKUP` function while filtering, and
can make more efficient use of indexes for `sku`.

<a name="table">The following table</a> contains examples of when it is possible to reverse calls to `LOOKUP` while in
Druid's default null handling mode. The list of examples is illustrative, albeit not exhaustive.

|SQL|Reversible?|
|---|-----------|
|`LOOKUP(sku, 'sku_to_name') = 'WhizBang Sprocket'`|Yes|
|`LOOKUP(sku, 'sku_to_name') IS NOT DISTINCT FROM 'WhizBang Sprocket'`|Yes, for non-null literals|
|`LOOKUP(sku, 'sku_to_name') <> 'WhizBang Sprocket'`|No, unless `sku_to_name` is [injective](#injective-lookups)|
|`LOOKUP(sku, 'sku_to_name') IS DISTINCT FROM 'WhizBang Sprocket'`|Yes, for non-null literals|
|`LOOKUP(sku, 'sku_to_name') = 'WhizBang Sprocket' IS NOT TRUE`|Yes|
|`LOOKUP(sku, 'sku_to_name') IN ('WhizBang Sprocket', 'WhizBang Chain')`|Yes|
|`LOOKUP(sku, 'sku_to_name') NOT IN ('WhizBang Sprocket', 'WhizBang Chain')`|No, unless `sku_to_name` is [injective](#injective-lookups)|
|`LOOKUP(sku, 'sku_to_name') IN ('WhizBang Sprocket', 'WhizBang Chain') IS NOT TRUE`|Yes|
|`LOOKUP(sku, 'sku_to_name') IS NULL`|No|
|`LOOKUP(sku, 'sku_to_name') IS NOT NULL`|No|
|`LOOKUP(UPPER(sku), 'sku_to_name') = 'WhizBang Sprocket'`|Yes, to `UPPER(sku) = [key for 'WhizBang Sprocket']` (the `UPPER` function remains)|
|`COALESCE(LOOKUP(sku, 'sku_to_name'), 'N/A') = 'WhizBang Sprocket'`|Yes, but see next item for `= 'N/A'`|
|`COALESCE(LOOKUP(sku, 'sku_to_name'), 'N/A') = 'N/A'`|No, unless `sku_to_name` is [injective](#injective-lookups), which allows Druid to ignore the `COALESCE`|
|`COALESCE(LOOKUP(sku, 'sku_to_name'), 'N/A') = 'WhizBang Sprocket' IS NOT TRUE`|Yes|
|`COALESCE(LOOKUP(sku, 'sku_to_name'), 'N/A') <> 'WhizBang Sprocket'`|Yes, but see next item for `<> 'N/A'`|
|`COALESCE(LOOKUP(sku, 'sku_to_name'), 'N/A') <> 'N/A'`|No, unless `sku_to_name` is [injective](#injective-lookups), which allows Druid to ignore the `COALESCE`|
|`COALESCE(LOOKUP(sku, 'sku_to_name'), sku) = 'WhizBang Sprocket'`|No, `COALESCE` is only reversible when the second argument is a constant|
|`LOWER(LOOKUP(sku, 'sku_to_name')) = 'whizbang sprocket'`|No, functions other than `COALESCE` are not reversible|
|`MV_CONTAINS(LOOKUP(sku, 'sku_to_name'), 'WhizBang Sprocket')`|Yes|
|`NOT MV_CONTAINS(LOOKUP(sku, 'sku_to_name'), 'WhizBang Sprocket')`|No, unless `sku_to_name` is [injective](#injective-lookups)|
|`MV_OVERLAP(LOOKUP(sku, 'sku_to_name'), ARRAY['WhizBang Sprocket'])`|Yes|
|`NOT MV_OVERLAP(LOOKUP(sku, 'sku_to_name'), ARRAY['WhizBang Sprocket'])`|No, unless `sku_to_name` is [injective](#injective-lookups)|

You can see the difference in the native query that is generated during SQL planning, which you
can retrieve with [`EXPLAIN PLAN FOR`](sql.md#explain-plan). When a lookup is reversed in this way, the `lookup`
function disappears and is replaced by a simpler filter, typically of type `equals` or `in`.

Lookups are not reversed if the number of matching keys exceeds the [`sqlReverseLookupThreshold`](sql-query-context.md)
or [`inSubQueryThreshold`](sql-query-context.md) for the query.

This rewrite adds some planning time that may become noticeable for larger lookups, especially if many keys map to the
same value. You can see the impact on planning time in the `sqlQuery/planningTimeMs` metric. You can also measure the
time taken by `EXPLAIN PLAN FOR`, which plans the query but does not execute it.

This rewrite can be disabled by setting [`sqlReverseLookup: false`](sql-query-context.md) in your query context.

### Pull up

Lookups marked as [_injective_](#injective-lookups) can be pulled up through a `GROUP BY`. For example, if the lookup
`sku_to_name` is injective, Druid automatically rewrites this query:

```sql
SELECT
  LOOKUP(sku, 'sku_to_name') AS name,
  SUM(revenue)
FROM sales
GROUP BY LOOKUP(sku, 'sku_to_name')
```

Into this:

```sql
SELECT
  LOOKUP(sku, 'sku_to_name') AS name,
  SUM(revenue)
FROM sales
GROUP BY sku
```

The difference is that the `LOOKUP` function is not applied until after the `GROUP BY` is finished, which speeds up
the `GROUP BY`.

You can see the difference in the native query that is generated during SQL planning, which you
can retrieve with [`EXPLAIN PLAN FOR`](sql.md#explain-plan). When a lookup is pulled up in this way, the `lookup`
function call typically moves from the `virtualColumns` or `dimensions` section of a native query into the
`postAggregations`.

This rewrite can be disabled by setting [`sqlPullUpLookup: false`](sql-query-context.md) in your query context.

### Injective lookups

Injective lookups are eligible for the largest set of query rewrites. Injective lookups must satisfy the following
"one-to-one lookup" properties:

- All values in the lookup table must be unique. That is, no two keys can map to the same value.
- The lookup table must have a key-value pair defined for every input that the `LOOKUP` function call may
  encounter. For example, when calling `LOOKUP(sku, 'sku_to_name')`, the `sku_to_name` lookup table must have a key
  for all possible `sku`.
- In SQL-compatible null handling mode (when `druid.generic.useDefaultValueForNull = false`, the default) injective
  lookup tables are not required to have keys for `null`, since `LOOKUP` of `null` is always `null` itself.
- When `druid.generic.useDefaultValueForNull = true`, a `LOOKUP` of `null` retrieves the value mapped to the
  empty-string key (`""`). In this mode, injective lookup tables must have an empty-string key if the `LOOKUP`
  function may encounter null input values.

To determine whether a lookup is injective, Druid relies on an `injective` property that you can set in the
[lookup definition](../development/extensions-core/lookups-cached-global.md). In general, you should set
`injective: true` for any lookup that satisfies the required properties, to allow Druid to run your queries as fast as
possible.

Druid does not verify whether lookups satisfy these required properties. Druid may return incorrect query results
if you set `injective: true` for a lookup table that is not actually a one-to-one lookup.

Dynamic Configuration
---------------------

The following documents the behavior of the cluster-wide config which is accessible through the Coordinator.
The configuration is propagated through the concept of "tier" of servers.
A "tier" is defined as a group of services which should receive a set of lookups.
For example, you might have all Historicals be part of `__default`, and Peons be part of individual tiers for the datasources they are tasked with.
The tiers for lookups are completely independent of Historical tiers.

These configs are accessed using JSON through the following URI template

```
http://<COORDINATOR_IP>:<PORT>/druid/coordinator/v1/lookups/config/{tier}/{id}
```

All URIs below are assumed to have `http://<COORDINATOR_IP>:<PORT>` prepended.

If you have NEVER configured lookups before, you MUST post an empty json object `{}` to `/druid/coordinator/v1/lookups/config` to initialize the configuration.

These endpoints will return one of the following results:

* 404 if the resource is not found
* 400 if there is a problem in the formatting of the request
* 202 if the request was accepted asynchronously (`POST` and `DELETE`)
* 200 if the request succeeded (`GET` only)

## Configuration propagation behavior
The configuration is propagated to the query serving processes (Broker / Router / Peon / Historical) by the Coordinator.
The query serving processes have an internal API for managing lookups on the process and those are used by the Coordinator.
The Coordinator periodically checks if any of the processes need to load/drop lookups and updates them appropriately.

Please note that only 2 simultaneous lookup configuration propagation requests can be concurrently handled by a single query serving process. This limit is applied to prevent lookup handling from consuming too many server HTTP connections.

## API
See [Lookups API](../api-reference/lookups-api.md) for reference on configuring lookups and lookup status. 

## Configuration

See [Lookups Dynamic Configuration](../configuration/index.md#lookups-dynamic-configuration) for Coordinator configuration.

To configure a Broker / Router / Historical / Peon to announce itself as part of a lookup tier, use following properties.

|Property | Description | Default |
|---------|-------------|---------|
|`druid.lookup.lookupTier`| The tier for **lookups** for this process. This is independent of other tiers.|`__default`|
|`druid.lookup.lookupTierIsDatasource`|For some things like indexing service tasks, the datasource is passed in the runtime properties of a task. This option fetches the tierName from the same value as the datasource for the task. It is suggested to only use this as Peon options for the indexing service, if at all. If true, `druid.lookup.lookupTier` MUST NOT be specified|`"false"`|

To configure the behavior of the dynamic configuration manager, use the following properties on the Coordinator:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.manager.lookups.hostTimeout`|Timeout (in ms) PER HOST for processing request|`2000`(2 seconds)|
|`druid.manager.lookups.allHostTimeout`|Timeout (in ms) to finish lookup management on all the processes.|`900000`(15 mins)|
|`druid.manager.lookups.period`|How long to pause between management cycles|`120000`(2 mins)|
|`druid.manager.lookups.threadPoolSize`|Number of service processes that can be managed concurrently|`10`|

## Saving configuration across restarts

It is possible to save the configuration across restarts such that a process will not have to wait for Coordinator action to re-populate its lookups. To do this the following property is set:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.lookup.snapshotWorkingDir`|Working path used to store snapshot of current lookup configuration, leaving this property null will disable snapshot/bootstrap utility|null|
|`druid.lookup.enableLookupSyncOnStartup`|Enable the lookup synchronization process with Coordinator on startup. The queryable processes will fetch and load the lookups from the Coordinator instead of waiting for the Coordinator to load the lookups for them. Users may opt to disable this option if there are no lookups configured in the cluster.|true|
|`druid.lookup.numLookupLoadingThreads`|Number of threads for loading the lookups in parallel on startup. This thread pool is destroyed once startup is done. It is not kept during the lifetime of the JVM|Available Processors / 2|
|`druid.lookup.coordinatorFetchRetries`|How many times to retry to fetch the lookup bean list from Coordinator, during the sync on startup.|3|
|`druid.lookup.lookupStartRetries`|How many times to retry to start each lookup, either during the sync on startup, or during the runtime.|3|
|`druid.lookup.coordinatorRetryDelay`|How long to delay (in millis) between retries to fetch lookup list from the Coordinator during the sync on startup.|60_000|

## Introspect a Lookup

The Broker provides an API for lookup introspection if the lookup type implements a `LookupIntrospectHandler`.

A `GET` request to `/druid/v1/lookups/introspect/{lookupId}` will return the map of complete values.

ex: `GET /druid/v1/lookups/introspect/nato-phonetic`
```
{
    "A": "Alfa",
    "B": "Bravo",
    "C": "Charlie",
    ...
    "Y": "Yankee",
    "Z": "Zulu",
    "-": "Dash"
}

```

The list of keys can be retrieved via `GET` to `/druid/v1/lookups/introspect/{lookupId}/keys"`

ex: `GET /druid/v1/lookups/introspect/nato-phonetic/keys`
```
[
    "A",
    "B",
    "C",
    ...
    "Y",
    "Z",
    "-"
]
```

A `GET` request to `/druid/v1/lookups/introspect/{lookupId}/values"` will return the list of values.

ex: `GET /druid/v1/lookups/introspect/nato-phonetic/values`
```
[
    "Alfa",
    "Bravo",
    "Charlie",
    ...
    "Yankee",
    "Zulu",
    "Dash"
]
```
