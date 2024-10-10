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

You can use lookups in Apache Druid to replace dimension values in tables with new values, allowing join-like capability.
Applying lookups in Druid is similar to joining a dimension table in a data warehouse.

In Druid lookups, a `key` refers to a dimension value to match, and a `value` refers to its replacement. So if you want to map `appid-12345` to `Example app` the key is `appid-12345` and the value is `Example app`.

Specific lookup types are available as extensions:

- [lookups-cached-global](./lookups-cached-global.md): Globally cached lookups from local files, remote URIs, or JDBC.
- [kafka-extraction-namespace](./kafka-extraction-namespace.md): Globally cached lookups from a Kafka topic.

## Lookups use cases

Lookups support use cases where keys map one-to-one to unique values, such as country
code and country name, and also use cases where multiple IDs map to the same value, for example, multiple app IDs mapping to a single account manager. Druid can apply [query rewrite](#query-rewrites) operations to one-to-one lookups.

### Data accessed by lookups

Lookups always use current data and can't access historical data. This means that if the chief account manager for a particular app ID changes, and you issue a query with a lookup to store the app ID to account manager relationship, Druid returns the current account manager for that app ID regardless of the time range you specify in the query.

:::info
[Multi-value dimensions](multi-value-dimensions.md) (MVDs) are not supported as keys in lookups. For example, to map the MVD `["A", "B", "C"]` to the value `x` in your lookup, flatten the MVD and map each element of the MVD to the value. Your lookup will have separate key-value pairs for each element of the MVD: `"A": "x"`, `"B": "x"`, and `"C": "x"`.
:::

Query Syntax
------------

You can query lookups using the [`LOOKUP`](sql-scalar.md#string-functions) function in [Druid SQL](sql.md). For example:

```sql
SELECT
  LOOKUP(store, 'store_to_country') AS country,
  SUM(revenue)
FROM sales
GROUP BY 1
```

The `LOOKUP` function also accepts a third argument called `replaceMissingValueWith` as a constant string.
If the lookup doesn't contain a value for the provided key, the function returns `replaceMissingValueWith` rather than `NULL`, just like `COALESCE`.

`LOOKUP(store, 'store_to_country', 'NA')` is equivalent to
`COALESCE(LOOKUP(store, 'store_to_country'), 'NA')`.

You can also query lookups using the [JOIN](datasource.md#join) operator. For example:

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
The `LOOKUP` function can perform automatic [query rewrites](#query-rewrites). Rewrites aren't available when using `JOIN`.
:::

In native queries, you can query lookups using [dimension specs or extraction functions](dimensionspecs.md).

## Query rewrites

Druid can perform automatic query rewrites when using the LOOKUP function: [reverse lookups](#reverse-lookup) and [pulling up through `GROUP BY`](#pull-up). The following sections describe these rewrites and their requirements.

### Reverse lookup

When `LOOKUP` function calls appear in the WHERE clause of a query, Druid reverses them [when possible](#table).
For example, if the lookup table `sku_to_name` contains the mapping `'WB00013' => 'WhizBang Sprocket'`, Druid automatically rewrites the following query:

```sql
SELECT
  LOOKUP(sku, 'sku_to_name') AS name,
  SUM(revenue)
FROM sales
WHERE LOOKUP(sku, 'sku_to_name') = 'WhizBang Sprocket'
GROUP BY LOOKUP(sku, 'sku_to_name')
```

The result of the rewrite is as follows:

```sql
SELECT
  LOOKUP(sku, 'sku_to_name') AS name,
  SUM(revenue)
FROM sales
WHERE sku = 'WB00013'
GROUP BY LOOKUP(sku, 'sku_to_name')
```

In the rewrite, data servers don't need to apply the LOOKUP function while filtering, and can make more efficient use of indexes for `sku`.

The following table shows some examples that are reversible in Druid's default null handling mode:

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

You can see the difference in the native query Druid generates during SQL planning, which you can retrieve with [`EXPLAIN PLAN FOR`](sql.md#explain-plan).
When a lookup is reversed in this way, the `lookup` function disappears and is replaced by a simpler filter, typically of type `equals` or `in`.

Lookups are not reversed if the number of matching keys exceeds the [`sqlReverseLookupThreshold`](sql-query-context.md)
or [`inSubQueryThreshold`](sql-query-context.md) for the query.

This rewrite adds some planning time that may become noticeable for larger lookups, especially if many keys map to the same value.
You can see the impact on planning time in the `sqlQuery/planningTimeMs` metric.
You can also measure the time taken by `EXPLAIN PLAN FOR`, which plans the query but doesn't execute it.

To disable reverse lookup rewrites, set `sqlReverseLookup` to `false` in your [query context](sql-query-context.md).

### Pull up

Lookups marked as [injective](#injective-lookups) can be pulled up through a `GROUP BY`.
For example, if the lookup `sku_to_name` is injective, Druid automatically rewrites this query:

```sql
SELECT
  LOOKUP(sku, 'sku_to_name') AS name,
  SUM(revenue)
FROM sales
GROUP BY LOOKUP(sku, 'sku_to_name')
```

The result of the rewrite is as follows:

```sql
SELECT
  LOOKUP(sku, 'sku_to_name') AS name,
  SUM(revenue)
FROM sales
GROUP BY sku
```

The difference is that the `LOOKUP` function isn't applied until after the `GROUP BY` operation finishes, which speeds up the `GROUP BY` operation.

You can see the difference in the native query Druid generates during SQL planning, which you can retrieve with [`EXPLAIN PLAN FOR`](sql.md#explain-plan).
When Druid pulls up a lookup in this way, the `LOOKUP` function call typically moves from the `virtualColumns` or `dimensions` section of a native query into the
`postAggregations` section.

To disable pullup lookup rewrites, set `sqlPullUpLookup` to `false` in your [query context](sql-query-context.md).

### Injective lookups

Injective lookups are eligible for the largest set of query rewrites.
Injective lookups must satisfy the following "one-to-one lookup" properties:

- All values in the lookup table must be unique&mdash;no two keys can map to the same value.
- The lookup table must have a key-value pair defined for every input that the `LOOKUP` function call might encounter. For example, when calling `LOOKUP(sku, 'sku_to_name')`, the `sku_to_name` lookup table must contain a key for all possible `sku` entries.
- In SQL-compatible null-handling mode (when `druid.generic.useDefaultValueForNull = false`, which is the default) Druid doesn't require injective lookup tables to contain keys for `null`, since `LOOKUP` of `null` is always `null` itself.
- When `druid.generic.useDefaultValueForNull = true`, a `LOOKUP` of `null` retrieves the value mapped to the empty-string key (`""`). In this mode, injective lookup tables must contain an empty-string key if the `LOOKUP` function might encounter null input values.

To determine whether a lookup is injective, Druid relies on an `injective` property that you can set in the [lookup definition](./lookups-cached-global.md).
In general, set `injective: true` for any lookup that satisfies the required properties, to allow Druid to run your queries as fast as possible.

Druid doesn't verify whether lookups satisfy these required properties.
Druid might return incorrect query results if you set `injective: true` for a lookup table that isn't actually a one-to-one lookup.

## Dynamic configuration

The following documents the behavior of the cluster-wide config which is accessible through the Coordinator.
The configuration is propagated through the concept of "tier" of servers.
A "tier" is defined as a group of services which should receive a set of lookups.
For example, you might have all Historicals be part of `__default`, and Peons be part of individual tiers for the datasources they are tasked with.
The tiers for lookups are completely independent of Historical tiers.

You can access these configs using JSON through the following URI template:

```
http://<COORDINATOR_IP>:<PORT>/druid/coordinator/v1/lookups/config/{tier}/{id}
```

Prepend `http://<COORDINATOR_IP>:<PORT>` to all URIs below.

If you have never configured lookups before, you must post an empty JSON object `{}` to `/druid/coordinator/v1/lookups/config` to initialize the configuration.

These endpoints return one of the following results:

* 404 if the resource is not found.
* 400 if there is a problem in the formatting of the request.
* 202 if the request was accepted asynchronously (`POST` and `DELETE`).
* 200 if the request succeeded (`GET` only).

## Configuration propagation behavior

Druid propagates the configuration to the query serving processes (Broker, Router, Peon, or Historical) by the Coordinator.
The query serving processes have an internal API for managing lookups.
The Coordinator uses the API to periodically check whether any of the processes need to load/drop lookups and updates them appropriately.

Note that a single query serving process can only handle two simultaneous lookup configuration propagation requests. Druid applies this limit to prevent lookup handling from consuming too many server HTTP connections.

## API

See [Lookups API](../api-reference/lookups-api.md) for reference on configuring lookups and lookup status. 

## Configuration

See [Lookups Dynamic Configuration](../configuration/index.md#lookups-dynamic-configuration) for Coordinator configuration.

To configure a Broker, Router, Historical, or Peon to announce itself as part of a lookup tier, use following properties:

|Property | Description | Default |
|---------|-------------|---------|
|`druid.lookup.lookupTier`| The tier for **lookups** for this process. Independent of other tiers.|`__default`|
|`druid.lookup.lookupTierIsDatasource`|For some tasks like indexing services, Druid passed the datasource in the task's runtime properties. This option fetches the `tierName` from the same value as the datasource for the task. We suggest that you only use this as Peon options for the indexing service, if at all. If `true`, you must set `druid.lookup.lookupTier` to `true`.|`false`|

To configure the behavior of the dynamic configuration manager, use the following properties on the Coordinator:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.manager.lookups.hostTimeout`|Timeout (in ms) per host for processing requests.|`2000`(2 seconds)|
|`druid.manager.lookups.allHostTimeout`|Timeout (in ms) to finish lookup management on all processes.|`900000`(15 mins)|
|`druid.manager.lookups.period`|How long to pause between management cycles.|`120000`(2 mins)|
|`druid.manager.lookups.threadPoolSize`|Number of service processes to manage concurrently.|`10`|

## Saving configuration across restarts

You can save the configuration across restarts so a process doesn't have to wait for Coordinator action to repopulate its lookups. To do this, set the following properties:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.lookup.snapshotWorkingDir`|Working path used to store snapshot of current lookup configuration. Leaving this property null disables the snapshot/bootstrap utility.|null|
|`druid.lookup.enableLookupSyncOnStartup`|Enables the lookup synchronization process with Coordinator on startup. The queryable processes fetch and load the lookups from the Coordinator instead of waiting for the Coordinator to load the lookups for them. You can disable this option if there are no lookups configured in the cluster.|true|
|`druid.lookup.numLookupLoadingThreads`|Number of threads for loading the lookups in parallel on startup. Druid destroys this thread pool once startup is complete. It is not kept during the lifetime of the JVM.|Available Processors / 2|
|`druid.lookup.coordinatorFetchRetries`|Number of retries to fetch the lookup bean list from the Coordinator, during the sync on startup.|3|
|`druid.lookup.lookupStartRetries`|Number of retries to start each lookup, either during the sync on startup, or during the runtime.|3|
|`druid.lookup.coordinatorRetryDelay`|Delay period (in ms) between retries to fetch the lookup list from the Coordinator during the sync on startup.|60_000|

## Introspect a Lookup

The Broker provides an API for lookup introspection if the lookup type implements a `LookupIntrospectHandler`.

Send a `GET` request to `/druid/v1/lookups/introspect/{lookupId}` to return the map of complete values. For example:

`GET /druid/v1/lookups/introspect/nato-phonetic`
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

Send a `GET` request to `/druid/v1/lookups/introspect/{lookupId}/keys` to return the list of keys. For example:

`GET /druid/v1/lookups/introspect/nato-phonetic/keys`
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

Send a `GET` request to `/druid/v1/lookups/introspect/{lookupId}/values` to return the list of values. For example:

`GET /druid/v1/lookups/introspect/nato-phonetic/values`
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
