---
layout: doc_page
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

# Lookups

<div class="note caution">
Lookups are an <a href="../development/experimental.html">experimental</a> feature.
</div>

Lookups are a concept in Druid where dimension values are (optionally) replaced with new values, allowing join-like
functionality. Applying lookups in Druid is similar to joining a dimension table in a data warehouse. See
[dimension specs](../querying/dimensionspecs.html) for more information. For the purpose of these documents, a "key"
refers to a dimension value to match, and a "value" refers to its replacement. So if you wanted to map
`appid-12345` to `Super Mega Awesome App` then the key would be `appid-12345` and the value would be
`Super Mega Awesome App`.

It is worth noting that lookups support not just use cases where keys map one-to-one to unique values, such as country
code and country name, but also support use cases where multiple IDs map to the same value, e.g. multiple app-ids
mapping to a single account manager. When lookups are one-to-one, Druid is able to apply additional optimizations at
query time; see [Query execution](#query-execution) below for more details.

Lookups do not have history. They always use the current data. This means that if the chief account manager for a 
particular app-id changes, and you issue a query with a lookup to store the app-id to account manager relationship, 
it will return the current account manager for that app-id REGARDLESS of the time range over which you query.

If you require data time range sensitive lookups, such a use case is not currently supported dynamically at query time, 
and such data belongs in the raw denormalized data for use in Druid.

Very small lookups (count of keys on the order of a few dozen to a few hundred) can be passed at query time as a "map" 
lookup as per [dimension specs](../querying/dimensionspecs.html).

Other lookup types are available as extensions, including:

- Globally cached lookups from local files, remote URIs, or JDBC through [lookups-cached-global](../development/extensions-core/lookups-cached-global.html).
- Globally cached lookups from a Kafka topic through [kafka-extraction-namespace](../development/extensions-core/kafka-extraction-namespace.html).

Query Execution
---------------
When executing an aggregation query involving lookups, Druid can decide to apply lookups either while scanning and
aggregating rows, or to apply them after aggregation is complete. It is more efficient to apply lookups after
aggregation is complete, so Druid will do this if it can. Druid decides this by checking if the lookup is marked
as "injective" or not. In general, you should set this property for any lookup that is naturally one-to-one, to allow
Druid to run your queries as fast as possible.

Injective lookups should include _all_ possible keys that may show up in your dataset, and should also map all keys to
_unique values_. This matters because non-injective lookups may map different keys to the same value, which must be
accounted for during aggregation, lest query results contain two result values that should have been aggregated into
one.

This lookup is injective (assuming it contains all possible keys from your data):

```
1 -> Foo
2 -> Bar
3 -> Billy
```

But this one is not, since both "2" and "3" map to the same key:

```
1 -> Foo
2 -> Bar
3 -> Bar
```

To tell Druid that your lookup is injective, you must specify `"injective" : true` in the lookup configuration. Druid
will not detect this automatically.

Dynamic Configuration
---------------------
<div class="note caution">
Dynamic lookup configuration is an <a href="../development/experimental.html">experimental</a> feature. Static
configuration is no longer supported.
</div>
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
The query serving nodes have an internal API for managing lookups on the node and those are used by the Coordinator.
The Coordinator periodically checks if any of the nodes need to load/drop lookups and updates them appropriately.

# API for configuring lookups

## Bulk update
Lookups can be updated in bulk by posting a JSON object to `/druid/coordinator/v1/lookups/config`. The format of the json object is as follows:

```json
{
    "<tierName>": {
        "<lookupName>": {
          "version": "<version>",
          "lookupExtractorFactory": {
            "type": "<someExtractorFactoryType>",
            "<someExtractorField>": "<someExtractorValue>"
          }
        }
    }
}
```

Note that "version" is an arbitrary string assigned by the user, when making updates to existing lookup then user would need to specify a lexicographically higher version.

For example, a config might look something like:

```json
{
  "__default": {
    "country_code": {
      "version": "v0",
      "lookupExtractorFactory": {
        "type": "map",
        "map": {
          "77483": "United States"
        }
      }
    },
    "site_id": {
      "version": "v0",
      "lookupExtractorFactory": {
        "type": "cachedNamespace",
        "extractionNamespace": {
          "type": "jdbc",
          "connectorConfig": {
            "createTables": true,
            "connectURI": "jdbc:mysql:\/\/localhost:3306\/druid",
            "user": "druid",
            "password": "diurd"
          },
          "table": "lookupTable",
          "keyColumn": "country_id",
          "valueColumn": "country_name",
          "tsColumn": "timeColumn"
        },
        "firstCacheTimeout": 120000,
        "injective": true
      }
    },
    "site_id_customer1": {
      "version": "v0",
      "lookupExtractorFactory": {
        "type": "map",
        "map": {
          "847632": "Internal Use Only"
        }
      }
    },
    "site_id_customer2": {
      "version": "v0",
      "lookupExtractorFactory": {
        "type": "map",
        "map": {
          "AHF77": "Home"
        }
      }
    }
  },
  "realtime_customer1": {
    "country_code": {
      "version": "v0",
      "lookupExtractorFactory": {
        "type": "map",
        "map": {
          "77483": "United States"
        }
      }
    },
    "site_id_customer1": {
      "version": "v0",
      "lookupExtractorFactory": {
        "type": "map",
        "map": {
          "847632": "Internal Use Only"
        }
      }
    }
  },
  "realtime_customer2": {
    "country_code": {
      "version": "v0",
      "lookupExtractorFactory": {
        "type": "map",
        "map": {
          "77483": "United States"
        }
      }
    },
    "site_id_customer2": {
      "version": "v0",
      "lookupExtractorFactory": {
        "type": "map",
        "map": {
          "AHF77": "Home"
        }
      }
    }
  }
}
```

All entries in the map will UPDATE existing entries. No entries will be deleted.

## Update Lookup
A `POST` to a particular lookup extractor factory via `/druid/coordinator/v1/lookups/config/{tier}/{id}` will update that specific extractor factory.

For example, a post to `/druid/coordinator/v1/lookups/config/realtime_customer1/site_id_customer1` might contain the following:

```json
{
  "version": "v1",
  "lookupExtractorFactory": {
    "type": "map",
    "map": {
      "847632": "Internal Use Only"
    }
  }
}
```

This will replace the `site_id_customer1` lookup in the `realtime_customer1` with the definition above.

## Get Lookup
A `GET` to a particular lookup extractor factory is accomplished via `/druid/coordinator/v1/lookups/{tier}/{id}`

Using the prior example, a `GET` to `/druid/coordinator/v1/lookups/config/realtime_customer2/site_id_customer2` should return

```json
{
  "version": "v1",
  "lookupExtractorFactory": {
    "type": "map",
    "map": {
      "AHF77": "Home"
    }
  }
}
```

## Delete Lookup
A `DELETE` to `/druid/coordinator/v1/lookups/config/{tier}/{id}` will remove that lookup from the cluster.

## List tier names
A `GET` to `/druid/coordinator/v1/lookups/config` will return a list of known tier names in the dynamic configuration.
To discover a list of tiers currently active in the cluster **instead of** ones known in the dynamic configuration, the parameter `discover=true` can be added as per `/druid/coordinator/v1/lookups?discover=true`.

## List lookup names
A `GET` to `/druid/coordinator/v1/lookups/config/{tier}` will return a list of known lookup names for that tier.

# Additional API related to status of configured lookups
These end points can be used to get the propagation status of configured lookups to lookup nodes such as Historicals.

## List load status of all lookups
`GET /druid/coordinator/v1/lookups/status` with optional query parameter `detailed`.

## List load status of lookups in a tier
`GET /druid/coordinator/v1/lookups/status/{tier}` with optional query parameter `detailed`.

## List load status of single lookup
`GET /druid/coordinator/v1/lookups/status/{tier}/{lookup}` with optional query parameter `detailed`.

## List lookup state of all nodes
`GET /druid/coordinator/v1/lookups/nodeStatus` with optional query parameter `discover` to discover tiers from zookeeper or configured lookup tiers are listed.

## List lookup state of nodes in a tier
`GET /druid/coordinator/v1/lookups/nodeStatus/{tier}`

## List lookup state of single node
`GET /druid/coordinator/v1/lookups/nodeStatus/{tier}/{host:port}`

# Internal API

The Peon, Router, Broker, and Historical nodes all have the ability to consume lookup configuration.
There is an internal API these nodes use to list/load/drop their lookups starting at `/druid/listen/v1/lookups`.
These follow the same convention for return values as the cluster wide dynamic configuration. Following endpoints
can be used for debugging purposes but not otherwise.

## Get Lookups

A `GET` to the node at `/druid/listen/v1/lookups` will return a json map of all the lookups currently active on the node.
The return value will be a json map of the lookups to their extractor factories.

```json
{
  "site_id_customer2": {
    "version": "v1",
    "lookupExtractorFactory": {
      "type": "map",
      "map": {
        "AHF77": "Home"
      }
    }
  }
}
```

## Get Lookup

A `GET` to the node at `/druid/listen/v1/lookups/some_lookup_name` will return the LookupExtractorFactory for the lookup identified by `some_lookup_name`.
The return value will be the json representation of the factory.

```json
{
  "version": "v1",
  "lookupExtractorFactory": {
    "type": "map",
    "map": {
      "AHF77": "Home"
    }
  }
}
```

# Configuration
See [Lookups Dynamic Configuration](../configuration/index.html#lookups-dynamic-configuration) for Coordinator configuration.

To configure a Broker / Router / Historical / Peon to announce itself as part of a lookup tier, use the `druid.zk.paths.lookupTier` property.

|Property | Description | Default |
|---------|-------------|---------|
|`druid.lookup.lookupTier`| The tier for **lookups** for this node. This is independent of other tiers.|`__default`|
|`druid.lookup.lookupTierIsDatasource`|For some things like indexing service tasks, the datasource is passed in the runtime properties of a task. This option fetches the tierName from the same value as the datasource for the task. It is suggested to only use this as Peon options for the indexing service, if at all. If true, `druid.lookup.lookupTier` MUST NOT be specified|`"false"`|

To configure the behavior of the dynamic configuration manager, use the following properties on the Coordinator:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.manager.lookups.hostTimeout`|Timeout (in ms) PER HOST for processing request|`2000`(2 seconds)|
|`druid.manager.lookups.allHostTimeout`|Timeout (in ms) to finish lookup management on all the nodes.|`900000`(15 mins)|
|`druid.manager.lookups.period`|How long to pause between management cycles|`120000`(2 mins)|
|`druid.manager.lookups.threadPoolSize`|Number of service nodes that can be managed concurrently|`10`|

## Saving configuration across restarts

It is possible to save the configuration across restarts such that a node will not have to wait for Coordinator action to re-populate its lookups. To do this the following property is set:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.lookup.snapshotWorkingDir`|Working path used to store snapshot of current lookup configuration, leaving this property null will disable snapshot/bootstrap utility|null|
|`druid.lookup.enableLookupSyncOnStartup`|Enable the lookup synchronization process with Coordinator on startup. The queryable nodes will fetch and load the lookups from the Coordinator instead of waiting for the Coordinator to load the lookups for them. Users may opt to disable this option if there are no lookups configured in the cluster.|true|
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
 
## Druid version 0.10.0 to 0.10.1 upgrade/downgrade
Overall druid cluster lookups configuration is persisted in metadata store and also individual lookup nodes optionally persist a snapshot of loaded lookups on disk.
If upgrading from druid version 0.10.0 to 0.10.1, then migration for all persisted metadata is handled automatically.
If downgrading from 0.10.1 to 0.9.0 then lookups updates done via Coordinator while 0.10.1 was running, would be lost.

 
