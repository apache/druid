---
layout: doc_page
---

# Lookups

<div class="note caution">
Lookups are an <a href="../development/experimental.html">experimental</a> feature.
</div>

Lookups are a concept in Druid where dimension values are (optionally) replaced with new values. 
See [dimension specs](../querying/dimensionspecs.html) for more information. For the purpose of these documents, 
a "key" refers to a dimension value to match, and a "value" refers to its replacement. 
So if you wanted to rename `appid-12345` to `Super Mega Awesome App` then the key would be `appid-12345` and the value 
would be `Super Mega Awesome App`. 

It is worth noting that lookups support use cases where keys map to unique values (injective) such as a country code and 
a country name, and also supports use cases where multiple IDs map to the same value, e.g. multiple app-ids belonging to 
a single account manager.

Lookups do not have history. They always use the current data. This means that if the chief account manager for a 
particular app-id changes, and you issue a query with a lookup to store the app-id to account manager relationship, 
it will return the current account manager for that app-id REGARDLESS of the time range over which you query.

If you require data time range sensitive lookups, such a use case is not currently supported dynamically at query time, 
and such data belongs in the raw denormalized data for use in Druid.

Very small lookups (count of keys on the order of a few dozen to a few hundred) can be passed at query time as a "map" 
lookup as per [dimension specs](../querying/dimensionspecs.html).

For static lookups defined in `runtime.properties` rather than embedded in the query, please look at the experimental [namespaced lookup extension](../development/extensions-core/namespaced-lookup.html).

For other additional lookups, please see our [extensions list](../development/extensions.html).

Dynamic configuration
---------------------
<div class="note caution">
Dynamic lookup configuration is an <a href="../development/experimental.html">experimental</a> feature.
</div>
The following documents the behavior of the cluster-wide config which is accessible through the coordinator.
The configuration is propagated through the concept of "tier" of servers.
A "tier" is defined as a group of services which should receive a set of lookups.
For example, you might have all historicals be part of `__default`, and Peons be part of individual tiers for the datasources they are tasked with.
The tiers for lookups are completely independent of historical tiers.

These configs are accessed using JSON through the following URI template

```
http://<COORDINATOR_IP>:<PORT>/druid/coordinator/v1/lookups/{tier}/{id}
```

All URIs below are assumed to have `http://<COORDINATOR_IP>:<PORT>` prepended.

If you have NEVER configured lookups before, you MUST post an empty json object `{}` to `/druid/coordinator/v1/lookups` to initialize the configuration.

These endpoints will return one of the following results:

* 404 if the resource is not found
* 400 if there is a problem in the formatting of the request
* 202 if the request was accepted asynchronously (`POST` and `DELETE`)
* 200 if the request succeeded (`GET` only)

## Configuration propagation behavior
The configuration is propagated to the query serving nodes (broker / router / peon / historical) by the coordinator.
The query serving nodes have an internal API for managing `POST`/`GET`/`DELETE` of lookups.
The coordinator periodically checks the dynamic configuration for changes and, when it detects a change it does the following:

1. Post all lookups for a tier to all Druid nodes within that tier.
2. Delete lookups from a tier which were dropped between the prior configuration values and this one.

If there is no configuration change, the coordinator checks for any nodes which might be new since the last time it propagated lookups and adds all lookups for that node (assuming that node's tier has lookups).
If there are errors while trying to add or update configuration on a node, that node is temporarily skipped until the next management period. The next management period the update will attempt to be propagated again.
If there is an error while trying to delete a lookup from a node (or if a node is down when the coordinator is propagating the config), the delete is not attempted again. In such a case it is possible that a node has lookups that are no longer managed by the coordinator.

## Bulk update
Lookups can be updated in bulk by posting a JSON object to `/druid/coordinator/v1/lookups`. The format of the json object is as follows:


```json
{
    "tierName": {
        "lookupExtractorFactoryName": {
          "someExtractorField": "someExtractorValue"
        }
    }
}
```

So a config might look something like:
```json
{
    "__default": {
        "country_code": {
          "type": "simple_json",
          "uri": "http://some.host.com/codes.json"
        },
        "site_id": {
            "type": "confidential_jdbc",
            "auth": "/etc/jdbc.internal",
            "table": "sites",
            "key": "site_id",
            "value": "site_name"
        },
        "site_id_customer1": {
            "type": "confidential_jdbc",
            "auth": "/etc/jdbc.customer1",
            "table": "sites",
            "key": "site_id",
            "value": "site_name"
        },
        "site_id_customer2": {
            "type": "confidential_jdbc",
            "auth": "/etc/jdbc.customer2",
            "table": "sites",
            "key": "site_id",
            "value": "site_name"
        }
    },
    "realtime_customer1": {
        "country_code": {
          "type": "simple_json",
          "uri": "http://some.host.com/codes.json"
        },
        "site_id_customer1": {
            "type": "confidential_jdbc",
            "auth": "/etc/jdbc.customer1",
            "table": "sites",
            "key": "site_id",
            "value": "site_name"
        }
    },
    "realtime_customer2": {
        "country_code": {
          "type": "simple_json",
          "uri": "http://some.host.com/codes.json"
        },
        "site_id_customer2": {
            "type": "confidential_jdbc",
            "auth": "/etc/jdbc.customer2",
            "table": "sites",
            "key": "site_id",
            "value": "site_name"
        }
    }
}
```

All entries in the map will UPDATE existing entries. No entries will be deleted.

## Update Lookup
A `POST` to a particular lookup extractor factory via `/druid/coordinator/v1/lookups/{tier}/{id}` will update that specific extractor factory.

For example, a post to `/druid/coordinator/v1/lookups/realtime_customer1/site_id_customer1` might contain the following:

```json
{
    "type": "confidential_jdbc",
    "auth": "/etc/jdbc.customer1",
    "table": "sites_updated",
    "key": "site_id",
    "value": "site_name"
}
```

This will replace the `site_id_customer1` lookup in the `realtime_customer1` with the definition above.

## Get Lookup
A `GET` to a particular lookup extractor factory is accomplished via `/druid/coordinator/v1/lookups/{tier}/{id}`

Using the prior example, a `GET` to `/druid/coordinator/v1/lookups/realtime_customer2/site_id_customer2` should return

```json
{
    "type": "confidential_jdbc",
    "auth": "/etc/jdbc.customer2",
    "table": "sites",
    "key": "site_id",
    "value": "site_name"
}
```

## Delete Lookup
A `DELETE` to `/druid/coordinator/v1/lookups/{tier}/{id}` will remove that lookup from the cluster.

## List tier names
A `GET` to `/druid/coordinator/v1/lookups` will return a list of known tier names in the dynamic configuration.
To discover a list of tiers currently active in the cluster **instead of** ones known in the dynamic configuration, the parameter `discover=true` can be added as per `/druid/coordinator/v1/lookups?discover=true`.

## List lookup names
A `GET` to `/druid/coordinator/v1/lookups/{tier}` will return a list of known lookup names for that tier.

# Internal API

The Peon, Router, Broker, and Historical nodes all have the ability to consume lookup configuration.
There is an internal API these nodes use to list/load/drop their lookups starting at `/druid/listen/v1/lookups`.
These follow the same convention for return values as the cluster wide dynamic configuration.
Usage of these endpoints is quite advanced and not recommended for most users.
The endpoints are as follows:

## Get Lookups

A `GET` to the node at `/druid/listen/v1/lookups` will return a json map of all the lookups currently active on the node.
The return value will be a json map of the lookups to their extractor factories.

```json

{
    "some_lookup_name": {
        "type": "simple_json",
        "uri": "http://some.host.com/codes.json"
    }
}

```

## Get Lookup

A `GET` to the node at `/druid/listen/v1/lookups/some_lookup_name` will return the LookupExtractorFactory for the lookup identified by `some_lookup_name`.
The return value will be the json representation of the factory.

```json
{
    "type": "simple_json",
    "uri": "http://some.host.com/codes.json"
}
```

## Bulk Add or Update Lookups

A `POST` to the node at `/druid/listen/v1/lookups` of a JSON map of lookup names to LookupExtractorFactory will cause the service to add or update its lookups.
The return value will be a JSON map in the following format:

```json
{
    "status": "accepted",
    "failedUpdates": {}
}

```

If a lookup cannot be started, or is left in an undefined state, the lookup in error will be returned in the `failedUpdates` field as per:

```json
{
    "status": "accepted",
    "failedUpdates": {
        "country_code": {
            "type": "simple_json",
            "uri": "http://some.host.com/codes.json"
        }
    }
}

```

The `failedUpdates` field of the return value should be checked if a user is wanting to assure that every update succeeded.

## Add or Update Lookup

A `POST` to the node at `/druid/listen/v1/lookups/some_lookup_name` will behave very similarly to a bulk update.

If `some_lookup_name` is desired to have the LookupExtractorFactory definition of 

```json
{
    "type": "simple_json",
    "uri": "http://some.host.com/codes.json"
}
```

Then a post to `/druid/listen/v1/lookups/some_lookup_name` will behave the same as a `POST` to `/druid/listen/v1/lookups` of

```json

{
    "some_lookup_name": {
        "type": "simple_json",
        "uri": "http://some.host.com/codes.json"
    }
}

```

## Remove a Lookup
A `DELETE` to `/druid/listen/v1/lookups/some_lookup_name` will remove that lookup from the node. Success will reflect the ID.

# Configuration
See the [coordinator configuration guilde](../configuration/coordinator.html) for coordinator configuration

To configure a Broker / Router / Historical / Peon to announce itself as part of a lookup tier, use the `druid.zk.paths.lookupTier` property.

|Property | Description | Default |
|---------|-------------|---------|
|`druid.lookup.tierName`| The tier for **lookups** for this node. This is independent of other tiers.|`__default`|
