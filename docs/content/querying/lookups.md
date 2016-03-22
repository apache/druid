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

For static lookups defined in `runtime.properties` rather than embedded in the query, please look at
the experimental [namespaced lookup extension](../development/extensions-core/namespaced-lookup.html).

For other additional lookups, please see our [extensions list](../development/extensions.html).
