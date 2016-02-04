---
layout: doc_page
---
# Query Caching

Druid supports query result caching through an LRU cache. Results are stored on a per segment basis, along with the 
parameters of a given query. This allows Druid to return final results based partially on segment results in the cache and partially 
on segment results from scanning historical/real-time segments.

Segment results can be stored in a local heap cache or in an external distributed key/value store. Segment query caches 
can be enabled at either the Historical and Broker level (it is not recommended to enable caching on both).

## Query caching on Brokers

Enabling caching on the broker can yield faster results than if query caches were enabled on Historicals for small clusters. This is 
the recommended setup for smaller production clusters (< 20 servers). Take note that when caching is enabled on the Broker, 
results from Historicals are returned on a per segment basis, and Historicals will be able to do any local result merging.

## Query caching on Historicals

Larger production clusters should enable caching only on the Historicals to avoid having to use Brokers to merge all query 
results. Enabling caching on the Historicals enables the Historicals do their own local result merging, and puts less strain 
on the Brokers.
