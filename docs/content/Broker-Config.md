---
layout: doc_page
---
Broker Node Configuration
=========================
For general Broker Node information, see [here](Broker.html).

Runtime Configuration
---------------------

The broker module uses several of the default modules in [Configuration](Configuration.html) and has the following set of configurations as well:

|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.broker.balancer.type`|`random`, `connectionCount`|Determines how the broker balances connections to historical nodes. `random` choose randomly, `connectionCount` picks the node with the fewest number of active connections to|`random`|
|`druid.broker.select.tier`|`highestPriority`, `lowestPriority`, `custom`|If segments are cross-replicated across tiers in a cluster, you can tell the broker to prefer to select segments in a tier with a certain priority.|`highestPriority`|
|`druid.broker.select.tier.custom.priorities`|`An array of integer priorities.`|Select servers in tiers with a custom priority list.|None|
|`druid.broker.cache.type`|`local`, `memcached`|The type of cache to use for queries.|`local`|
|`druid.broker.cache.unCacheable`|All druid query types|All query types to not cache.|["groupBy", "select"]|


#### Local Cache

|Property|Description|Default|
|--------|-----------|-------|
|`druid.broker.cache.sizeInBytes`|Maximum cache size in bytes. Zero disables caching.|0|
|`druid.broker.cache.initialSize`|Initial size of the hashtable backing the cache.|500000|
|`druid.broker.cache.logEvictionCount`|If non-zero, log cache eviction every `logEvictionCount` items.|0|

#### Memcache

|Property|Description|Default|
|--------|-----------|-------|
|`druid.broker.cache.expiration`|Memcached [expiration time](https://code.google.com/p/memcached/wiki/NewCommands#Standard_Protocol).|2592000 (30 days)|
|`druid.broker.cache.timeout`|Maximum time in milliseconds to wait for a response from Memcached.|500|
|`druid.broker.cache.hosts`|Command separated list of Memcached hosts `<host:port>`.|none|
|`druid.broker.cache.maxObjectSize`|Maximum object size in bytes for a Memcached object.|52428800 (50 MB)|
|`druid.broker.cache.memcachedPrefix`|Key prefix for all keys in Memcached.|druid|
