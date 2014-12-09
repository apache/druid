---
layout: doc_page
---
Historical Node Configuration
=============================
For general Historical Node information, see [here](Historical.html).


Runtime Configuration
---------------------

The historical module uses several of the default modules in [Configuration](Configuration.html) and has a few configs of its own.

#### Local Cache

|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.historical.cache.useCache`|`true`,`false`|Allow cache to be used. Cache will NOT be used unless this is set.|`false`|
|`druid.historical.cache.populateCache`|`true`,`false`|Allow cache to be populated. Cache will NOT be populated unless this is set.|`false`|
|`druid.historical.cache.unCacheable`|All druid query types|Do not attempt to cache queries whose types are in this array|`["groupBy","select"]`|
|`druid.historical.cache.numBackgroundThreads`|Non-negative integer|Number of background threads in the thread pool to use for eventual-consistency caching results if caching is used. It is recommended to set this value greater or equal to the number of processing threads. To force caching to execute in the same thread as the query (query results are blocked on caching completion), use a thread count of 0. Setups who use a Druid backend in programatic settings (sub-second re-querying) should consider setting this to 0 to prevent eventual consistency from biting overall performance in the ass. If this is you, please experiment to find out what setting works best.|`0`|

