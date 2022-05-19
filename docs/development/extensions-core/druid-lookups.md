---
id: druid-lookups
title: "Cached Lookup Module"
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


> Please note that this is an experimental module and the development/testing still at early stage. Feel free to try it and give us your feedback.

## Description
This Apache Druid module provides a per-lookup caching mechanism for JDBC data sources.
The main goal of this cache is to speed up the access to a high latency lookup sources and to provide a caching isolation for every lookup source.
Thus user can define various caching strategies or and implementation per lookup, even if the source is the same.
This module can be used side to side with other lookup module like the global cached lookup module.

To use this Apache Druid extension, [include](../extensions.md#loading-extensions) `druid-lookups-cached-single` in the extensions load list.

> If using JDBC, you will need to add your database's client JAR files to the extension's directory.
> For Postgres, the connector JAR is already included.
> See the MySQL extension documentation for instructions to obtain [MySQL](./mysql.md#installing-the-mysql-connector-library) or [MariaDB](./mysql.md#alternative-installing-the-mariadb-connector-library) connector libraries.
> Copy or symlink the downloaded file to `extensions/druid-lookups-cached-single` under the distribution root directory.

## Architecture
Generally speaking this module can be divided into two main component, namely, the data fetcher layer and caching layer.

### Data Fetcher layer

First part is the data fetcher layer API `DataFetcher`, that exposes a set of fetch methods to fetch data from the actual Lookup dimension source.
For instance `JdbcDataFetcher` provides an implementation of `DataFetcher` that can be used to fetch key/value from a RDBMS via JDBC driver.
If you need new type of data fetcher, all you need to do, is to implement the interface `DataFetcher` and load it via another druid module.
### Caching layer

This extension comes with two different caching strategies. First strategy is a poll based and the second is a load based.
#### Poll lookup cache

The poll strategy cache strategy will fetch and swap all the pair of key/values periodically from the lookup source.
Hence, user should make sure that the cache can fit all the data.
The current implementation provides 2 type of poll cache, the first is on-heap (uses immutable map), while the second uses MapDB based off-heap map.
User can also implement a different lookup polling cache by implementing `PollingCacheFactory` and `PollingCache` interfaces.

#### Loading lookup
Loading cache strategy will load the key/value pair upon request on the key it self, the general algorithm is load key if absent.
Once the key/value  pair is loaded eviction will occur according to the cache eviction policy.
This module comes with two loading lookup implementation, the first is on-heap backed by a Guava cache implementation, the second is MapDB off-heap implementation.
Both implementations offer various eviction strategies.
Same for Loading cache, developer can implement a new type of loading cache by implementing `LookupLoadingCache` interface.

## Configuration and Operation:


### Polling Lookup

**Note that the current implementation of `offHeapPolling` and `onHeapPolling` will create two caches one to lookup value based on key and the other to reverse lookup the key from value**

|Field|Type|Description|Required|default|
|-----|----|-----------|--------|-------|
|dataFetcher|JSON object|Specifies the lookup data fetcher type for fetching data|yes|null|
|cacheFactory|JSON Object|Cache factory implementation|no |onHeapPolling|
|pollPeriod|Period|polling period |no |null (poll once)|


#####   Example of Polling On-heap Lookup
This example demonstrates a polling cache that will update its on-heap cache every 10 minutes

```json
{
    "type":"pollingLookup",
   "pollPeriod":"PT10M",
   "dataFetcher":{ "type":"jdbcDataFetcher", "connectorConfig":"jdbc://mysql://localhost:3306/my_data_base", "table":"lookup_table_name", "keyColumn":"key_column_name", "valueColumn": "value_column_name"},
   "cacheFactory":{"type":"onHeapPolling"}
}

```

#####   Example Polling Off-heap Lookup
This example demonstrates an off-heap lookup that will be cached once and never swapped `(pollPeriod == null)`

```json
{
    "type":"pollingLookup",
   "dataFetcher":{ "type":"jdbcDataFetcher", "connectorConfig":"jdbc://mysql://localhost:3306/my_data_base", "table":"lookup_table_name", "keyColumn":"key_column_name", "valueColumn": "value_column_name"},
   "cacheFactory":{"type":"offHeapPolling"}
}

```


### Loading lookup

|Field|Type|Description|Required|default|
|-----|----|-----------|--------|-------|
|dataFetcher|JSON object|Specifies the lookup data fetcher type  to use in order to fetch data|yes|null|
|loadingCacheSpec|JSON Object|Lookup cache spec implementation|yes |null|
|reverseLoadingCacheSpec|JSON Object| Reverse lookup cache  implementation|yes |null|


##### Example Loading On-heap Guava

Guava cache configuration spec.

|Field|Type|Description|Required|default|
|-----|----|-----------|--------|-------|
|concurrencyLevel|int|Allowed concurrency among update operations|no|4|
|initialCapacity|int|Initial capacity size|no |null|
|maximumSize|long| Specifies the maximum number of entries the cache may contain.|no |null (infinite capacity)|
|expireAfterAccess|long| Specifies the eviction time after last read in milliseconds.|no |null (No read-time-based eviction when set to null)|
|expireAfterWrite|long| Specifies the eviction time after last write in milliseconds.|no |null (No write-time-based eviction when set to null)|

```json
{
   "type":"loadingLookup",
   "dataFetcher":{ "type":"jdbcDataFetcher", "connectorConfig":"jdbc://mysql://localhost:3306/my_data_base", "table":"lookup_table_name", "keyColumn":"key_column_name", "valueColumn": "value_column_name"},
   "loadingCacheSpec":{"type":"guava"},
   "reverseLoadingCacheSpec":{"type":"guava", "maximumSize":500000, "expireAfterAccess":100000, "expireAfterWrite":10000}
}
```

##### Example Loading Off-heap MapDB

Off heap cache is backed by [MapDB](http://www.mapdb.org/) implementation. MapDB is using direct memory as memory pool, please take that into account when limiting the JVM direct memory setup.

|Field|Type|Description|Required|default|
|-----|----|-----------|--------|-------|
|maxStoreSize|double|maximal size of store in GiB, if store is larger entries will start expiring|no |0|
|maxEntriesSize|long| Specifies the maximum number of entries the cache may contain.|no |0 (infinite capacity)|
|expireAfterAccess|long| Specifies the eviction time after last read in milliseconds.|no |0 (No read-time-based eviction when set to null)|
|expireAfterWrite|long| Specifies the eviction time after last write in milliseconds.|no |0 (No write-time-based eviction when set to null)|


```json
{
   "type":"loadingLookup",
   "dataFetcher":{ "type":"jdbcDataFetcher", "connectorConfig":"jdbc://mysql://localhost:3306/my_data_base", "table":"lookup_table_name", "keyColumn":"key_column_name", "valueColumn": "value_column_name"},
   "loadingCacheSpec":{"type":"mapDb", "maxEntriesSize":100000},
   "reverseLoadingCacheSpec":{"type":"mapDb", "maxStoreSize":5, "expireAfterAccess":100000, "expireAfterWrite":10000}
}
```

### JDBC Data Fetcher

|Field|Type|Description|Required|default|
|-----|----|-----------|--------|-------|
|`connectorConfig`|JSON object|Specifies the database connection details. You can set `connectURI`, `user` and `password`. You can selectively allow JDBC properties in `connectURI`. See [JDBC connections security config](../../configuration/index.md#jdbc-connections-to-external-databases) for more details.|yes||
|`table`|string|The table name to read from.|yes||
|`keyColumn`|string|The column name that contains the lookup key.|yes||
|`valueColumn`|string|The column name that contains the lookup value.|yes||
|`streamingFetchSize`|int|Fetch size used in JDBC connections.|no|1000|
