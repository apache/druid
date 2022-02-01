---
id: oak-incremental-index
title: "Oak Incremental Index"
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


## Overview
This extension improves the CPU and memory efficiency of Druid's ingestion.
The same performance is achieved with 60% less memory and 50% less CPU time.
Ingestion-throughput was nearly doubled with the same memory budget,
and throughput was increased by 75% with the same CPU budget.
Full details of the experimental setup and results are available [here](https://github.com/liran-funaro/druid/wiki/Evaluation).

It uses [OakMap open source library](https://github.com/yahoo/Oak) to store the keys and values outside the JVM heap.

#### Main enhancements provided by this extension:
1. **Resource efficiency**: Use less CPU and RAM for the same performance
2. **Performance**: Improving performance by using more concurrent workers with the same resource allocation

### Installation
Use the [pull-deps](../../operations/pull-deps.md) tool included with Druid to install this [extension](../../development/extensions.md#community-extensions) on all Druid middle-manager nodes.

```bash
java -classpath "<your_druid_dir>/lib/*" org.apache.druid.cli.Main tools pull-deps -c org.apache.druid.extensions.contrib:oak-incremental-index
```

### Enabling
After installation, to enable this extension, just add `oak-incremental-index` to `druid.extensions.loadList` in the 
middle-manager's `runtime.properties` file and then restart its nodes.

For example:
```bash
druid.extensions.loadList=["oak-incremental-index"]
```

### Resource Configurations
Since Oak allocates its keys/values directly, it is not subject to the JVM's on/off-heap limitations.
Despite this, we have to respect runtime resource limits if they aren't specified by the ingestion specs.
To ensure that the Oak index doesn't use more resources than available, we use the same default `maxBytesInMemory` as the on-heap index, i.e., 1/6 of the maximal heap size.
It assumes that the middle-manager is configured correctly according to the machine's resources.

As the middle-manager resource configuration typically consumes all the instance memory, we need to reduce the heap size in order to accommodate the Oak index.
This can be achieved by ensuring the minimal heap size of the ingestion task is 2/3 (or less) of the current maximal heap size.
Generally, smaller heap sizes lead to greater resource efficiency.

If, for example, the middle-manager's `runtime.properties` file had the following configuration:
```properties
druid.indexer.runner.javaOpts=-Xms3g -Xmx3g
```
It is suggested to change it to:
```properties
druid.indexer.runner.javaOpts=-Xms2g -Xmx3g
```
If you'd like to maximize resource efficiency, try:
```properties
druid.indexer.runner.javaOpts=-Xms256m -Xmx3g
```

## Usage
Some workloads may benefit from this extension, but others may perform better with the on-heap implementation 
(despite consuming more RAM than the Oak implementation).
Hence, the user can choose which incremental-index implementation to use, depending on its needs.

By default, the built-in on-heap incremental index is used.
After enabling the extension, the user must modify the [`ingestion spec`](../../ingestion/ingestion-spec.md) 
to use Oak incremental index.
Namely, modify [`appendable index spec`](#appendableindexspec) 
under [`tuning config`](../../ingestion/ingestion-spec.md#tuningconfig) as follows.

### `spec`
All properties in the [`ingestion spec`](../../ingestion/ingestion-spec.md) remains as before.
However, the user need to pay attention to the following configurations under [`tuning config`](../../ingestion/ingestion-spec.md#tuningconfig).

| Field                            | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | Default                    |
|----------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------|
| `appendableIndexSpec`            | Tune which incremental index to use. See table [below](#appendableindexspec) for more information.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | See table below            |
| `maxRowsInMemory`                | The maximum number of records to store in memory before persisting to disk. Note that this is the number of rows post-rollup, and so it may not be equal to the number of input records. Ingested records will be persisted to disk when either `maxRowsInMemory` or `maxBytesInMemory` are reached (whichever happens first), so they both determines the memory usage of the ingestion task.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | `1000000`                  |
| `maxBytesInMemory`               | The maximum aggregate size of records, in bytes, to store in memory before persisting. For oak-incremental-index, this accurately limits the memory usage, but for the default on-heap implementation, this is based on a rough estimate of memory usage. Ingested records will be persisted to disk when either `maxRowsInMemory` or `maxBytesInMemory` are reached (whichever happens first), so they both determines the memory usage of the ingestion task. `maxBytesInMemory` also includes heap usage of artifacts created from intermediary persists. This means that after every persist, the amount of `maxBytesInMemory` until next persist will decrease, and task will fail when the sum of bytes of all intermediary persisted artifacts exceeds `maxBytesInMemory`.<br /><br />Setting `maxBytesInMemory` to `-1` disables this check, meaning Druid will rely entirely on `maxRowsInMemory` to control memory usage. Setting it to zero (default) means the default value will be used (see on the right).<br /><br />Note that for the on-heap implementation, the estimate of memory usage is designed to be an overestimate, and can be especially high when using complex ingest-time aggregators, including sketches. If this causes your indexing workloads to persist to disk too often, you can set `maxBytesInMemory` to `-1` and rely on `maxRowsInMemory` instead.<br /><br />When using this extension, the `maxBytesInMemory` should be set according to your machine's memory limit to ensure Druid doesn't run into allocation problems. Specifically, take the available memory on your machine after deducting the JVM heap space, the off-heap buffers space, and memory used for other process on the machine, and then divide by two so that we'll have room for both the ingested index and the flushed one. | one-sixth of JVM heap size |
| `skipBytesInMemoryOverheadCheck` | The calculation of `maxBytesInMemory` takes into account overhead objects created during ingestion and each intermediate persist. Setting this to true can exclude the bytes of these overhead objects from `maxBytesInMemory` check.<br /><br />Note that the oak incremental index is stored in a different memory area than the overhead objects. As a result, it doesn't make sense to subject them to the same restrictions. Thus, setting this value to `true` is recommended for this extension.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | false                      |
| Other properties                 | See [`tuning config`](../../ingestion/ingestion-spec.md#tuningconfig) for more information.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      ||

### `appendableIndexSpec`
The user can choose between the built-in on-heap incremental index and this extension.

| Field  | Description                                                                                                                                                      | Default  |
|--------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|
| `type` | Each in-memory index has its own tuning type code. You must specify the type code that matches your in-memory index. Available options are `onheap` and `oak`.   | `onheap` |

On-heap implementations have no additional parameters. The following are the parameters for this extension.
For most use cases, the defaults work well, but if there are issues, these can be adjusted.

| Field                  | Description                                                                                                                                                                                                                                                                                           | Default |
|------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|
| `type`                 | `oak`                                                                                                                                                                                                                                                                                                 |         |
| `oakBlockSize`         | OakMap stores its data outside the JVM heap in memory blocks. Larger blocks consolidate allocations and reduce overhead, but can also waste more memory if not fully utilized. The default has a reasonable balance between performance and memory usage when tested in a batch ingestion scenario.   | `8MiB`  |
| `oakMaxMemoryCapacity` | OakMap maintains its memory blocks with an internal data-structure. Structure size is roughly `oakMaxMemoryCapacity/oakBlockSize`. We set this number to a large enough yet reasonable value so that this structure does not consume too much memory.                                                 | `32GiB` |
| `oakChunkMaxItems`     | OakMap maintains its entries in small chunks. Using larger chunks reduces the number of on-heap objects, but may incur more overhead when balancing the entries between chunks. The default showed the best performance in batch ingestion scenarios.                                                 | `256`   |

# Example
The following tuning configuration is recommended for this extension:
```json
"tuningConfig": {
    <other ingestion-method-specific properties>,
    "maxRowsInMemory": 1000000,
    "maxBytesInMemory": 0,
    "skipBytesInMemoryOverheadCheck": true,
    "appendableIndexSpec": {
      "type": "oak"
    }
}
```

The following is an example tuning configuration that sets all the common properties:
```json
"tuningConfig": {
    <other ingestion-method-specific properties>,
    "maxRowsInMemory": 1000000,
    "maxBytesInMemory": 0,
    "skipBytesInMemoryOverheadCheck": true,
    "appendableIndexSpec": {
        "type": "oak",
        "oakMaxMemoryCapacity": 34359738368,
        "oakBlockSize": 8388608,
        "oakChunkMaxItems": 256
    }
}
```

