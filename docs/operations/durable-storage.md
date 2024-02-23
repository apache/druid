---
id: durable-storage
title: "Durable storage for the multi-stage query engine"
sidebar_label: "Durable storage"
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

You can use durable storage to improve querying from deep storage and SQL-based ingestion.

> Note that S3, Azure and Google are all supported as durable storage locations.

Durable storage for queries from deep storage provides a location where you can write the results of deep storage queries to. Durable storage for SQL-based ingestion is used to temporarily house intermediate files, which can improve reliability.

Enabling durable storage also enables the use of local disk to store temporary files, such as the intermediate files produced
while sorting the data. Tasks will use whatever has been configured for their temporary usage as described in [Configuring task storage sizes](../ingestion/tasks.md#configuring-task-storage-sizes).
If the configured limit is too low, Druid may throw the error, `NotEnoughTemporaryStorageFault`.

## Enable durable storage

To enable durable storage, you need to set the following common service properties:

```
druid.msq.intermediate.storage.enable=true
druid.msq.intermediate.storage.tempDir=/path/to/your/temp/dir

# Include these configs if you're using S3
# druid.msq.intermediate.storage.type=s3
# druid.msq.intermediate.storage.bucket=YOUR_BUCKET

# Include these configs if you're using Azure Blob Storage
# druid.msq.intermediate.storage.type=azure
# druid.sq.intermediate.storage.container=YOUR_CONTAINER

druid.msq.intermediate.storage.prefix=YOUR_PREFIX
```

For detailed information about these and additional settings related to durable storage, see [Durable storage configurations](../multi-stage-query/reference.md#durable-storage-configurations).


## Use durable storage for SQL-based ingestion queries

When you run a query, include the context parameter `durableShuffleStorage` and set it to `true`.

For queries where you want to use fault tolerance for workers,  set `faultTolerance` to `true`, which automatically sets `durableShuffleStorage` to `true`.

## Use durable storage for queries from deep storage

Depending on the size of the results you're expecting, saving the final results for queries from deep storage to durable storage might be needed.

By default, Druid saves the final results for queries from deep storage to task reports. Generally, this is acceptable for smaller result sets but may lead to timeouts for larger result sets. 

When you run a query, include the context parameter `selectDestination` and set it to `DURABLESTORAGE`:

```json
    "context":{
        ...
        "selectDestination": "DURABLESTORAGE"
    }
```

You can also write intermediate results to durable storage (`durableShuffleStorage`) for better reliability. The location where workers write intermediate results is different than the location where final results get stored. This means that durable storage for results can be enabled even if you don't write intermediate results to durable storage. 

If you write the results for queries from deep storage to durable storage, the results are cleaned up when the task is removed from the metadata store. 

## Durable storage clean up

To prevent durable storage from getting filled up with temporary files in case the tasks fail to clean them up, a periodic
cleaner can be scheduled to clean the directories corresponding to which there isn't a controller task running. It utilizes
the storage connector to work upon the durable storage. The durable storage location should only be utilized to store the output
for the cluster's MSQ tasks. If the location contains other files or directories, then they will get cleaned up as well.

Use `druid.msq.intermediate.storage.cleaner.enabled` and `druid.msq.intermediate.storage.cleaner.delaySeconds` to configure the cleaner. For more information, see [Durable storage configurations](../multi-stage-query/reference.md#durable-storage-configurations).

Note that if you choose to write query results to durable storage,the results are cleaned up when the task is removed from the metadata store.

