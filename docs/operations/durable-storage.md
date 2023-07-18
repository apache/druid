---
id: durable-storage
title: "Durable storage for the multi-stage query engine"
sidebar_label: "Durable storage"
---

You can use durable storage to improve querying from deep storage and SQL-based ingestion.

> Note that only S3 is supported as a durable storage location.

Durable storage for queries from deep storage provides a location where you can write the results of deep storage queries to. Durable storage for SQL-based ingestion is used to temporarily house intermediate files, which can improve reliability.

## Enable durable storage

To enable durable storage, you need to set the following common service properties:

```
druid.msq.intermediate.storage.enable=true
druid.msq.intermediate.storage.type=s3
druid.msq.intermediate.storage.bucket=YOUR_BUCKET
druid.msq.intermediate.storage.prefix=YOUR_PREFIX
druid.msq.intermediate.storage.tempDir=/path/to/your/temp/dir
```

For detailed information about the settings related to durable storage, see [Durable storage configurations](../multi-stage-query/reference.md#durable-storage-configurations).


## Use durable storage for SQL-based ingestion queries

When you run a query, include the context parameter `durableShuffleStorage` and set it to `true`.

For queries where you want to use fault tolerance for workers,  set `faultTolerance` to `true`, which automatically sets `durableShuffleStorage` to `true`.

## Use durable storage for queries from deep storage

When you run a query, include the context parameter `selectDestination` and set it to `DURABLE_STORAGE`. This context parameter configures queries from deep storage to write their results to durable storage.

## Durable storage clean up

To prevent durable storage from getting filled up with temporary files in case the tasks fail to clean them up, a periodic
cleaner can be scheduled to clean the directories corresponding to which there isn't a controller task running. It utilizes
the storage connector to work upon the durable storage. The durable storage location should only be utilized to store the output
for cluster's MSQ tasks. If the location contains other files or directories, then they will get cleaned up as well.

Enabling durable storage also enables the use of local disk to store temporary files, such as the intermediate files produced
by the super sorter.  Tasks will use whatever has been configured for their temporary usage as described in [Configuring task storage sizes](../ingestion/tasks.md#configuring-task-storage-sizes)
If the configured limit is too low, `NotEnoughTemporaryStorageFault` may be thrown.