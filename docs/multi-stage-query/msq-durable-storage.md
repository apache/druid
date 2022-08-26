---
id: durable-storage
title: Enable durage storage for mesh shuffle for the MSQ task engine
sidebar_label: Durable storage 
description: Using durable storage for mesh shuffle can improve the reliability of your SQL-based ingestion at the cost of some performance.
---

> SQL-based ingestion and the multi-stage query task engine are experimental features available starting in Druid 24.0. You can use it in place of the existing native batch and Hadoop based ingestion systems. As an experimental feature, functionality documented on this page is subject to change or removal in future releases. Review the release notes and this page to stay up to date on changes.

By default, the multi-stage query task engine (MSQ task engine) uses the local storage of a node to store data from intermediate steps when executing a query. Although this method provides better speed when executing a query, the data is lost if the node encounters an issue. When you enable durable storage, intermediate data is stored in Amazon S3 instead. Using this feature can improve the reliability of queries that use more than 20 workers. In essence, you trade some performance for better reliability. This is especially useful for long-running queries.

To use durable storage for mesh shuffles: 

- Enable durable storage for mesh shuffle
- Include the context parameter when you submit a query


## Enable durable storage

1. Load the `druid-s3-extensions` extension by adding it to `druid.extensions.loadList` in your `_common/common.runtime.properties` file:
   
   ```
   druid.extensions.loadList=["druid-s3-extensions", ...]
   ```

2. Add the following service properties to `_common/common.runtime.properties`: 
     
   ```bash
   # Required for using durable storage for mesh shuffle
   druid.msq.intermediate.storage.enable=true
   druid.msq.intermediate.storage.type=s3
   druid.msq.intermediate.storage.bucket=<your_bucket>
   druid.msq.intermediate.storage.prefix=<your_prefix<>
   druid.msq.intermediate.storage.tempDir=</path/to/your/temp/dir>
   # Optional for using durable storage for mesh shuffle
   druid.msq.intermediate.storage.maxResultsSize=5GiB
   ```

   For more information about these properties, see [Durable storage properties](./msq-reference.md#durable-storage-properties).

3. In S3, verify that you have the correct permissions set:
   
- `s3:GetObject`
- `s3:PutObject`
- `s3:AbortMultipartUpload`
- `s3:DeleteObject`

   For information about what the permissions are used for, see [S3](./msq-security.md#s3).

6. Apply the changes to your cluster.

### Include the context parameter

When you submit a query, include the context parameter for durable storage as follows:

**API**

   ```json
   "context": {
       "durableShuffleStorage": true
   }
   ```

