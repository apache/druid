---
id: deep-storage
title: "Deep storage"
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


Deep storage is where segments are stored.  It is a storage mechanism that Apache Druid does not provide.  This deep storage infrastructure defines the level of durability of your data. As long as Druid processes can see this storage infrastructure and get at the segments stored on it, you will not lose data no matter how many Druid nodes you lose.  If segments disappear from this storage layer, then you will lose whatever data those segments represented.

In addition to being the backing store for segments, you can use [query from deep storage](#querying-from-deep-storage) and run queries against segments stored primarily in deep storage. The [load rules](../operations/rule-configuration.md#load-rules) you configure determine whether segments exist primarily in deep storage or in a combination of deep storage and Historical processes.

## Deep storage options

Druid supports multiple options for deep storage, including blob storage from major cloud providers. Select the one that fits your environment.

### Local

Local storage is intended for use in the following situations:

- You have just one server.
- Or, you have multiple servers, and they all have access to a shared filesystem (for example: NFS).

In multi-server production clusters, rather than local storage with a shared filesystem, it is instead recommended to
use cloud-based deep storage ([Amazon S3](#amazon-s3-or-s3-compatible), [Google Cloud Storage](#google-cloud-storage),
or [Azure Blob Storage](#azure-blob-storage)), S3-compatible storage (like Minio), or [HDFS](#hdfs). These options are
generally more convenient, more scalable, and more robust than setting up a shared filesystem.

The following configurations in `common.runtime.properties` apply to local storage:

|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.storage.type`|`local`||Must be set.|
|`druid.storage.storageDirectory`|any local directory|Directory for storing segments. Must be different from `druid.segmentCache.locations` and `druid.segmentCache.infoDir`.|`/tmp/druid/localStorage`|
|`druid.storage.zip`|`true`, `false`|Whether segments in `druid.storage.storageDirectory` are written as directories (`false`) or zip files (`true`).|`false`|

For example:

```
druid.storage.type=local
druid.storage.storageDirectory=/tmp/druid/localStorage
```

The `druid.storage.storageDirectory` must be set to a different path than `druid.segmentCache.locations` or
`druid.segmentCache.infoDir`.

### Amazon S3 or S3-compatible

See [`druid-s3-extensions`](../development/extensions-core/s3.md).

### Google Cloud Storage

See [`druid-google-extensions`](../development/extensions-core/google.md).

### Azure Blob Storage

See [`druid-azure-extensions`](../development/extensions-core/azure.md).

### HDFS

See [druid-hdfs-storage extension documentation](../development/extensions-core/hdfs.md).

### Additional options

For additional deep storage options, please see our [extensions list](../configuration/extensions.md).

## Querying from deep storage

Although not as performant as querying segments stored on disk for Historical processes, you can query from deep storage to access segments that you may not need frequently or with the extreme low latency Druid queries traditionally provide. You trade some performance for a total lower storage cost because you can access more of your data without the need to increase the number or capacity of your Historical processes.

For information about how to run queries, see [Query from deep storage](../querying/query-from-deep-storage.md).