---
id: deep-storage-migration
title: "Deep storage migration"
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


If you have been running an evaluation Druid cluster using local deep storage and wish to migrate to a
more production-capable deep storage system such as S3 or HDFS, this document describes the necessary steps.

Migration of deep storage involves the following steps at a high level:

- Copying segments from local deep storage to the new deep storage
- Exporting Druid's segments table from metadata
- Rewriting the load specs in the exported segment data to reflect the new deep storage location
- Reimporting the edited segments into metadata

## Shut down cluster services

To ensure a clean migration, shut down the non-coordinator services to ensure that metadata state will not
change as you do the migration.

When migrating from Derby, the coordinator processes will still need to be up initially, as they host the Derby database.

## Copy segments from old deep storage to new deep storage.

Before migrating, you will need to copy your old segments to the new deep storage.

For information on what path structure to use in the new deep storage, please see [deep storage migration options](../operations/export-metadata.html#deep-storage-migration).

## Export segments with rewritten load specs

Druid provides an [Export Metadata Tool](../operations/export-metadata.md) for exporting metadata from Derby into CSV files
which can then be reimported.

By setting [deep storage migration options](../operations/export-metadata.html#deep-storage-migration), the `export-metadata` tool will export CSV files where the segment load specs have been rewritten to load from your new deep storage location.

Run the `export-metadata` tool on your existing cluster, using the migration options appropriate for your new deep storage location, and save the CSV files it generates. After a successful export, you can shut down the coordinator.

### Import metadata

After generating the CSV exports with the modified segment data, you can reimport the contents of the Druid segments table from the generated CSVs.

Please refer to [import commands](../operations/export-metadata.html#importing-metadata) for examples. Only the `druid_segments` table needs to be imported.

### Restart cluster

After importing the segment table successfully, you can now restart your cluster.
