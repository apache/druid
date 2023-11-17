---
id: metadata-storage
title: "Metadata storage"
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


Apache Druid relies on an external dependency for metadata storage.
Druid uses the metadata store to house various metadata about the system, but not to store the actual data.
The metadata store retains all metadata essential for a Druid cluster to work.

The metadata store includes the following:
- Segments records
- Rule records
- Configuration records
- Task-related tables
- Audit records

Derby is the default metadata store for Druid, however, it is not suitable for production.
[MySQL](../development/extensions-core/mysql.md) and [PostgreSQL](../development/extensions-core/postgresql.md) are more production suitable metadata stores.
See [Metadata storage configuration](../configuration/index.md#metadata-storage) for the default configuration settings.

:::info
 We also recommend you set up a high availability environment because there is no way to restore lost metadata.
:::

## Available metadata stores

Druid supports Derby, MySQL, and PostgreSQL for storing metadata.

To avoid issues with upgrades that require schema changes to a large metadata table, consider a metadata store version that supports instant ADD COLUMN semantics.
See the database-specific docs for guidance on versions.

### MySQL

See [mysql-metadata-storage extension documentation](../development/extensions-core/mysql.md).

### PostgreSQL

See [postgresql-metadata-storage](../development/extensions-core/postgresql.md).


### Derby

:::info
 For production clusters, consider using MySQL or PostgreSQL instead of Derby.
:::

Configure metadata storage with Derby by setting the following properties in your Druid configuration.

```properties
druid.metadata.storage.type=derby
druid.metadata.storage.connector.connectURI=jdbc:derby://localhost:1527//opt/var/druid_state/derby;create=true
```

## Adding custom DBCP properties

You can add custom properties to customize the database connection pool (DBCP) for connecting to the metadata store.
Define these properties with a `druid.metadata.storage.connector.dbcp.` prefix.
For example:

```properties
druid.metadata.storage.connector.dbcp.maxConnLifetimeMillis=1200000
druid.metadata.storage.connector.dbcp.defaultQueryTimeout=30000
```

Certain properties cannot be set through `druid.metadata.storage.connector.dbcp.` and must be set with the prefix `druid.metadata.storage.connector.`:
* `username`
* `password`
* `connectURI`
* `validationQuery`
* `testOnBorrow`

See [BasicDataSource Configuration](https://commons.apache.org/proper/commons-dbcp/configuration) for a full list of configurable properties.

## Metadata storage tables

This section describes the various tables in metadata storage.

### Segments table

This is dictated by the `druid.metadata.storage.tables.segments` property.

This table stores metadata about the segments that should be available in the system. (This set of segments is called
"used segments" elsewhere in the documentation and throughout the project.) The table is polled by the
[Coordinator](../design/coordinator.md) to determine the set of segments that should be available for querying in the
system. The table has two main functional columns, the other columns are for indexing purposes.

Value 1 in the `used` column means that the segment should be "used" by the cluster (i.e., it should be loaded and
available for requests). Value 0 means that the segment should not be loaded into the cluster. We do this as a means of
unloading segments from the cluster without actually removing their metadata (which allows for simpler rolling back if
that is ever an issue). The `used` column has a corresponding `used_status_last_updated` column which denotes the time
when the `used` status of the segment was last updated. This information can be used by the Coordinator to determine if
a segment is a candidate for deletion (if automated segment killing is enabled).

The `payload` column stores a JSON blob that has all of the metadata for the segment.
Some of the data in the `payload` column intentionally duplicates data from other columns in the segments table.
As an example, the `payload` column may take the following form:

```json
{
 "dataSource":"wikipedia",
 "interval":"2012-05-23T00:00:00.000Z/2012-05-24T00:00:00.000Z",
 "version":"2012-05-24T00:10:00.046Z",
 "loadSpec":{
    "type":"s3_zip",
    "bucket":"bucket_for_segment",
    "key":"path/to/segment/on/s3"
 },
 "dimensions":"comma-delimited-list-of-dimension-names",
 "metrics":"comma-delimited-list-of-metric-names",
 "shardSpec":{"type":"none"},
 "binaryVersion":9,
 "size":size_of_segment,
 "identifier":"wikipedia_2012-05-23T00:00:00.000Z_2012-05-24T00:00:00.000Z_2012-05-23T00:10:00.046Z"
}
```

### Rule table

The rule table stores the various rules about where segments should
land. These rules are used by the [Coordinator](../design/coordinator.md)
  when making segment (re-)allocation decisions about the cluster.

### Config table

The config table stores runtime configuration objects. We do not have
many of these yet and we are not sure if we will keep this mechanism going
forward, but it is the beginnings of a method of changing some configuration
parameters across the cluster at runtime.

### Task-related tables

Task-related tables are created and used by the [Overlord](../design/overlord.md) and [MiddleManager](../design/middlemanager.md) when managing tasks.

### Audit table

The audit table stores the audit history for configuration changes
such as rule changes done by [Coordinator](../design/coordinator.md) and other
config changes.

## Metadata storage access

Only the following processes access the metadata storage:

1. Indexing service processes (if any)
2. Realtime processes (if any)
3. Coordinator processes

Thus you need to give permissions (e.g., in AWS security groups) for only these machines to access the metadata storage.

## Learn more

See the following topics for more information:
* [Metadata storage configuration](../configuration/index.md#metadata-storage)
* [Automated cleanup for metadata records](../operations/clean-metadata-store.md)

