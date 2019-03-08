---
layout: doc_page
title: "Metadata Storage"
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

# Metadata Storage

The Metadata Storage is an external dependency of Druid. Druid uses it to store
various metadata about the system, but not to store the actual data. There are
a number of tables used for various purposes described below.

Derby is the default metadata store for Druid, however, it is not suitable for production. 
[MySQL](../development/extensions-core/mysql.html) and [PostgreSQL](../development/extensions-core/postgresql.html) are more production suitable metadata stores.

<div class="note caution">
The Metadata Storage stores the entire metadata which is essential for a Druid cluster to work.
For production clusters, consider using MySQL or PostgreSQL instead of Derby.
Also, it's highly recommended to set up a high availability environment
because there is no way to restore if you lose any metadata.
</div>

## Using derby

Add the following to your Druid configuration.

```properties
druid.metadata.storage.type=derby
druid.metadata.storage.connector.connectURI=jdbc:derby://localhost:1527//opt/var/druid_state/derby;create=true
```

## MySQL
  
See [mysql-metadata-storage extension documentation](../development/extensions-core/mysql.html).  
  
## PostgreSQL 

See [postgresql-metadata-storage](../development/extensions-core/postgresql.html). 

## Adding custom dbcp properties

NOTE: These properties are not settable through the druid.metadata.storage.connector.dbcp properties : username, password, connectURI, validationQuery, testOnBorrow. These must be set through druid.metadata.storage.connector properties.

Example supported properties:

```properties
druid.metadata.storage.connector.dbcp.maxConnLifetimeMillis=1200000
druid.metadata.storage.connector.dbcp.defaultQueryTimeout=30000
```

See [BasicDataSource Configuration](https://commons.apache.org/proper/commons-dbcp/configuration.html) for full list.

## Metadata Storage Tables

### Segments Table

This is dictated by the `druid.metadata.storage.tables.segments` property.

This table stores metadata about the segments that are available in the system.
The table is polled by the [Coordinator](../design/coordinator.html) to
determine the set of segments that should be available for querying in the
system. The table has two main functional columns, the other columns are for
indexing purposes.

The `used` column is a boolean "tombstone". A 1 means that the segment should
be "used" by the cluster (i.e. it should be loaded and available for requests).
A 0 means that the segment should not be actively loaded into the cluster. We
do this as a means of removing segments from the cluster without actually
removing their metadata (which allows for simpler rolling back if that is ever
an issue).

The `payload` column stores a JSON blob that has all of the metadata for the segment (some of the data stored in this payload is redundant with some of the columns in the table, that is intentional). This looks something like

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

Note that the format of this blob can and will change from time-to-time.

### Rule Table

The rule table is used to store the various rules about where segments should
land. These rules are used by the [Coordinator](../design/coordinator.html)
  when making segment (re-)allocation decisions about the cluster.

### Config Table

The config table is used to store runtime configuration objects. We do not have
many of these yet and we are not sure if we will keep this mechanism going
forward, but it is the beginnings of a method of changing some configuration
parameters across the cluster at runtime.

### Task-related Tables

There are also a number of tables created and used by the [Overlord](../design/overlord.html) and [MiddleManager](../design/middlemanager.html) when managing tasks.

### Audit Table

The Audit table is used to store the audit history for configuration changes
e.g rule changes done by [Coordinator](../design/coordinator.html) and other
config changes.

##Accessed By: ##

The Metadata Storage is accessed only by:

1. Indexing Service Processes (if any)
2. Realtime Processes (if any)
3. Coordinator Processes

Thus you need to give permissions (eg in AWS Security Groups)  only for these machines to access the Metadata storage.
