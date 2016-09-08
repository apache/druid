---
layout: doc_page
---

# Metadata Storage

The Metadata Storage is an external dependency of Druid. Druid uses it to store
various metadata about the system, but not to store the actual data. There are
a number of tables used for various purposes described below.

Derby is the default metadata store for Druid, however, it is not suitable for production. 
[MySQL](../development/extensions-core/mysql.html) and [PostgreSQL](../development/extensions-core/postgresql.html) are more production suitable metadata stores.

<div class="note caution">
Derby is not suitable for production use as a metadata store. Use MySQL or PostgreSQL instead.
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

There are also a number of tables created and used by the [Indexing
Service](../design/indexing-service.html) in the course of its work.

### Audit Table

The Audit table is used to store the audit history for configuration changes
e.g rule changes done by [Coordinator](../design/coordinator.html) and other
config changes.

##Accessed By: ##

The Metadata Storage is accessed only by:

1. Indexing Service Nodes (if any)
2. Realtime Nodes (if any)
3. Coordinator Nodes

Thus you need to give permissions (eg in AWS Security Groups)  only for these machines to access the Metadata storage.
