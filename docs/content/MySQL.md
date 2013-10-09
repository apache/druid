---
layout: doc_page
---
MySQL is an external dependency of Druid. We use it to store various metadata about the system, but not to store the actual data. There are a number of tables used for various purposes described below.

Segments Table
--------------

This is dictated by the `druid.db.tables.segments` property.

This table stores metadata about the segments that are available in the system. The table is polled by the [Coordinator](Coordinator.html) to determine the set of segments that should be available for querying in the system. The table has two main functional columns, the other columns are for indexing purposes.

The `used` column is a boolean "tombstone". A 1 means that the segment should be "used" by the cluster (i.e. it should be loaded and available for requests). A 0 means that the segment should not be actively loaded into the cluster. We do this as a means of removing segments from the cluster without actually removing their metadata (which allows for simpler rolling back if that is ever an issue).

The `payload` column stores a JSON blob that has all of the metadata for the segment (some of the data stored in this payload is redundant with some of the columns in the table, that is intentional). This looks something like

```
{
 "dataSource":"wikipedia",
 "interval":"2012-05-23T00:00:00.000Z/2012-05-24T00:00:00.000Z",
 "version":"2012-05-24T00:10:00.046Z",
 "loadSpec":{"type":"s3_zip",
             "bucket":"bucket_for_segment",
             "key":"path/to/segment/on/s3"},
 "dimensions":"comma-delimited-list-of-dimension-names",
 "metrics":"comma-delimited-list-of-metric-names",
 "shardSpec":{"type":"none"},
 "binaryVersion":9,
 "size":size_of_segment,
 "identifier":"wikipedia_2012-05-23T00:00:00.000Z_2012-05-24T00:00:00.000Z_2012-05-23T00:10:00.046Z"
}
```

Note that the format of this blob can and will change from time-to-time.

Rule Table
----------

The rule table is used to store the various rules about where segments should land. These rules are used by the [Coordinator](Coordinator.html) when making segment (re-)allocation decisions about the cluster.

Config Table
------------

The config table is used to store runtime configuration objects. We do not have many of these yet and we are not sure if we will keep this mechanism going forward, but it is the beginnings of a method of changing some configuration parameters across the cluster at runtime.

Task-related Tables
-------------------

There are also a number of tables created and used by the [Indexing Service](Indexing-Service.html) in the course of its work.
