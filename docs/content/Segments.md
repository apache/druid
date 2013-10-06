---
layout: doc_page
---
Segments
========

Segments are the fundamental structure to store data in Druid. [Historical](Historical.html) and [Realtime](Realtime.html) nodes load and serve segments for querying. To construct segments, Druid will always shard data by a time partition. Data may be further sharded based on dimension cardinality and row count.

The latest Druid segment version is `v9`.

Naming Convention
-----------------

Identifiers for segments are typically constructed using the segment datasource, interval start time (in ISO 8601 format), interval end time (in ISO 8601 format), and a version. If data is additionally sharded beyond a time range, the segment identifier will also contain a partition number.

An example segment identifier may be:
datasource_intervalStart_intervalEnd_version_partitionNum

Segment Components
------------------

A segment is compromised of several files, listed below.

* `version.bin`

    4 bytes representing the current segment version as an integer. E.g., for v9 segments, the version is 0x0, 0x0, 0x0, 0x9

* `meta.smoosh`

    A file with metadata (filenames and offsets) about the contents of the other `smoosh` files

* `XXXXX.smoosh`

    There are some number of these files, which are concatenated binary data

    The `smoosh` files represent multiple files "smooshed" together in order to minimize the number of file descriptors that must be open to house the data. They are files of up to 2GB in size (to match the limit of a memory mapped ByteBuffer in Java). The `smoosh` files house individual files for each of the columns in the data as well as an `index.drd` file with extra metadata about the segment.

    There is also a special column called `__time` that refers to the time column of the segment. This will hopefully become less and less special as the code evolves, but for now it’s as special as my Mommy always told me I am.

Format of a column
------------------

Each column is stored as two parts:

1.  A Jackson-serialized ColumnDescriptor
2.  The rest of the binary for the column

A ColumnDescriptor is essentially an object that allows us to use jackson’s polymorphic deserialization to add new and interesting methods of serialization with minimal impact to the code. It consists of some metadata about the column (what type is it, is it multi-valued, etc.) and then a list of serde logic that can deserialize the rest of the binary.

Sharding Data to Create Segments
--------------------------------

### Sharding Data by Dimension

If the cumulative total number of rows for the different values of a given column exceed some configurable threshold, multiple segments representing the same time interval for the same datasource may be created. These segments will contain some partition number as part of their identifier. Sharding by dimension reduces some of the the costs associated with operations over high cardinality dimensions.
