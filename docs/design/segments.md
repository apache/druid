---
id: segments
title: "Segments"
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


Apache Druid (incubating) stores its index in *segment files*, which are partitioned by
time. In a basic setup, one segment file is created for each time
interval, where the time interval is configurable in the
`segmentGranularity` parameter of the
[`granularitySpec`](../ingestion/index.md#granularityspec).  For Druid to
operate well under heavy query load, it is important for the segment
file size to be within the recommended range of 300MB-700MB. If your
segment files are larger than this range, then consider either
changing the granularity of the time interval or partitioning your
data and tweaking the `targetPartitionSize` in your `partitionsSpec`
(a good starting point for this parameter is 5 million rows).  See the
sharding section below and the 'Partitioning specification' section of
the [Batch ingestion](../ingestion/hadoop.md#partitionsspec) documentation
for more information.

### A segment file's core data structures

Here we describe the internal structure of segment files, which is
essentially *columnar*: the data for each column is laid out in
separate data structures. By storing each column separately, Druid can
decrease query latency by scanning only those columns actually needed
for a query.  There are three basic column types: the timestamp
column, dimension columns, and metric columns, as illustrated in the
image below:

![Druid column types](../assets/druid-column-types.png "Druid Column Types")

The timestamp and metric columns are simple: behind the scenes each of
these is an array of integer or floating point values compressed with
LZ4. Once a query knows which rows it needs to select, it simply
decompresses these, pulls out the relevant rows, and applies the
desired aggregation operator. As with all columns, if a query doesn’t
require a column, then that column’s data is just skipped over.

Dimensions columns are different because they support filter and
group-by operations, so each dimension requires the following
three data structures:

1. A dictionary that maps values (which are always treated as strings) to integer IDs,
2. A list of the column’s values, encoded using the dictionary in 1, and
3. For each distinct value in the column, a bitmap that indicates which rows contain that value.


Why these three data structures? The dictionary simply maps string
values to integer ids so that the values in \(2\) and \(3\) can be
represented compactly. The bitmaps in \(3\) -- also known as *inverted
indexes* allow for quick filtering operations (specifically, bitmaps
are convenient for quickly applying AND and OR operators). Finally,
the list of values in \(2\) is needed for *group by* and *TopN*
queries. In other words, queries that solely aggregate metrics based
on filters do not need to touch the list of dimension values stored in \(2\).

To get a concrete sense of these data structures, consider the ‘page’
column from the example data above.  The three data structures that
represent this dimension are illustrated in the diagram below.

```
1: Dictionary that encodes column values
  {
    "Justin Bieber": 0,
    "Ke$ha":         1
  }

2: Column data
  [0,
   0,
   1,
   1]

3: Bitmaps - one for each unique value of the column
  value="Justin Bieber": [1,1,0,0]
  value="Ke$ha":         [0,0,1,1]
```

Note that the bitmap is different from the first two data structures:
whereas the first two grow linearly in the size of the data (in the
worst case), the size of the bitmap section is the product of data
size * column cardinality. Compression will help us here though
because we know that for each row in 'column data', there will only be a
single bitmap that has non-zero entry. This means that high cardinality
columns will have extremely sparse, and therefore highly compressible,
bitmaps. Druid exploits this using compression algorithms that are
specially suited for bitmaps, such as roaring bitmap compression.

### Multi-value columns

If a data source makes use of multi-value columns, then the data
structures within the segment files look a bit different. Let's
imagine that in the example above, the second row were tagged with
both the 'Ke$ha' *and* 'Justin Bieber' topics. In this case, the three
data structures would now look as follows:

```
1: Dictionary that encodes column values
  {
    "Justin Bieber": 0,
    "Ke$ha":         1
  }

2: Column data
  [0,
   [0,1],  <--Row value of multi-value column can have array of values
   1,
   1]

3: Bitmaps - one for each unique value
  value="Justin Bieber": [1,1,0,0]
  value="Ke$ha":         [0,1,1,1]
                            ^
                            |
                            |
    Multi-value column has multiple non-zero entries
```

Note the changes to the second row in the column data and the Ke$ha
bitmap. If a row has more than one value for a column, its entry in
the 'column data' is an array of values. Additionally, a row with *n*
values in 'column data' will have *n* non-zero valued entries in
bitmaps.

## Naming Convention

Identifiers for segments are typically constructed using the segment datasource, interval start time (in ISO 8601 format), interval end time (in ISO 8601 format), and a version. If data is additionally sharded beyond a time range, the segment identifier will also contain a partition number.

An example segment identifier may be:
datasource_intervalStart_intervalEnd_version_partitionNum

## Segment Components

Behind the scenes, a segment is comprised of several files, listed below.

* `version.bin`

    4 bytes representing the current segment version as an integer. E.g., for v9 segments, the version is 0x0, 0x0, 0x0, 0x9

* `meta.smoosh`

    A file with metadata (filenames and offsets) about the contents of the other `smoosh` files

* `XXXXX.smoosh`

    There are some number of these files, which are concatenated binary data

    The `smoosh` files represent multiple files "smooshed" together in order to minimize the number of file descriptors that must be open to house the data. They are files of up to 2GB in size (to match the limit of a memory mapped ByteBuffer in Java). The `smoosh` files house individual files for each of the columns in the data as well as an `index.drd` file with extra metadata about the segment.

    There is also a special column called `__time` that refers to the time column of the segment. This will hopefully become less and less special as the code evolves, but for now it’s as special as my Mommy always told me I am.

In the codebase, segments have an internal format version. The current segment format version is `v9`.

## Format of a column

Each column is stored as two parts:

1.  A Jackson-serialized ColumnDescriptor
2.  The rest of the binary for the column

A ColumnDescriptor is essentially an object that allows us to use Jackson's polymorphic deserialization to add new and interesting methods of serialization with minimal impact to the code. It consists of some metadata about the column (what type is it, is it multi-value, etc.) and then a list of serialization/deserialization logic that can deserialize the rest of the binary.

## Sharding Data to Create Segments

### Sharding

Multiple segments may exist for the same interval of time for the same datasource. These segments form a `block` for an interval.
Depending on the type of `shardSpec` that is used to shard the data, Druid queries may only complete if a `block` is complete. That is to say, if a block consists of 3 segments, such as:

`sampleData_2011-01-01T02:00:00:00Z_2011-01-01T03:00:00:00Z_v1_0`

`sampleData_2011-01-01T02:00:00:00Z_2011-01-01T03:00:00:00Z_v1_1`

`sampleData_2011-01-01T02:00:00:00Z_2011-01-01T03:00:00:00Z_v1_2`

All 3 segments must be loaded before a query for the interval `2011-01-01T02:00:00:00Z_2011-01-01T03:00:00:00Z` completes.

The exception to this rule is with using linear shard specs. Linear shard specs do not force 'completeness' and queries can complete even if shards are not loaded in the system.
For example, if your real-time ingestion creates 3 segments that were sharded with linear shard spec, and only two of the segments were loaded in the system, queries would return results only for those 2 segments.

## Schema changes

## Replacing segments

Druid uniquely
identifies segments using the datasource, interval, version, and partition number. The partition number is only visible in the segment id if
there are multiple segments created for some granularity of time. For example, if you have hourly segments, but you
have more data in an hour than a single segment can hold, you can create multiple segments for the same hour. These segments will share
the same datasource, interval, and version, but have linearly increasing partition numbers.

```
foo_2015-01-01/2015-01-02_v1_0
foo_2015-01-01/2015-01-02_v1_1
foo_2015-01-01/2015-01-02_v1_2
```

In the example segments above, the `dataSource = foo`, `interval = 2015-01-01/2015-01-02`, `version = v1`, and `partitionNum = 0`.
If at some later point in time, you reindex the data with a new schema, the newly created segments will have a higher version id.

```
foo_2015-01-01/2015-01-02_v2_0
foo_2015-01-01/2015-01-02_v2_1
foo_2015-01-01/2015-01-02_v2_2
```

Druid batch indexing (either Hadoop-based or IndexTask-based) guarantees atomic updates on an interval-by-interval basis.
In our example, until all `v2` segments for `2015-01-01/2015-01-02` are loaded in a Druid cluster, queries exclusively use `v1` segments.
Once all `v2` segments are loaded and queryable, all queries ignore `v1` segments and switch to the `v2` segments.
Shortly afterwards, the `v1` segments are unloaded from the cluster.

Note that updates that span multiple segment intervals are only atomic within each interval. They are not atomic across the entire update.
For example, you have segments such as the following:

```
foo_2015-01-01/2015-01-02_v1_0
foo_2015-01-02/2015-01-03_v1_1
foo_2015-01-03/2015-01-04_v1_2
```

`v2` segments will be loaded into the cluster as soon as they are built and replace `v1` segments for the period of time the
segments overlap. Before v2 segments are completely loaded, your cluster may have a mixture of `v1` and `v2` segments.

```
foo_2015-01-01/2015-01-02_v1_0
foo_2015-01-02/2015-01-03_v2_1
foo_2015-01-03/2015-01-04_v1_2
```

In this case, queries may hit a mixture of `v1` and `v2` segments.

## Different schemas among segments

Druid segments for the same datasource may have different schemas. If a string column (dimension) exists in one segment but not
another, queries that involve both segments still work. Queries for the segment missing the dimension will behave as if the dimension has only null values.
Similarly, if one segment has a numeric column (metric) but another does not, queries on the segment missing the
metric will generally "do the right thing". Aggregations over this missing metric behave as if the metric were missing.
