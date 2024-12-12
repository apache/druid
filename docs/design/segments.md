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


Apache Druid stores its data and indexes in *segment files* partitioned by time. Druid creates a segment for each segment interval that contains data. If an interval is empty—that is, containing no rows—no segment exists for that time interval. Druid may create multiple segments for the same interval if you ingest data for that period via different ingestion jobs. [Compaction](../data-management/compaction.md) is the Druid process that attempts to combine these segments into a single segment per interval for optimal performance.

The time interval is configurable in the `segmentGranularity` parameter of the [`granularitySpec`](../ingestion/ingestion-spec.md#granularityspec).

For Druid to operate well under heavy query load, it is important for the segment
file size to be within the recommended range of 300-700 MB. If your
segment files are larger than this range, then consider either
changing the granularity of the segment time interval or partitioning your
data and/or adjusting the `targetRowsPerSegment` in your `partitionsSpec`.
A good starting point for this parameter is 5 million rows.
See the Sharding section below and the "Partitioning specification" section of
the [Batch ingestion](../ingestion/hadoop.md#partitionsspec) documentation
for more guidance.

## Segment file structure

Segment files are *columnar*: the data for each column is laid out in
separate data structures. By storing each column separately, Druid decreases query latency by scanning only those columns actually needed for a query. There are three basic column types: timestamp, dimensions, and metrics:

![Druid column types](../assets/druid-column-types.png "Druid Column Types")

Timestamp and metrics type columns are arrays of integer or floating point values compressed with
[LZ4](https://github.com/lz4/lz4-java). Once a query identifies which rows to select, it decompresses them, pulls out the relevant rows, and applies the
desired aggregation operator. If a query doesn’t require a column, Druid skips over that column's data.

Dimension columns are different because they support filter and
group-by operations, so each dimension requires the following
three data structures:

- __Dictionary__: Maps values (which are always treated as strings) to integer IDs, allowing compact representation of the list and bitmap values.
- __List__: The column’s values, encoded using the dictionary. Required for GroupBy and TopN queries. These operators allow queries that solely aggregate metrics based on filters to run without accessing the list of values.
- __Bitmap__: One bitmap for each distinct value in the column, to indicate which rows contain that value. Bitmaps allow for quick filtering operations because they are convenient for quickly applying AND and OR operators. Also known as inverted indexes.

To get a better sense of these data structures, consider the "Page" column from the example data above, represented by the following data structures:

```
1: Dictionary
   {
    "Justin Bieber": 0,
    "Ke$ha":         1
   }

2: List of column data
   [0,
   0,
   1,
   1]

3: Bitmaps
   value="Justin Bieber": [1,1,0,0]
   value="Ke$ha":         [0,0,1,1]
```

Note that the bitmap is different from the dictionary and list data structures: the dictionary and list grow linearly with the size of the data, but the size of the bitmap section is the product of data size and column cardinality. That is, there is one bitmap per separate column value. Columns with the same value share the same bitmap.

For each row in the list of column data, there is only a single bitmap that has a non-zero entry. This means that high cardinality columns have extremely sparse, and therefore highly compressible, bitmaps. Druid exploits this using compression algorithms that are specially suited for bitmaps, such as [Roaring bitmap compression](https://github.com/RoaringBitmap/RoaringBitmap).

## Handling null values

By default Druid stores segments in a SQL compatible null handling mode. String columns always store the null value as id 0, the first position in the value dictionary and an associated entry in the bitmap value indexes used to filter null values. Numeric columns also store a null value bitmap index to indicate the null valued rows, which is used to null check aggregations and for filter matching null values. 

Druid also has a legacy mode which uses default values instead of nulls, which was the default prior to Druid 28.0.0. This legacy mode is deprecated and will be removed in a future release, but can be enabled by setting `druid.generic.useDefaultValueForNull=true`.

In legacy mode, Druid segments created _at ingestion time_ have the following characteristics:

* String columns can not distinguish `''` from `null`, they are treated interchangeably as the same value
* Numeric columns can not represent `null` valued rows, and instead store a `0`.

In legacy mode, numeric columns do not have the null value bitmap, and so can have slightly decreased segment sizes, and queries involving numeric columns can have slightly increased performance in some cases since there is no need to check the null value bitmap.

## Segments with different schemas

Druid segments for the same datasource may have different schemas. If a string column (dimension) exists in one segment but not another, queries that involve both segments still work. In default mode, queries for the segment without the dimension behave as if the dimension contains only blank values. In SQL-compatible mode, queries for the segment without the dimension behave as if the dimension contains only null values. Similarly, if one segment has a numeric column (metric) but another does not, queries on the segment without the metric generally operate as expected. Aggregations over the missing metric operate as if the metric doesn't exist.

## Column format

Each column is stored as two parts:

- A Jackson-serialized `ColumnDescriptor`.
- The binary data for the column.

A `ColumnDescriptor` is  Jackson-serialized instance of the internal Druid `ColumnDescriptor` class . It allows the use of Jackson's polymorphic deserialization to add new and interesting methods of serialization with minimal impact to the code. It consists of some metadata about the column (for example: type, whether it's multi-value) and a list of serialization/deserialization logic that can deserialize the rest of the binary.

### Multi-value columns

A multi-value column allows a single row to contain multiple strings for a column. You can think of it as an array of strings. If a datasource uses multi-value columns, then the data structures within the segment files look a bit different. Let's imagine that in the example above, the second row is tagged with both the `Ke$ha` *and* `Justin Bieber` topics, as follows:

```
1: Dictionary
   {
    "Justin Bieber": 0,
    "Ke$ha":         1
   }

2: List of column data
   [0,
   [0,1],  <--Row value in a multi-value column can contain an array of values
   1,
   1]

3: Bitmaps
   value="Justin Bieber": [1,1,0,0]
   value="Ke$ha":         [0,1,1,1]
                            ^
                            |
                            |
   Multi-value column contains multiple non-zero entries
```

Note the changes to the second row in the list of column data and the `Ke$ha`
bitmap. If a row has more than one value for a column, its entry in
the list is an array of values. Additionally, a row with *n* values in the list has *n* non-zero valued entries in bitmaps.

## Compression

Druid uses LZ4 by default to compress blocks of values for string, long, float, and double columns. Druid uses Roaring to compress bitmaps for string columns and numeric null values. We recommend that you use these defaults unless you've experimented with your data and query patterns suggest that non-default options will perform better in your specific case. 

Druid also supports Concise bitmap compression. For string column bitmaps, the differences between using Roaring and Concise are most pronounced for high cardinality columns. In this case, Roaring is substantially faster on filters that match many values, but in some cases Concise can have a lower footprint due to the overhead of the Roaring format (but is still slower when many values are matched). You configure compression at the segment level, not for individual columns. See [IndexSpec](../ingestion/ingestion-spec.md#indexspec) for more details.

## Segment identification

Segment identifiers typically contain the segment datasource, interval start time (in ISO 8601 format), interval end time (in ISO 8601 format), and version information. If data is additionally sharded beyond a time range, the segment identifier also contains a partition number:

`datasource_intervalStart_intervalEnd_version_partitionNum`

### Segment ID examples

The increasing partition numbers in the following segments indicate that multiple segments exist for the same interval:

```
foo_2015-01-01/2015-01-02_v1_0
foo_2015-01-01/2015-01-02_v1_1
foo_2015-01-01/2015-01-02_v1_2
```

If you reindex the data with a new schema, Druid allocates a new version ID to the newly created segments:

```
foo_2015-01-01/2015-01-02_v2_0
foo_2015-01-01/2015-01-02_v2_1
foo_2015-01-01/2015-01-02_v2_2
```

## Sharding

Multiple segments can exist for a single time interval and datasource. These segments form a `block` for an interval. Depending on the type of `shardSpec` used to shard the data, Druid queries may only complete if a `block` is complete. For example, if a block consists of the following three segments:

```
sampleData_2011-01-01T02:00:00:00Z_2011-01-01T03:00:00:00Z_v1_0
sampleData_2011-01-01T02:00:00:00Z_2011-01-01T03:00:00:00Z_v1_1
sampleData_2011-01-01T02:00:00:00Z_2011-01-01T03:00:00:00Z_v1_2
```

All three segments must load before a query for the interval `2011-01-01T02:00:00:00Z_2011-01-01T03:00:00:00Z` can complete.

Linear shard specs are an exception to this rule. Linear shard specs do not enforce "completeness" so queries can complete even if shards are not completely loaded.

For example, if a real-time ingestion creates three segments that were sharded with linear shard spec, and only two of the segments are loaded, queries return results for those two segments.

## Segment components

A segment contains several files:

* `version.bin`

    4 bytes representing the current segment version as an integer. For example, for v9 segments the version is 0x0, 0x0, 0x0, 0x9.

* `meta.smoosh`

    A file containing metadata (filenames and offsets) about the contents of the other `smoosh` files.

* `XXXXX.smoosh`

    Smoosh (`.smoosh`) files contain concatenated binary data. This file consolidation reduces the number of file descriptors that must be open when accessing data. The files are 2 GB or less in size to remain within the limit of a memory-mapped `ByteBuffer` in Java. 
    Smoosh files contain the following: 
    - Individual files for each column in the data, including one for the `__time` column that refers to the timestamp of the segment. 
    - An `index.drd` file that contains additional segment metadata.

In the codebase, segments have an internal format version. The current segment format version is `v9`.

## Implications of updating segments

Druid uses versioning to manage updates to create a form of multi-version concurrency control (MVCC). These MVCC versions are distinct from the segment format version discussed above.

Note that updates that span multiple segment intervals are only atomic within each interval. They are not atomic across the entire update. For example, if you have the following segments:

```
foo_2015-01-01/2015-01-02_v1_0
foo_2015-01-02/2015-01-03_v1_1
foo_2015-01-03/2015-01-04_v1_2
```

`v2` segments are loaded into the cluster as soon as they are built and replace `v1` segments for the period of time the segments overlap. Before `v2` segments are completely loaded, the cluster may contain a mixture of `v1` and `v2` segments.

```
foo_2015-01-01/2015-01-02_v1_0
foo_2015-01-02/2015-01-03_v2_1
foo_2015-01-03/2015-01-04_v1_2
```

In this case, queries may hit a mixture of `v1` and `v2` segments.
