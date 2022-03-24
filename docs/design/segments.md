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


Apache Druid stores its index in *segment files* partitioned by
time. In a basic setup, Druid creates one segment file for each time
interval, where the time interval is configurable in the
`segmentGranularity` parameter of the
[`granularitySpec`](../ingestion/ingestion-spec.md#granularityspec).

For Druid to operate well under heavy query load, it is important for the segment
file size to be within the recommended range of 300MB-700MB. If your
segment files are larger than this range, then consider either
changing the granularity of the time interval or partitioning your
data and tweaking the `targetRowsPerSegment` in your `partitionsSpec`
(a good starting point for this parameter is 5 million rows).

See the Sharding section below and the 'Partitioning specification' section of
the [Batch ingestion](../ingestion/hadoop.md#partitionsspec) documentation
for more guidance.

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

## Segment file structure

Segment files are *columnar*: the data for each column is laid out in
separate data structures. By storing each column separately, Druid decreases query latency by scanning only those columns actually needed for a query.  There are three basic column types: timestamp, dimensions, and metrics:

![Druid column types](../assets/druid-column-types.png "Druid Column Types")

Timestamp and metrics type columns are arrays of integer or floating point values compressed with
[LZ4](https://github.com/lz4/lz4-java). Once a query identifies which rows to select, it decompresses them, pulls out the relevant rows, and applies the
desired aggregation operator. If a query doesn’t require a column, Druid skips over that column's data.

Dimensions columns are different because they support filter and
group-by operations, so each dimension requires the following
three data structures:

- Dictionary: Maps values (which are always treated as strings) to integer IDs, allowing compact representation of the list and bitmap values.
- List: The column’s values, encoded using the dictionary. Required for GROUP BY and TOPN queries. These operators allow queries that solely aggregate metrics based on filters to run without accessing the list of values.
- Bitmap: One bitmal for each distinct value in the column, to indicate which rows contain that value. Bitmaps allow for quick filtering operations because they are convenient for quickly applying AND and OR operators. Also known as inverted indexes.

To get a better sense of these data structures, consider the ‘page’ column from the example data above.  The three data structures that represent this dimension are illustrated below:

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

Note that the bitmap is different from the dictionary and list data structures: the dictionary and list grow linearly with the size of the data, but the size of the bitmap section is the product of data size * column cardinality. 

For each row in the list of column data, there is only a single bitmap that has a non-zero entry. This means that high cardinality columns have extremely sparse, and therefore highly compressible, bitmaps. Druid exploits this using compression algorithms that are specially suited for bitmaps, such as [Roaring bitmap compression](https://github.com/RoaringBitmap/RoaringBitmap).

### Multi-value columns

If a data source uses multi-value columns, then the data structures within the segment files look a bit different. Let's imagine that in the example above, the second row is tagged with both the `Ke$ha` *and* `Justin Bieber` topics, as follows:

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

## Handling null values

By default, Druid string dimension columns use the values `''` and `null` interchangeably and numeric and metric columns can not represent `null` at all, instead coercing nulls to `0`. However, Druid also provides a SQL compatible null handling mode, which you can enable at the system level, through `druid.generic.useDefaultValueForNull`. This setting, when set to `false`, allows Druid to create segments _at ingestion time_ in which the string columns can distinguish `''` from `null`, and numeric columns which can represent `null` valued rows instead of `0`.

String dimension columns contain no additional column structures in this mode, instead they reserve an additional dictionary entry for the `null` value. Numeric columns are stored in the segment with an additional bitmap in which the set bits indicate `null` valued rows. 

In addition to slightly increased segment sizes, SQL compatible null handling can incur a performance cost at query time, due to the need to check the null bitmap. This performance cost only occurs for columns that actually contain null values.

## Segment components

A segment contains several files:

* `version.bin`

    4 bytes representing the current segment version as an integer. For example, for v9 segments the version is 0x0, 0x0, 0x0, 0x9.

* `meta.smoosh`

    A file containing metadata (filenames and offsets) about the contents of the other `smoosh` files

* `XXXXX.smoosh`

    A number of files containing concatenated binary data.

    The `smoosh` files represent multiple files "smooshed" together in order to minimize the number of file descriptors that must be open to house the data. They are files of up to 2GB in size (to match the limit of a memory mapped ByteBuffer in Java). The `smoosh` files house individual files for each of the columns in the data as well as an `index.drd` file with extra metadata about the segment.

Additionally, a column called `__time` refers to the time column of the segment.

In the codebase, segments have an internal format version. The current segment format version is `v9`.

## Column format

Each column is stored as two parts:

- A Jackson-serialized ColumnDescriptor.
- The rest of the binary for the column.

A ColumnDescriptor is an object that allows the use of Jackson's polymorphic deserialization to add new and interesting methods of serialization with minimal impact to the code. It consists of some metadata about the column (for example: type, whether it's multi-value) and a list of serialization/deserialization logic that can deserialize the rest of the binary.

## Compression
Druid uses LZ4 by default to compress blocks of values for string, long, float, and double columns. Druid using Roaring to compress bitmaps for string columns and numeric null values. We recommend that you use these defaults unless you've experimented with your data and query patterns suggest that non-default options will perform better in your specific case. 

For bitmap in string columns, the differences between using Roaring and Concise are most pronounced for high cardinality columns. In this case, Roaring is substantially faster on filters that match a lot of values, but in some cases Concise can have a lower footprint due to the overhead of the Roaring format (but is still slower when a lot of values are matched). You configure compression at the segment level, not for individual columns. See [IndexSpec](../ingestion/ingestion-spec.md#indexspec) for more details.

## Sharding

Multiple segments can exist for a single time interval and datasource. These segments form a `block` for an interval. Depending on the type of `shardSpec` used to shard the data, Druid queries may only complete if a `block` is complete. For example, if a block consists of the following three segments:

```
sampleData_2011-01-01T02:00:00:00Z_2011-01-01T03:00:00:00Z_v1_0
sampleData_2011-01-01T02:00:00:00Z_2011-01-01T03:00:00:00Z_v1_1
sampleData_2011-01-01T02:00:00:00Z_2011-01-01T03:00:00:00Z_v1_2
```

All three segments must load before a query for the interval `2011-01-01T02:00:00:00Z_2011-01-01T03:00:00:00Z` can complete.

Linear shard specs are an exception to this rule. Linear shard specs do not enforce 'completeness' so queries can complete even if shards are not completely loaded.

For example, if a real-time ingestion creates three segments that were sharded with linear shard spec, and only two of the segments are loaded, queries return results for those two segments.

## Segment update implications

Druid batch indexing (either Hadoop-based or IndexTask-based) guarantees atomic updates on an interval-by-interval basis. In our example, until all `v2` segments for `2015-01-01/2015-01-02` are loaded in a Druid cluster, queries exclusively use `v1` segments. Once all `v2` segments are loaded and queryable, all queries ignore `v1` segments and switch to the `v2` segments. Shortly afterwards, the `v1` segments are unloaded from the cluster.

Note that updates that span multiple segment intervals are only atomic within each interval. They are not atomic across the entire update. For example, if you have the following segments:

```
foo_2015-01-01/2015-01-02_v1_0
foo_2015-01-02/2015-01-03_v1_1
foo_2015-01-03/2015-01-04_v1_2
```

`v2` segments are loaded into the cluster as soon as they are built and replace `v1` segments for the period of time the segments overlap. Before v2 segments are completely loaded, the cluster may contain a mixture of `v1` and `v2` segments.

```
foo_2015-01-01/2015-01-02_v1_0
foo_2015-01-02/2015-01-03_v2_1
foo_2015-01-03/2015-01-04_v1_2
```

In this case, queries may hit a mixture of `v1` and `v2` segments.

## Segments with different schemas

Druid segments for the same datasource may have different schemas. If a string column (dimension) exists in one segment but not another, queries that involve both segments still work. Queries for the segment without the dimension behave as if the dimension contains only null values. Similarly, if one segment has a numeric column (metric) but another does not, queries on the segment without the metric generally operate as expected. Aggregations over the missing metric operate as if the metric doesn't exist.
