---
layout: doc_page
---
Segments
========


Druid stores its index in *segment files*, which are partitioned by
time. In a basic setup, one segment file is created for each time
interval, where the time inteval is configurable in the
`segmentGranularity` parameter of the `granularitySpec`, which is
documented [here](../ingestion/batch-ingestion.html).  For druid to
operate well under heavy query load, it is important for the segment
file size to be within the recommended range of 300mb-700mb. If your
segment files are larger than this range, then consider either
changing the the granularity of the time interval or partitioning your
data and tweaking the `targetPartitionSize` in your `partitioningSpec`
(a good starting point for this parameter is 5 million rows).  See the
sharding section below and the 'Partitioning specification' section of
the [Batch ingestion](../ingestion/batch-ingestion.html) documentation
for more information.

### A segment file's core data structures
Here we describe the internal structure of segment files, which is
essentially *columnar*: the data for each column is laid out in
separate data structures. By storing each column separately, Druid can
decrease query latency by scanning only those columns actually needed
for a query.  There are three basic column types: the timestamp
column, dimension columns, and metric columns, as illustrated in the
image below:

![Druid column types](../../img/druid-column-types.png "Druid Column Types")

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
values to integer ids so that the values in 2 and 3 can be
represented compactly. The bitmaps in 3 -- also known as *inverted
indexes* allow for quick filtering operations (specifically, bitmaps
are convenient for quickly applying AND and OR operators). Finally,
the list of values in 2 is needed for *group by* and *TopN*
queries. In other words, queries that solely aggregate metrics based
on filters do not need to touch the list of dimension values stored in
2.

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

Naming Convention
-----------------

Identifiers for segments are typically constructed using the segment datasource, interval start time (in ISO 8601 format), interval end time (in ISO 8601 format), and a version. If data is additionally sharded beyond a time range, the segment identifier will also contain a partition number.

An example segment identifier may be:
datasource_intervalStart_intervalEnd_version_partitionNum

Segment Components
------------------

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

Format of a column
------------------

Each column is stored as two parts:

1.  A Jackson-serialized ColumnDescriptor
2.  The rest of the binary for the column

A ColumnDescriptor is essentially an object that allows us to use jackson’s polymorphic deserialization to add new and interesting methods of serialization with minimal impact to the code. It consists of some metadata about the column (what type is it, is it multi-valued, etc.) and then a list of serde logic that can deserialize the rest of the binary.

Sharding Data to Create Segments
--------------------------------

### Sharding Data by Dimension

If the cumulative total number of rows for the different values of a
given column exceed some configurable threshold, multiple segments
representing the same time interval for the same datasource may be
created. These segments will contain some partition number as part of
their identifier. Sharding by dimension reduces some of the the costs
associated with operations over high cardinality dimensions. For more
information on sharding, see the ingestion documentation.
