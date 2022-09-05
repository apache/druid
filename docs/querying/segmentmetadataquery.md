---
id: segmentmetadataquery
title: "SegmentMetadata queries"
sidebar_label: "SegmentMetadata"
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

> Apache Druid supports two query languages: [Druid SQL](sql.md) and [native queries](querying.md).
> This document describes a query
> type that is only available in the native language. However, Druid SQL contains similar functionality in
> its [metadata tables](sql-metadata-tables.md).

Segment metadata queries return per-segment information about:

* Number of rows stored inside the segment
* Interval the segment covers
* Estimated total segment byte size in if it was stored in a 'flat format' (e.g. a csv file)
* Segment id
* Is the segment rolled up
* Detailed per column information such as:
  - type
  - cardinality
  - min/max values
  - presence of null values
  - estimated 'flat format' byte size


```json
{
  "queryType":"segmentMetadata",
  "dataSource":"sample_datasource",
  "intervals":["2013-01-01/2014-01-01"]
}
```

There are several main parts to a segment metadata query:

|property|description|required?|
|--------|-----------|---------|
|queryType|This String should always be "segmentMetadata"; this is the first thing Apache Druid looks at to figure out how to interpret the query|yes|
|dataSource|A String or Object defining the data source to query, very similar to a table in a relational database. See [DataSource](../querying/datasource.md) for more information.|yes|
|intervals|A JSON Object representing ISO-8601 Intervals. This defines the time ranges to run the query over.|no|
|toInclude|A JSON Object representing what columns should be included in the result. Defaults to "all".|no|
|merge|Merge all individual segment metadata results into a single result|no|
|context|See [Context](../querying/query-context.md)|no|
|analysisTypes|A list of Strings specifying what column properties (e.g. cardinality, size) should be calculated and returned in the result. Defaults to ["cardinality", "interval", "minmax"], but can be overridden with using the [segment metadata query config](../configuration/index.md#segmentmetadata-query-config). See section [analysisTypes](#analysistypes) for more details.|no|
|lenientAggregatorMerge|If true, and if the "aggregators" analysisType is enabled, aggregators will be merged leniently. See below for details.|no|

The format of the result is:

```json
[ {
  "id" : "some_id",
  "intervals" : [ "2013-05-13T00:00:00.000Z/2013-05-14T00:00:00.000Z" ],
  "columns" : {
    "__time" : { "type" : "LONG", "hasMultipleValues" : false, "hasNulls": false, "size" : 407240380, "cardinality" : null, "errorMessage" : null },
    "dim1" : { "type" : "STRING", "hasMultipleValues" : false, "hasNulls": false, "size" : 100000, "cardinality" : 1944, "errorMessage" : null },
    "dim2" : { "type" : "STRING", "hasMultipleValues" : true, "hasNulls": true, "size" : 100000, "cardinality" : 1504, "errorMessage" : null },
    "metric1" : { "type" : "FLOAT", "hasMultipleValues" : false, "hasNulls": false, "size" : 100000, "cardinality" : null, "errorMessage" : null }
  },
  "aggregators" : {
    "metric1" : { "type" : "longSum", "name" : "metric1", "fieldName" : "metric1" }
  },
  "queryGranularity" : {
    "type": "none"
  },
  "size" : 300000,
  "numRows" : 5000000
} ]
```

All columns contain a `typeSignature` that Druid uses to represent the column type information internally. The `typeSignature` is typically the same value used to identify the JSON type information at query or ingest time. One of: `STRING`, `FLOAT`, `DOUBLE`, `LONG`, or `COMPLEX<typeName>`, e.g. `COMPLEX<hyperUnique>`.

Columns also have a legacy `type` name. For some column types, the value may match the `typeSignature`  (`STRING`, `FLOAT`, `DOUBLE`, or `LONG`). For `COMPLEX` columns, the `type` only contains the name of the underlying complex type such as `hyperUnique`.

New applications should use `typeSignature`, not `type`.

If the `errorMessage` field is non-null, you should not trust the other fields in the response. Their contents are
undefined.

Only columns which are dictionary encoded (i.e., have type `STRING`) will have any cardinality. Rest of the columns (timestamp and metric columns) will show cardinality as `null`.

## intervals

If an interval is not specified, the query will use a default interval that spans a configurable period before the end time of the most recent segment.

The length of this default time period is set in the Broker configuration via:
  druid.query.segmentMetadata.defaultHistory

## toInclude

There are 3 types of toInclude objects.

### All

The grammar is as follows:

``` json
"toInclude": { "type": "all"}
```

### None

The grammar is as follows:

``` json
"toInclude": { "type": "none"}
```

### List

The grammar is as follows:

``` json
"toInclude": { "type": "list", "columns": [<string list of column names>]}
```

## analysisTypes

This is a list of properties that determines the amount of information returned about the columns, i.e. analyses to be performed on the columns.

By default, the "cardinality", "interval", and "minmax" types will be used. If a property is not needed, omitting it from this list will result in a more efficient query.

The default analysis types can be set in the Broker configuration via:
  `druid.query.segmentMetadata.defaultAnalysisTypes`

Types of column analyses are described below:

### cardinality

* `cardinality` is the number of unique values present in string columns. It is null for other column types.

Druid examines the size of string column dictionaries to compute the cardinality value. There is one dictionary per column per
segment. If `merge` is off (false), this reports the cardinality of each column of each segment individually. If
`merge` is on (true), this reports the highest cardinality encountered for a particular column across all relevant
segments.

### minmax

* Estimated min/max values for each column. Only reported for string columns.

### size

* `size` is the estimated total byte size as if the data were stored in text format. This is _not_ the actual storage
size of the column in Druid. If you want the actual storage size in bytes of a segment, look elsewhere. Some pointers:

- To get the storage size in bytes of an entire segment, check the `size` field in the
[`sys.segments` table](sql-metadata-tables.md#segments-table). This is the size of the memory-mappable content.
- To get the storage size in bytes of a particular column in a particular segment, unpack the segment and look at the
`meta.smoosh` file inside the archive. The difference between the third and fourth columns is the size in bytes.
Currently, there is no API for retrieving this information.

### interval

* `intervals` in the result will contain the list of intervals associated with the queried segments.

### timestampSpec

* `timestampSpec` in the result will contain timestampSpec of data stored in segments. this can be null if timestampSpec of segments was unknown or unmergeable (if merging is enabled).

### queryGranularity

* `queryGranularity` in the result will contain query granularity of data stored in segments. this can be null if query granularity of segments was unknown or unmergeable (if merging is enabled).

### aggregators

* `aggregators` in the result will contain the list of aggregators usable for querying metric columns. This may be
null if the aggregators are unknown or unmergeable (if merging is enabled).

* Merging can be strict or lenient. See *lenientAggregatorMerge* below for details.

* The form of the result is a map of column name to aggregator.

### rollup

* `rollup` in the result is true/false/null.
* When merging is enabled, if some are rollup, others are not, result is null.

## lenientAggregatorMerge

Conflicts between aggregator metadata across segments can occur if some segments have unknown aggregators, or if
two segments use incompatible aggregators for the same column (e.g. longSum changed to doubleSum).

Aggregators can be merged strictly (the default) or leniently. With strict merging, if there are any segments
with unknown aggregators, or any conflicts of any kind, the merged aggregators list will be `null`. With lenient
merging, segments with unknown aggregators will be ignored, and conflicts between aggregators will only null out
the aggregator for that particular column.

In particular, with lenient merging, it is possible for an individual column's aggregator to be `null`. This will not
occur with strict merging.
