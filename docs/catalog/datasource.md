---
id: intro
title: Table Metadata Catalog
sidebar_label: Intro
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

# Datasource Metadata

Datasources are collections of segments. The Druid catalog (also called the
Druid metadata store) provides both _physical_ and _logical_ infrormation about
datasources. The physical information describes existing segments: their columns,
load status and so on. The logical information describes how you'd like the
data source to be shaped, even if no segments yet exist for that datasource.

With logical table metadata, you can define a table (datasource) before any data
is loaded. Once the logical table definition exists, you can immediately query your
datasource, though, of course, it will contain zero rows. Further,
the table will appear in Druid's system tables so that applications can
learn about the table.

## Creating a New Datasource

You can create a datasource directly via ingestion even if no metadata already
exists for that datasource. As described in the introduction, this can be handy
to learn Druid, or to prototype a new application.

When you create a datasource for a production system, it is best to start by
defining the datasource structure using table metadata. As noted above, your new
datasource "exists" (is visible to queries, appears in system tables) as soon as
you create your logical definition. This allows you to double-check that the schema
matches your input data and the needs of your application.

## Adding Metadata to an Existing Datasource

If you have an existing datasource, you can still use table metadata. A table without
metadata acts as it always has: the ingest defines the schema of new segments, and
queries use the schema of the most recently created column. You can add metadata to
an existing datasource. Likely you will define a _partial schema_: you'll specify
metadata for some columns, but not others. The rules are:

* A datasource with no metadata follows the existing physical schema rules.
* A datasource with an empty table specification works the same as one with no table
  specification at all.
* A datasource with some properties or columns set enforces those settings, but not
  others.
* A datasource with complete metadata works like an RDBMS table: new ingestions must
  comply with the rules set in metadata, and queries use the types defined in metadata.

When adding metadata to an existing data source, you will move through these various
states, stopping when you have defined enough metadata to solve the problem at hand.

## MSQ Ingestion

The Druid Multi-Stage Query (MSQ) ingestion engine uses SQL to drive the ingestion of new data.
MSQ SQL follows standard SQL rules, with a few exceptions unique to Druid.

* If a column is defined in the logical metadata, then that column in a new segment will be
  of the type declared in the table metadata.
* The column definition and type are optional. If not specified, then MSQ will determine the
  column type based on the type of the column in the input, along with any expressions that
  you provide in SQL.
* The table can specify a time partition option. If provided, then MSQ will use that option
  and you should not include a `PARTITIONED BY` clause in your MSQ `INSERT` statement.
* Similarly, the table can specify a clustering option. If provided, then MSQ will use that
  option and you should not include a `CLUSTERED BY` clause in your MSQ `INSERT` statement.

## Classic Batch and Streaming Ingestion

Note that in this release, the table metadata feature has not yet been integrated into either
the classic batch ingest (which uses ingest specs) or the streaming ingest (using supervisiors.)
Nor has the feature been integrated into auto-compaction. (However, auto-compaction should still
pick up the proper table schema using existing mechanisms.)

## Querying

When you query a table that has logical metadata defined, then the metadata controls the
query output.

* Columns defined in the logical metadata appear in a `SELECT *` in the order in which
  you list the columns in the logical metadata. This means that the metadata lets you control
  column ordering.
* If the datasource contains columns other than those that appear in the logical metadata,
  then those columns appear in a `SELECT *` after those defined in the logical metadata. These "extra"
  columns appear in the same order that they appear when no metadata exists.
* If the logical metadata defines a column which exists in no segments, then you can still
  query the column. The column will return `NULL` (or default) values for every row.
* You can use table metadata to hide columns. If so, those columns act in a query as if they
  never existed: they do not appear in a `SELECT *` query, nor can you explicitly request them
  in a `SELECT a, b, c` style query.

## Datasource Properties

Druid defines the following properties for a datasource. As noted earlier, you are free to
add other ad-hoc properties of use to your application. Druid ignores such properties.

### `description` (String)

The description property hold human-readable text to describe the datasource. Druid does not
yet use this property, though the intent is that the Druid Console will display this text
in some useful way.

```json
{
  "type" : "datasource",
  "properties" : {
     "description" : "Web server performance metrics"
  }
}
```

### `segmentGranularity` (String)

Defines the primary partitioning for the datasources.
Tells MSQ the size of the time chunk that each segment is to cover. This is the equivalent of
the MSQ `PARTITIONED BY` clause. Provide the granularity
as an ISO 8601 time period format that matches one of Druid's supported segment granularities.
For example, `PT1H` or `P1D`. Once you specify this property, you no longer need the
`PARTITIONED BY` clause.

Example:

```json
{
  "type" : "datasource",
  "properties" : {
     "segmentGranularity" : "PT1H"
  }
}
```

### `clusterKeys` (List of Objects)

This property identifies the secondary partitioning for a datasource. This property is optional
if you do not need secondary partitioning. Else, it is a list of pairs of column names and
sort directions. Ascending is the default if you do not specify a direction.

Example:

```json
{
  "type" : "datasource",
  "properties" : {
    "clusterKeys" : [ {
      "column" : "a"
    }, {
      "column" : "b",
      "desc" : true
    } ]
  }
}
```

The two fields per cluster key are:

* `column`: the name of a column. (The name need not be defined in table metadata, but it
  must exist as part of the MSQ query at ingest time.)
* `desc`: a Boolean value. `true` means descending, `false` (or omitted) means ascending.

### `targetSegmentRows` (Integer)

Druid normally creates segments with 5 million rows. However, if your rows are especially small, you
may find it useful to create segments with more rows. If your rows are unusually large, then you
may want to create segments with fewer rows. Use this property to control the target row count.
(Actual segments will be limited by actual data, and will be of roughly the requested size.)
Omit this property unless you have a special need: Druid will use its default.

Example:


```json
{
  "type" : "datasource",
  "properties" : {
     "segmentGranularity" : "PT1H"
  }
}
```

### `hiddenColumns` (List of Strings)

Druid does not provide an easy way to drop columns from existing datasources. Dropping a column
can be done with compaction specs, but is awkward and costly for large data sets. If your data
has a limited lifetime (that is, it expires after a month or two), then there is a simpler, more
efficient solution: simply hide the existing columns. Hidden columns will not appear in SQL
queries, though the data still exists and is visible to native queries. You hide a column by
adding it to the `hiddenColumns` list.

Example:

```json
  "properties": {
    "targetSegmentRows": 1000000
  }
```

### `sealed` (Boolean)

In many databases, one can insert data only into columns that are already defined in the
table schema. Druid is unique in that each ingestion can define new columns not already
in any existing segment. The `sealed` property, when set to `true`, makes a Druid
datasource work like an RDBMS table: insertion can only occur into columns defined in
the table schema. Use this option when the schema has been curated, and the goal is for
ingestion queries to map to an existing set of columns. The default is `false`, meaning
that MSQ will add new columns as they occur.

This property prevents MSQ form accidentally creating new columns not in the schema.
Use column properties below to ensure that, for existing columns, MSQ uses the desired
column type.

Example:

```json
  "properties" : {
    "sealed" : true,
  }
```

## Datasource Columns

As noted above, columns are optional. When provided, they appear as a list under the
`columns` property of a datasource. Not all columns need be declared. Each column is a JSON
object with three fields: `name`, `sqlType` and `properties`. Example:

```json
{
  "type" : "datasource",
  "columns" : [ {
    "name" : "__time",
    "sqlType" : "TIMESTAMP"
  }, {
    "name" : "host",
    "sqlType" : "VARCHAR",
    "properties" : {
      "description" : "The web server host"
    }
  }, {
    "name" : "bytesSent",
    "sqlType" : "BIGINT",
    "properties" : {
      "description" : "Number of response bytes sent"
    }
  } ]
}
```

Names must follow Druid's usual naming rules. Column types are given using SQL (not Druid)
types. That is:

| SQL Type | Druid Type |
| -------- | ---------- |
| `TIMESTAMP` | `long` |
| `VARCHAR` | `string` |
| `BIGINT`  | `long` |
| `FLOAT` | `float` |
| `DOUBLE` | `double` |

The use of other SQL types will result in an error. For example, if you use the SQL types
`INT` or `INTEGER`, you'll get an error because Druid does not support a 4-byte integral
type.

Future versions will define measure types (as suggesed in the example above.)

If you define the `__time` column, it must be of type `TIMESTAMP`. (The time column always
exists in segments. The metadata definition is optional.)

## Changing Properties or Columns

You can modify a datasource's metadata after the datasource exists. Since Druid is a
distributed, eventually-consistent system, Druid's schema change rules are different from
those of a traditonal RDBMS. Druid's metadata acts like a "hint" on top of the physical
segment structure.

* You can add a column at any time. The column is available to query immediately. It
  will affect an MSQ ingestion that starts after you define the column. The definition
  has no affect on in-flight ingestions or existing segments. (That is, Druid will not
  rewrite existing segments to include the new column. There is no need, since all such
  values would be `NULL` (or the default value.)
* You can reorder columns at any time. Order has no affect on ingest. (Columns within
  segments have no implied order.) The new order will immediately be reflected in
  `SELECT *` queries.
* You can hide a column by adding it to the `hiddenColumns` property. The change takes
  effect on the next query after the change.
* You can remove a column entry from metadata. Doing so _does not_ drop or hide the
  column. Instead, it simply says to use the physical column data from segments, and to not
  constrain type of the column on ingest.
* Changing primary or secondary partitioning affects any MSQ ingest task that starts
  after the change: it does not change any existing segments or affect in-flight ingest
  tasks.

## Dropping a Datasource

If you find you need to drop a datasource, and that datasource has metadata, then you must
both physically drop the datasource and delete the metadata for that datasource. Of course,
if you simply want to "truncate" the table (remove all existing data), but keep the table
definition, then just delete the segments. The table itself will continue to exist as long
as the table metadata exists. (That is, Druid works as if you had just created a new table.)
