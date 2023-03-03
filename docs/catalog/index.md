---
id: index
title: Table Metadata Catalog
sidebar_label: Introduction
description: Introduces the table metadata features
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

Druid 26.0 introduces the idea of _metadata_ to describe datasources and external
tables. External table metadata simplifies MSQ ingestion statements. Datasource
metadata gives you another data governance tool to manage and communicate the data
stored in a datasource. The user of table metadata is entirely optional: you can
use it for no tables, some tables or all tables as your needs evolve.

The table metadata feature consists of the main parts:

* A new table in Druid's catalog (also known as the metadata store),
* A set of new REST APIs to manage table metadata entries,
* SQL integration to put the metadata to work.

Table metadata is an "alpha" feature in this release. We encourage you to try
it and to provide feedback. However, since the feature is new, some parts
may change based on feedback. The feature will continue to evolve over time.
See [Limitations](#Limitations) for the current scope of the feature.

In this section, we use the term "table" to refer to both datasources and external
tables. To SQL, these are both tables, and the table metadata feature treats them
similarly.

## Contents

- [Introduction](./index.md) - Overview of the table metadata feature.
- [Metadata API](./api.md) - Reference for the REST API operations to create and modify metadata.
- [Datasource Metadata](./datasource.md) - Details for datasource metadata and partial tables.
- [External Tables](./ext.md) - Details about external tables used in MSQ ingestion.


## Enable Table Metadata Storage

Table metadata storage resides in an extension which is not loaded by default. Instead,
you must explicitly "opt in" to using this feature. Table metadata store is required
for the other features described in this section.

Enable table metadata storage by adding `druid-catalog` to your list of loaded
extensions in `_common/common.runtime.properties`:

```text
druid.extensions.loadList=[..., "druid-catalog"]
```

Enabing this extension causes Druid to create the table metadata table and to
make the table metadata REST APIs available for use.

## When to Use Table Metadata

People use Druid in many ways. When learning Druid, we often load data just to
see how Druid works and to experiment. For this, "classic" Druid is ideal: just
load data and go: there is no need for additional datasource metadata.

As we put Druid to work, we craft an application around Druid: a data pipeline
that feeds data into Druid, UI (dashboards, applications) which pull data out,
and deployment scripts to keep the system running. At this point, it can be
useful to design the table schema with as much care as the rest of the system.
What columns are needed? What column names will be the most clear to the end
user? What data type should each column have?

Historically, developers encoded these decisions into Druid ingestion specs.
With the multi-stage query extension (MSQ), that information is encoded into
SQL statements. To control changes,
the specs or SQL statements are versioned in Git, subject to reviews when changes
occur.

This approach works well when there is a single source of data for each datasource.
It can become difficult to manage when we want to load multiple, different inputs
into a single datasource, and we want to create a single, composite schema that
represents all of them. Think, for example, of a metrics application in which
data arrives from various metrics feeds such as logs and so on. We want to decide
upon a common set of columns so that, say HTTP status is always stored as a string
in a column called "http_status", even if some of the inputs use names such as
"status", "httpCode", or "statusCode". This is, in fact, the classic [ETL
(extract, transform and load)](https://en.wikipedia.org/wiki/Extract,_transform,_load)
design challenge.

In this case, having a known, defined datasource schema becomes essential. The job
of each of many ingestion specs or MSQ queries is to transform the incoming data from
the format in which it appears into the form we've designed for our datasource.
While table metadata is generally useful, it is this many-to-one mapping scenario
where it finds its greatest need.

### Table Metadata is Optional

Table metadata is a new feature. Existing Druid systems have physical datasource
metadata, but no logical data. As a result, table metdata can be thought of as a set
of "hints": optional information that, if present, provides additional features not
available when only using the existing physical metadata. See the
[datasource section](./datasource.md) for information on the use of metadata with
datasources. Without external metadata, you can use the existing `extern()` table
function to specify an MSQ input source, format and schema.

### Table Metadata is Extensible

This documentation explains the Druid-defined properties available for datasources
and external tables. The metadata system is extensible: you can define other
properties, as long as those properties use names distinct from the Druid-defined
names. For example, you might want to track the source of the data, or might want
to include display name or display format properties to help your UI display
columns.

## Overview of Table Metadata

Here is a typical scenario to explain how table metadata works. Lets say we want
to create a new datasource. (Although you can use table metadata with an existing
datasource, this explaination will focus on a new one.) We know our input data is
not quite what dashboard users expect, so we want to clean things up a bit.

### Design the Datasource

We start with pencil and paper (actually, a text editor or spreadsheet) and we
list the input columns and their types. Next, we think about the name we want to
appear for that column in our UI, and the data type we'd prefer. If we are going
to use Druid's rollup feature, we also want to identify the aggregation we wish
to use. The result might look like this:

```text
Input source: web-metrics.csv
Datasource: webMetrics

    Input Column               Datasource Column
Name          Type        Name       Type      Aggregation
----          ----        ----       ----      -----------
timestamp     ISO string  __time     TIMESTAMP
host          string      host       VARCHAR
referer       string
status        int         httpStatus VARCHAR
request-size  int         bytesIn    BIGINT    SUM
response-size int         bytesOut   BIGINT    SUM
...
```

Here, we've decided we don't need the referer. For our metrics use, the referer
might not be helpful, so we just ignore it.

Notice that we use arbitrary types for the CSV input. CSV doesn't really have
types. We'll need to convert these to SQL types later. For datasource columns,
we use SQL types, not Druid types. There is a SQL type for each Druid type and
Druid aggregation. See [Datasource metadata](./datasource.md) for details.

For aggregates, we use a set of special aggregate types described [here](./datasource.md).
In our case, we use `SUM(BIGINT)` for the aggregate types.

If we are using rollup, we may want to truncate our timestamp. Druid rolls up
to the millsecond level by default. Perhaps we want to roll up to the second
level (that is, combine all data for a given host and status that occurs within
any one second of time.) We do by specifying a parameter to the time data type:
`TIMESTAMP('PT1S')`.

A Druid datasource needs two additional properties: our time partitioning
and clustering. [Time partitioning](../multi-stage-query/reference.html#partitioned-by)
gives the size of the time chunk stored in each segment file. Perhaps our system is of
moderate size, and we want our segments to hold a day of data, or `P1D`.

We can also choose [clustering](../multi-stage-query/reference.html#clustered-by), which
is handy as data sizes expand. Suppose we want to cluster on `host`.

We have the option of "sealing" the schema, meaning that we _only_ want these columns
to be ingested, but no others. We'll do that here. The default option is to be
unsealed, meaning that ingestion can add new columns at will without the need to first
declare them in the schema. Use the option that works best for your use case.

We are now ready to define our datasource metadata. We can craft a JSON payload with the
information (handy if we write a script to do the translation work.) Or, we
can use the Druid web console. Either way, we create a metadata entry for
our `webMetrics` datasource that contains the columns and types above.

Here's an example of the JSON payload:

```JSON
TO DO
```

If using the REST API, we create the table using the (TODO) API (NEED LINK).

### Define our Input Data

We plan to load new data every day, just after midnight, so our ingestion frequency
matches our time partitioning. (As our system grows, we will probably ingest more
frequently, and perhaps shift to streaming ingestion, but let's start simple.)
We don't want to repeat the same information on each ingest. Instead, we want to
store that information once and reuse it.

To make things simple, let's assume that Druid runs on only one server, so we can
load from a local directory. Let's say we always put the data in `/var/metrics/web`
and we name our directories with the date: `2022-12-02`, etc. Each directory has any
number of files for that day. Let's further suppose that we know that our inputs are
always CSV, and always have the columns we listed above.

We model this in Druid by defining an external table that includes everything about
the input _except_ the set of files (which change each day). Let's call our external
table `webInput`. All external tables reside in the `ext` schema (namespace).

We again use the (TODO) API, this time with a JSON payload for the eternal table.
The bulk of the information is simliar to what we use in an MSQ `extern` function:
the JSON serialized form of an input source and format. But, the input source is
"partial", we leave off the actual files.

Notice we use SQL types when defining the eternal table. Druid uses this information
to parse the CSV file coluns into the desired SQL (and Druid) type.

```JSON
TO DO
```

### MSQ Ingest

Our next step is to load some data. We'll use MSQ. We've defined our datasource, and
the input files from which we'll read. We use MSQ to define the mapping from the
input to the output.

```sql
INSERT INTO webMetrics
SELECT
  TIME_PARSE(timestamp) AS __time,
  host,
  CAST(status AS VARCHAR) AS httpStatus,
  SUM("request-size") AS bytesIn,
  SUM("response-size") AS bytesOut
FROM ext.webInput(filePattern => '2202-12-02/*.csv')
GROUP BY __time, host, status
```

Some things to notice:

* Druid's `INSERT` statement has a special way to match columns. Unlike standard SQL,
which matches by position, Druid SQL matches by name. Thus, we must use `AS` clauses
to ensure our data has the same name as the column in our datasource.
* We do not include a `TIME_FLOOR` function because Druid inserts that automatically
based on the `TIMESTAMP('PT5M')` data type.
* We include a `CAST` to convert `status` (a number) to `httpStatus`
(as string). Druid will validate the type based on the table metadata.
* We include the aggregations for the two aggregated fields.
The aggregations must match those defined in table metadata.
* We also must explicitly spell out the `GROUP BY` clause which must include all
non-aggregated columns.
* Notice that there is no `PARTITIONED BY` or `CLUSTERED BY` clauses: Druid fills those
in from table metadata.

If we make a mistake, and omit a required column alias, or use the wrong alias, the
MSQ query will fail. This is part of the data governance idea: Druid just prevented us
from creating segments that don't satisfy our declared table schema.

### Query and Schema Evolution

We can now query our data. Though it is not obvious in this example, the query experience
is also guided by table metadata. To see this, suppose that we soon discover that no one
actually needs the `bytesIn` field: our server doesn't accept `POST` requets and so all
requests are pretty small. To avoid confusing users, we want to remove that column. But,
we've already ingested data. We could throw away the data and start over. Or, we could
simply "hide" the unwanted column using the
[hide columns](api.md) API.

If we now do a `SELECT *` query we find that `bytesIn` is no longer available and we
no longer have to explain to our users to ignore that column. While hiding a column is
trivial in this case, imagine if you wanted to make that decision after loading a petabyte
of data. Hiding a column gives you an immediate solution, even if it might take a long
time to rewrite (compact) all the existing segments to physically remove the column.

We have to change our MSQ ingest statement also to avoid loading any more of the
now-unused column. This can happen at any time: MSQ ingest won't fail if we try to ingest
into a hidden column.

In a similar way, if we realize that we actually want `httpStatus` to be `BIGINT`, we
can change the type, again using the [REST API](api.md).

New ingestions will use the new type. Again, it may be costly to change existing data.
Druid will, however, automatically `CAST` the old `VARCHAR` (string) data to the
new `BIGINT` type, allow us to make the change immediately.

## Details

Now that we understand what table metadata is, and how we might use it, we are ready
to dive into the technical reference material which follows.

## Limitations

This is the first version of the catalog feature. A number of limitations are known:

* When using the catalog to specify column types for a table ingested via MSQ, the
  resulting types are determined by the `SELECT` list, not the table schema. Druid will
  ensure that the actual type is compatible with the declared type, the the actual type
  may still differ from the declared type. Use a `CAST` to ensure the types are as
  desired.
* The present version supports only the S3 cloud data source.
* Catalog information is used only for MSQ ingestion, but not for classic batch or
  Druid streaming ingestion. Catalog column types are reflected in queries.
* Catalog store is in an extension. Enable that extension as described above to try
  the catalog feature.
* Support for roll-up tables and metrics is limited.
* The catalog is currently integrated only with SQL queries and MSQ integestion.
  Native batch ingestion, streaming ingestion and native queries do not yet make
  use of the catalog.
