---
id: ext
title: External Tables
sidebar_label: External Tables
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

## External Tables

Druid ingests data from external sources. Druid has three main ways to perform
ingestion:

* [MSQ](../multi-stage-query/reference.md#extern) (SQL-based ingestion)
* "Classic" [batch ingestion](../ingestion/ingestion-spec.md#ioconfig)
* Streaming ingestion

In each case, Druid reads from an external source defined by a triple of
input source, input format and (with MSQ) input signature. MSQ uses SQL
to define ingestion, and SQL works with tables. MSQ refers to
the external source an an _external table_. Druid external tables are much
like those of similar systems (such as Hive), except that Druid, as a
database, never reads the same data twice. This leads to the idea that
Druid external tables are most often _parameterized_: you provide at least
the names of the files (or objects) to read on each ingestion.

Druid provides three levels of parameterization:

* An ad-hoc table, such as those defined by the `extern` and similar
  table functions, in which you specify all attributes that define the table.
* A _partial table_, defined in the Druid catalog, that specifies some of the
  table information (perhaps the S3 configuration and perhaps the file format
  and column layout), leaving the rest to be provided via a named table function
  at ingest time.
* A fully-defined table in which all information is provided in the catalog, and
  the table acts like a regular SQL table at run time. (Such tables are generally
  useful in Druid for testing or when using MSQ to run queries. As noted above,
  this format is less useful for ingestion.)

See [MSQ](../multi-stage-query/reference.md) for how to define an ad-hoc table.
This section focuses on the use
of table metadata in the Druid catalog. An external table is just another kind
of table in Druid's catalog: you use the same REST API methods to define and
retrieve information about external tables. The catalog API does not differentiate
between external and datasource tables.

External tables do, however, have a unique set of properties. External tables
specifications reside in the `ext` schema within Druid.

## External Table Specificaation

An external table has the type `external`, and has two key properties:
`source` and `format`.

### `description` (String)

The description property holds human-readable text to describe the external table.
Druid does not yet use this property, though the intent is that the Druid Console
will display this text in some useful way.

```json
{
  "type" : "extern",
  "properties" : {
    "description" : "Logs from Apache web servers",
    ...
  },
}
```

### `source` (InputSource object)

The `source` property is the JSON serialialized form of a Druid `InputSource`
object as described on the [data formats page](../ingestion/data-formats.md).
The JSON here is identical to that
you would use as the first argument to an MSQ `extern` function. As explained
below, you will often leave off some of the properties in order to define
a partial table for ingestion. See the sections on each input source for examples.

### `format` (InputFormat object)

The `format` property is the JSON serialized form of a Druid `InputFormat`
object as described here (LINK NEEDED).  The JSON here is identical to that
you would use as the first argument to an MSQ `extern` function. The format
is optional. If provided in the table specification, then it need not be
repeated at ingest time. Use this choice when you have a set of
identically-formatted files you read on each ingest, such as reading today's
batch of web server log files in which the format itself stays constant.

Example:

```json
{
  "type" : "extern",
  "properties" : {
    "format" : {
      "type" : "csv",
      "listDelimiter" : ";"
    },
    ...
}
```

You can also omit the format, which case you must provide it at ingest time.
Use this option if you want to define a _connection_: say the properties of
an S3 bucket, but you store multiple file types and formats in that bucket,
and so have to provide the specific format at ingest time.

When you do provide a format, the format is complete. Unlike the source,
you either provide an entire `InputFormat` spec, or none at all.

### External Columns

The set of external table columns can be provided in the table specification
in the catalog entry, or at run time. To provide columns in the table
specification, provide both a name and a Druid type:

```json
{
  "type" : "extern",
  "properties" : {
    ...
  },
  "columns" : [ {
    "name" : "timetamp",
    "dataType" : "STRING"
  }, {
    "name" : "sendBytes",
    "dataType" : "LONG"
  } ]
}
```

### Using a Complete Table Specification

Suppose you want to define an external table for the Wikipedia data, which
is available on our local system at `$DRUID_HOME/quickstart/tutorial`. Assume
Druid is installed at `/Users/bob/druid` and that the data used
is compressed JSON. The complete table specification looks like this:

```json
{
  "type" : "extern",
  "properties" : {
    "format" : {
      "type" : "json",
      "keepNullColumns" : true,
      "assumeNewlineDelimited" : true,
      "useJsonNodeReader" : false
    },
    "description" : "Sample Wikipedia data",
    "source" : {
      "type" : "local",
      "baseDir" : "/Users/bob/druid/quickstart/tutorial",
      "filter" : "wikiticker-*-sampled.json"
    }
  },
  "columns" : [ {
    "name" : "timetamp",
    "dataType" : "STRING"
  }, {
    "name" : "page",
    "dataType" : "STRING"
  }, {
    "name" : "language",
    "dataType" : "STRING"
  }, {
    "name" : "unpatrolled",
    "dataType" : "STRING"
  }, {
    "name" : "newPage",
    "dataType" : "STRING"
  }, {
    "name" : "robot",
    "dataType" : "STRING"
  }, {
    "name" : "namespace",
    "dataType" : "LONG"
  }, ... {
    "name" : "added",
    "dataType" : "STRING"
  }, {
    "name" : "deleted",
    "dataType" : "LONG"
  }, {
    "name" : "delta",
    "dataType" : "LONG"
  } ]
}
```

You can use the above to create an external table called `wikipedia`.
As noted above, the table resides in the `ext` schema.

Then, you can ingest the data, into a datasource also named `wikipedia`, using MSQ as follows:

```sql
INSERT INTO wikipedia
SELECT *
FROM ext.wikipedia
```

The real ingestion would do some data transforms such as converting the "TRUE"/"FALSE" fields
into 1/0 values, parse the date-time field and so on. This section focuses on the table
metadata aspect.

### Using a Partial Table Specication

As noted, you can omit some of the information from the catalog entry to
produce a _partial_ table specification. Suppose you have an HTTP input source
that provides a set of files: the file contents differ each day, but all have
the same format and schema. Define the `koala` catalog table as follows:

```json
{
  "type" : "extern",
  "properties" : {
    "uriTemplate" : "https://example.com/{}",
    "format" : {
      "type" : "csv"
    },
    "description" : "Example parameterized external table",
    "source" : {
      "type" : "http",
      "httpAuthenticationUsername" : "bob",
      "httpAuthenticationPassword" : {
        "type" : "default",
        "password" : "secret"
      }
    }
  },
  "columns" : [ {
    "name" : "timetamp",
    "dataType" : "STRING"
  }, {
    "name" : "metric",
    "dataType" : "STRING"
  }, {
    "name" : "value",
    "dataType" : "LONG"
  } ]
}
```

Ingest table using the table name as if it were a function. Provide
the missing information:

```sql
INSERT INTO koalaMetrics
SELECT *
FROM TABLE(ext.koalas(uris => 'Dec05.json, Dec06.json'))
```

On the other hand, suppose you want to define something more like a
connection: we provide the location of a web server, and credentials, but
the web server hosts a variety of files. Suppose you have a `dataHub`
web site that holds a variety of files.

```json
{
  "type" : "extern",
  "properties" : {
    "description" : "Example connection",
    "source" : {
      "type" : "http",
      "uris" : [ "https://example.com/" ],
      "httpAuthenticationUsername" : "bob",
      "httpAuthenticationPassword" : {
        "type" : "default",
        "password" : "secret"
      }
    }
  },
  "columns" : [ ]
}
```

Providing the file names plus the format and schema in the ingest query:

```sql
SELECT *
FROM TABLE(ext.dataHub(
  uris => 'DecLogs.csv',
  format => 'csv'))
  (timestamp VARCHAR, host VARCHAR, requests BIGINT)
```

## Input Sources

An external table can use any of Druid's input sources, including any that extensions
provide. Druid's own input sources have a few extra features to allow parameterized
tables; extensions may provide such functions or not, depending on the extension.

### Inline Input Source

The [inline input source](../ingestion/native-batch-input-sources.md#inline-input-source)
 is normally used interally by the Druid SQL planner. It is
primarly useful with ingest as a simple way to experiment with the catalog and MSQ.
You can use the inline input source ad-hoc by using the `inline` table function.

You can also define an inline input source in the Druid catalog.
The inline input source cannot be parameterized: you must provide the complete input
specification, format specification and schema as part of the catalog definition.

### Local Input Source

Use the [local input source](../ingestion/native-batch-input-sources.md#local-input-source) to read files from the local file system. This form works
best for a simple, single-server setup. See (LINK NEEDED) for the available properties.

A local input source can be parameterized in one of two ways. In both, you provide only
the `baseDir` property in the table specification. You can provide a format and schema
in the catalog defintion, or at run time.

Then, provide either a file list, or a file pattern, at run time using these two parameters:

* `files`: A list of files, represented as a string with a comma-delimited set of names.
* `filter`: A pattern to use to match files.

For example:

```json
{
  "type" : "extern",
  "properties" : {
    "source" : {
      "type" : "local",
      "baseDir" : "/var/data"
    }
  },
  "columns" : [ ]
}
```

Then, to read a set of files:

```sql
INSERT INTO myTable
SELECT *
FROM TABLE(ext.dataDir(files => 'a.csv, b.csv'))
```

Or:

```sql
INSERT INTO myTable
SELECT *
FROM TABLE(ext.dataDir(filter => 'Dec*.csv'))
```

Again, in a real ingestion, you would insert expressions to parse the date, etc.

### HTTP Input Source

Use the [HTTP input source](../ingestion/native-batch-input-sources.md#local-input-source)
to access data on a web server.

HTTP input sources can be parameterized. The external table specification provides an
additional property to assist. `uriTemplate` is a top-level property. (That is, it is not
inside the HTTP input source JSON.) It contains a string that holds a URI, with a `{}`
placeholder. You provide the missing values at runtime. You can define the format and
schema, leaving only the details of the URI to be given at runtime:

```json
{
  "type" : "extern",
  "properties" : {
    "uriTemplate" : "https://example.com/{}",
    "format" : {
      "type" : "csv"
    },
    "description" : "Example parameterized external table",
    "source" : {
      "type" : "http",
      "httpAuthenticationUsername" : "bob",
      "httpAuthenticationPassword" : {
        "type" : "default",
        "password" : "secret"
      }
    }
  },
  "columns" : [ {
    "name" : "timetamp",
    "dataType" : "STRING"
  }, {
    "name" : "metric",
    "dataType" : "STRING"
  }, {
    "name" : "value",
    "dataType" : "LONG"
  } ]
}
```

Then, to read multiple resources using the template:

```sql
INSERT INTO myTable
SELECT *
FROM TABLE(ext.dataHub(uris => 'a.csv, b.csv'))
```

Or, you can provide just the URI template, with the format and columns given at run time:

```json
{
  "type" : "extern",
  "properties" : {
    "uriTemplate" : "https://example.com/{}",
    "source" : {
      "type" : "http",
      "httpAuthenticationUsername" : "bob",
      "httpAuthenticationPassword" : {
        "type" : "default",
        "password" : "secret"
      }
    }
  },
  "columns" : [ ]
}
```

Insert query:

```sql
INSERT INTO myTable
SELECT *
FROM TABLE(ext.dataHub(uris => 'a.csv, b.csv', format => 'csv'))
     (timestamp VARCHAR, metric VARCHAR, value BIGINT)
```

Credentials, if needed, must be part of the table specification. When using the catalog,
you cannot provide credentials at run time.

### S3 Input Source

## Input Formats

An external table can use any supported Druid format, See
[Data formats](../ingestion/data-formats.md) for details.
Provide the format as the serialized JSON form.

Example:

```json
  "type" : "extern",
  "properties" : {
    "format" : {
      "type" : "csv"
    },
    ...
```

The JSON is the same as you would use with the
[MSQ `extern` function](../multi-stage-query/reference.md#extern).

When the format is to be provided at runtime, use the same function arguments
as described for the [ad-hoc table functions](../multi-stage-query/reference.md#HTTP-INLINE-and-LOCALFILES).

Example:

```sql
INSERT INTO myTable
SELECT ...
FROM TABLE(
  http(
    userName => 'bob',
    password => 'secret',
    uris => 'http:foo.com/bar.csv',
    format => 'csv'
    )
  ) EXTEND (x VARCHAR, y VARCHAR, z BIGINT)
```
