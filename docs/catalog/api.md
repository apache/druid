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

The table metadata feature adds a number of new REST APIs to create, read, update and delete
(CRUD) table metadata entries. Table metadata is a set of "hints" on top of physical datasources, and
a set of convenience entries for external tables. Changing the metadata "hints" has no effect
on existing datasources or segments, or on in-flight ingestion jobs. Metadata changes only
affect future queries and integestion jobs.

**Note**: a future enhancment might allow a single operation to, say, delete both a metadata
entry and the actual datasource. That functionality is not yet available. In this version, each
metadata operation affects _only_ the information in the table metadata table within Druid's
metadata store.

A metadata table entry is called a "table specification" (or "table spec") and is represented
by a Java class called `TableSpec`. The JSON representation of this object appears below.

The API calls are designed for two use cases:

* Configuration-as-code, in which the application uploads a complete table spec. Like in
Kubernetes or other deployment tools, the source of truth for specs is in a source repostitory,
typically Git. The user deploys new table specs to Druid as part of rolling out an updated
application.
* UI-driven, in which you use the Druid Console or similar tool to edit the table metadata
directly.

There are two ways to perform updates. In the first way, simply overwrite any existing spec.
Use this for the configuration-as-code use case: whatever is in your code repo is the
ultimate source of truth.

The other way to perform updates is with "optimistic locking", mostly for use in UI code.
Druid is a distributed, multi-user system: it could be that multiple users edit the same
table at the same time. To avoid accidential overwrites, each UI retrieves the current spec
along with the version number of that spec. The user applies some set of changes. The UI
then sends the updated spec, along with the version number, to the server. The server will
check if the version number in the request matches that of the spec in the metadata store.
If so, the change is applied atomically. If the versions do not match, the server rejects
the change as it represents an update to a stale version of the spec. The UI can request
the latest version, re-apply the changes, and submit again.

For many simple operations, the optimistic locking method is overkill: the user wants to
apply one task (add a column, drop a column, etc.) Some of these operations have special
API methods to "edit" a record atomically. Druid, rather than the UI client, applies the
chanage to avoid conflicts elsewhere in the spec.

All table metadata APIs begin with a common prefix: `/druid/coordinator/v1/catalog`.

Tables are identified by a path: the schema and table name. Druid has a fixed, pre-defined set
of schemas:

* `druid`: Datasources
* `ext`: External tables

There are others, but those do not accept table metadata entries.

## Configuration-as-Code

CRUD for an entire `TableSpec`, optionally protected by a version.

### `POST {prefix}/schemas/{schema}/tables/{table}[?version={n}|overwrite=true|false]`

Create or update a `TableSpec`.

* With no options, the semantics are "create if not exists."
* With a version, the semantics are "update if exists and is at the given version"
* With "overwrite", the semantics are "create or update"

Use `overwrite=true` when some other system is the source of truth: you want to force
the table metadata to match. Otherwise, first read the existing metadata, which provides
the data version. Apply changes and set `version={n}` to the version retrieved. Druid will
reject the change it other changes have occurred since the data was read. The client should
read the new version, reapply the changes, and submit the change again.

### `DELETE {prefix}/schemas/{schema}/tables/{table}`

Removes the metadata for the given table. Note that this API _does not_ affect the datasource itself
or any segments for the datasource. Deleting a table metadata entry simply says that you no longer
wish to use the data governance features for that datasource.

## Editing

Edit operations apply a specific transform to an existing table spec. Because the transform is very
specific, there is no need for a version as there is little scope for unintentional overwrites. The
edit actions are specficially designed to be use by UI tools such as the Druid Console.

### `POST {prefix}/schemas/{schema}/tables/{table}/edit`

The payload is a message that says what to change:

* Hide columns (add to the hidden columns list)
* Unhide columns (remove from the hidden columns list)
* Drop columns (remove items from the columns list)
* Move columns
* Update props (merge updated properties with existing)
* Update columns (merge updated columns with existing)

## Retrieval

Finally, the API provides a variety of ways to pull information from the table metadata
storage. Again, the information provided here is _only_ the metadata hints. Query the
Druid system tables to get the "effective" schema for all Druid datasources. The effective
schema includes not just the tables and columns defined in metadata, but any additional
items that physically exist in segments.

### `GET {prefix}/schemas[?format=name|path|metadata]`

Returns one of:

* `name`: The list of schema names (Default)
* `path`: The list of all table paths (i.e. `{schema}.{table}` pairs)
* `metadata`: A list of metadata for all tables in all schemas.

### `GET {prefix}/schemas/{schema}`

Not supported. Reserved to obtain information about the schema itself.

### `GET {prefix}/schemas/{schema}/tables[?format=name|metadata`]

Returns one of:

* `name`: The list of table names within the schema.
* `metadata`: The list of `TableMetadata` for each table within the schema.

### `GET {prefix}/schemas/{schema}/tables/{table}[?format=spec|metadata|status`

Retrieves information for one table. Formats are:

* `spec`: The `TableSpec` (default)
* `metadata`: the `TableMetadata`
* `status`: the `TableMetadata` without the spec (that is, the name, state, creation date and update date)

### Synchronization

Druid uses two additional APIs internally to synchronize table metadata. The Coordinator
stores the metadata, and services the APIs described above. The Broker uses the metadata
to plan queries. These synchronization API keep the Broker in sync with the Coordinator.
You should never need to use these (unless you are also creating a synchronization solution),
but you may want to know about them to understand traffic beteen Druid servers.

### `GET /sync/tables`

 Returns a list of `TableSpec` objects for bootstrapping a Broker.

Similar to`GET /entry/tables`, but that message returns metadata.

### `POST /sync/delta`

On the broker, receives update (delta) messages from the coordinator. The payload is an object:

* Create/update: table path plus the `TableSpec`
* Delete: table path only

## Table Specification

You submit a table spec when creating or updating a table metadataentry.
The table spec object consists of four parts:

* `type`: the kind of table described by the spec
* `properties`: the set of properties for the table (see below)
* `columns`: the set of columns for the table.

Example:

```json
"spec": {
  "type": "<type>",
  "properties": { ... },
  "columns": [ ... ]
}
```

### Datasource Table Spec

The syntax of a datasource table spec follows the general pattern. A datasource is
described by a set of properties, as defined (LINK NEEDED). The type of a datasource
table is `datasource`. Columns are of type `column`.

Example:

```json
{
   "type":"datasource",
   "properties": {
     "description": "<text>",
     "segmentGranularity": "<period>",
     "targetSegmentRows": <number>,
     "clusterKeys": [ { "column": "<name">, "desc": true|false } ... ],
     "hiddenColumns: [ "<name>" ... ]
   },
   "columns": [ ... ],
}
```

### External Table Spec

An external table spec has the same basic structure. Most external tables have just
two properties:

`source`: The JSON-serialized form of a Druid input source.
`format`: The JSON-serialized form of a Druid input format.

These two properties directly correspond to the JSON you would use with the MSQ
`extern` statement. (In the catalog, the third argument, signature, is computed
from the set of columns in the table spec.)

See (LINK NEEDED) for details on the available input sources and formats, and on
how to define a partial table completed with additional information at ingest time.

### Table Metadata

Druid returns a table metadata entry when you retrieve data for one or more tables.
Each instance includes the table spec plus additional "meta-metadata" about the entry.

* `id`: a map of two fields that gives the table path.
  * `schema`: one of the Druid-supported schemas described above.
  * `name`: table name
* `creationTime`: UTC Timestamp (in milliseconds since the epoch) of the original creation
time of the metadata entry (not actual datasource) was created.
* `updateTime`: UTC Timestamp (in milliseconds since the epoch) of the most recent creation
or update. This is the record's "version" when using optimistic locking. Return this in
the `version={updateTime}` field when doing an update using optimistic locking.
* `state`: For datasources. Normally ACTIVE. May be DELETING if datasource deletion is in
progress. (Not yet supported; reserved for future use.)
* `spec`: The user-defined table specification as described above.

Example:

```json
{
   "id": {
     "schema":"druid",
     "name":"myTable"
   },
   "creationTime":1654634106432,
   "updateTime":1654634106432,
   "state":"ACTIVE",
   "spec": <TableSpec>
   }
}
```

## Security

The table metadata follows the same rules as Druid itself.

* A user can read any resource for which they have `READ` permission.
* A user can update any resource for which they have `WRITE` permission.

The resource names are the same as those used for validating queries.

