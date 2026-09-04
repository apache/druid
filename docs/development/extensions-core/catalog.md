---
id: catalog
title: Catalog
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

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Consider this an [EXPERIMENTAL](../experimental.md) feature mostly because it has not been tested yet on a wide variety of long running Druid clusters.

This extension allows users to configure, update, retrieve, and manage metadata stored in Druid's catalog. At present, only metadata about tables is stored in the catalog. This extension only supports MSQ based ingestion.

## Configuration

To use this extension please make sure to  [include](../../configuration/extensions.md#loading-extensions) `druid-catalog` in the extensions load list.

# Catalog Metadata

## Tables

A user may define a table with a defined set of column names, and respective data types, along with other properties. When
ingesting data into a table defined in the catalog, the DML query is validated against the definition of the table
as defined in the catalog. This allows the user to omit the table's properties that are found in its definition,
allowing queries to be more concise, and simpler to write. This also allows the user to ensure that the type of data being
written into a defined column of the table is consistent with that columns definition, minimizing errors where unexpected
data is written into a particular column of the table.

### Effects of a table definition

A table definition takes effect when it is read, not when it is written: changing one, whether through SQL DDL or the
REST API, rewrites nothing. What it affects:

- **SELECT queries** report each declared column with its declared type, in declared order. Segments that store a
  column as a different physical type are converted to the declared type at query time. Columns present in segments
  but not declared in the catalog remain queryable, and follow the declared columns with their physical types.
- **INSERT and REPLACE** (the catalog applies only to SQL-based ingestion) validate against the definition rather
  than merely defaulting from it. A query column targeting a declared column must be convertible to the declared type
  without changing it: inserting a `BIGINT` value into a column declared `VARCHAR` stores the value as a string, while
  inserting a `VARCHAR` value into a column declared `BIGINT` is an error. Columns the query produces that the table
  does not declare are rejected when the table is [`sealed`](#table-properties) and ingested normally otherwise.
  Table properties such as `segmentGranularity` and `clusterKeys` act as defaults that an individual statement may
  override, such as with its own `PARTITIONED BY`.
- **Streaming and native batch ingestion** do not consult the catalog; their specs are unaffected by any table
  definition.

### SQL DDL

Tables can be defined with SQL instead of by posting a table specification. `CREATE TABLE` and `ALTER TABLE` are
submitted to the Broker like any other SQL statement, and write the same catalog metadata the REST API does. They
return no rows.

These statements change catalog metadata only. They never create, modify, or delete segments: defining a table does
not ingest anything, and altering a column does not rewrite existing data. Changes take effect for subsequent queries
and ingestion, as described in [Effects of a table definition](#effects-of-a-table-definition).

These statements are disabled by default. Set `druid.sql.planner.enableCatalogDdl` to `true` on the Broker to enable
them. They require both `READ` and `WRITE` permission on the datasource, the same permissions the catalog API
requires for its write operations, so enabling them lets anyone who can ingest into a datasource also change its
catalog definition; leave them disabled if you manage catalog entries with your own tooling. The setting cannot be
overridden per query.

The `druid-catalog` extension must be loaded on both the Broker and the Coordinator; without it, these statements
report that the extension is not available.

```sql
CREATE [OR REPLACE] TABLE [IF NOT EXISTS] <table>
  [ ( <table element> [, ...] ) ]
  [ PARTITIONED BY <granularity> ]
  [ CLUSTERED BY <column> [, ...] ]
  [ SEALED ]

<table element> ::=
    <column> <type>
  | PROJECTION <name> AS ( <select> )
```

`OR REPLACE` replaces the specification of an existing table; `IF NOT EXISTS` leaves an existing table unchanged.
The two cannot be combined. `PARTITIONED BY` sets [`segmentGranularity`](#table-properties) and `CLUSTERED BY` sets
`clusterKeys`, both of which a later `INSERT` or `REPLACE` inherits unless it states its own. `SEALED` sets
[`sealed`](#table-properties), which requires every ingested column to be declared.

Note that the table-level `CLUSTERED BY` is a sort order applied to each ingestion, which is a different thing from
the `CLUSTERED BY` inside a [`__base` projection](#the-base-table), which defines how segments physically group rows.

Column types are written as SQL types, such as `VARCHAR`, `BIGINT`, `DOUBLE`, or `VARCHAR ARRAY`. The `__time` column
is written as `TIMESTAMP`. Types that have no SQL spelling, such as complex types, use `TYPE('...')` with the Druid
native type string:

```sql
CREATE TABLE "druid"."visits" (
  __time TIMESTAMP,
  user_id VARCHAR,
  pages_visited BIGINT,
  sketch TYPE('COMPLEX<thetaSketch>')
)
PARTITIONED BY DAY
CLUSTERED BY user_id
```

`ALTER TABLE` supports one change per statement, so that each statement is a single atomic catalog operation:

```sql
ALTER TABLE <table> ADD COLUMN <column> <type>
ALTER TABLE <table> DROP COLUMN <column>
ALTER TABLE <table> ALTER COLUMN <column> SET DATA TYPE <type>
ALTER TABLE <table> ADD [IF NOT EXISTS] PROJECTION <name> AS ( <select> )
ALTER TABLE <table> DROP PROJECTION [IF EXISTS] <name>
ALTER TABLE <table> SET PROPERTIES ( <property> = <value> [, ...] )
```

`ADD COLUMN` fails if the column already exists, and `ALTER COLUMN` fails if it does not, so a misspelled column name
is reported rather than quietly creating or replacing a column. Each statement is also checked against the rest of the
table definition, not only the part it changes: adding a column, changing a type, or setting a property is rejected if
the resulting table would be invalid, such as a segment granularity coarser than a projection the table declares.

`DROP COLUMN` removes the column's declaration, not its data. A table's SQL schema is the declared columns followed by
any other columns present in its segments, so a dropped column that still has data remains queryable: it loses its
declared type and position and appears after the declared columns, like any column the catalog does not know about.
Whether it can still be ingested then follows the usual rule: rejected if the table is sealed, accepted as an
undeclared column otherwise. To remove a column from query results without rewriting data, use the catalog's
`hiddenColumns` property instead, which currently has no SQL spelling and is set through the REST API.

#### Projections

A table may declare [projections](../../querying/projections.md), which are pre-aggregated views stored inside each
segment. A projection is written as a `SELECT` over the table's own columns, with no `FROM` clause:

```sql
CREATE TABLE "druid"."visits" (
  __time TIMESTAMP,
  user_id VARCHAR,
  user_agent VARCHAR,
  pages_visited BIGINT,
  PROJECTION daily_by_agent AS (
    SELECT TIME_FLOOR(__time, 'P1D'), user_agent, SUM(pages_visited) AS total_pages
    WHERE user_agent IS NOT NULL
    GROUP BY 1, 2
  )
)
PARTITIONED BY DAY
```

The body is planned exactly as the equivalent query would be, so a projection matches the queries it was written to
serve. Every aggregate needs an alias, which becomes the name of the stored column. Time granularity is expressed
with `TIME_FLOOR`, as it would be in a query.

Because the body is planned like a query, it is planned under the statement's own query context, including any `SET`
clauses. Only context parameters that affect planning can change the stored definition; parameters that only affect
query execution have no effect, since the body is planned and stored rather than run. The context itself is not part
of the definition: what the catalog stores is the projection the body planned to, so nothing from the statement's
context is carried over to queries that later use it. Note also that a projection is matched to a query by its shape,
so a definition planned under a context that changes that shape only matches queries run under the same context.

A projection body accepts a select list, an optional `WHERE` and an optional `GROUP BY`. It cannot use `ORDER BY`,
`LIMIT` or `HAVING`: a projection's ordering follows its grouping columns and is not something you choose. It also
cannot use joins, subqueries, or expressions computed over aggregates, such as `SUM(x) / COUNT(x)`: store the two
aggregates as separate columns and divide at query time. A projection can store the same
[aggregation functions that are supported for rollup at ingestion time](../../multi-stage-query/concepts.md#rollup).

Projections may also be added to and removed from an existing table:

```sql
ALTER TABLE "druid"."visits" ADD [IF NOT EXISTS] PROJECTION by_agent AS (
  SELECT user_agent, SUM(pages_visited) AS total_pages GROUP BY user_agent
)
ALTER TABLE "druid"."visits" DROP PROJECTION [IF EXISTS] by_agent
```

Both take effect for subsequent ingestion. Segments already built keep whatever projections they were built with, so
dropping a projection does not rewrite data.

#### The base table

The reserved projection name `__base` describes the table's own physical layout rather than an additional
pre-aggregation. Defining it makes the table a 'clustered' table: rows of segments are stored grouped by the clustering
columns.

Its body lists the columns in the order segments store them, so it must name every declared column, in declared
order. An item written as `<expr> AS <name>` makes that column computed at ingest time, from the columns it reads:

```sql
CREATE TABLE "druid"."events" (
  tenant VARCHAR,
  bucket BIGINT,
  __time TIMESTAMP,
  user_id BIGINT,
  payload TYPE('COMPLEX<json>'),
  PROJECTION __base AS (
    SELECT tenant, ABS(user_id) % 128 AS bucket, __time, user_id, payload
    CLUSTERED BY tenant, bucket
  )
)
PARTITIONED BY DAY
SEALED
```

The clustering columns must be the leading columns of the table, because the declared order is the physical order.
`SEALED` is optional: a column the query produces but the table does not declare is stored after the declared
layout, in the order it arrives. Declare `SEALED` to reject such columns instead.

Computed columns, like `bucket` in the example above, are computed based on inputs provided by the `INSERT` or
`REPLACE`. In terms of the example, the `INSERT` or `REPLACE` command should provide `user_id`, not `bucket`.

Unlike an aggregate projection, a `__base` body cannot filter or group: the base table stores every ingested row.
It is the only projection that chooses a clustering.

`ALTER TABLE ... ADD PROJECTION __base AS ( ... )` gives an existing table a layout, and
`ALTER TABLE ... DROP PROJECTION __base` removes it. Both affect future segments only.

Every other name beginning with `__` remains reserved.

#### Setting table properties

`SET PROPERTIES` merges the given [table properties](#table-properties) into the table. A value of `NULL` removes a
property. Values must be literals:

```sql
ALTER TABLE "druid"."visits" SET PROPERTIES (targetSegmentRows = 3000000, sealed = TRUE)
ALTER TABLE "druid"."visits" SET PROPERTIES (sealed = NULL)
```

Both statements require `READ` and `WRITE` permission on the datasource, the same permissions the REST API checks. Table
names may be unqualified or qualified with the `druid` schema; other schemas are rejected. Names are case-sensitive.

There is no `DROP TABLE`. Deleting a table's catalog entry without deleting its data would be a surprising meaning
for the statement, so removing a specification is left to the [delete API](#delete-a-table) until the semantics are
settled.

### API Objects

#### TableSpec

A tableSpec defines a table

| Property     | Type                            | Description                                                               | Required | Default |
|--------------|---------------------------------|---------------------------------------------------------------------------|----------|---------|
| `type`       | String                          | the type of table. The only value supported at this time is `datasource`  | yes      | null    |
| `properties` | Map&lt;String, Object>             | the table's defined properties. see [table properties](#table-properties) | no       | null    |
| `columns`    | List&lt;[ColumnSpec](#columnspec)> | the table's defined columns                                               | no       | null    |

#### Table Properties

| PropertyKeyName      | PropertyValueType | Description                                                                                                                                                                                                                                                                                                                                              | Required | Default |
|----------------------|-------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|---------|
| `segmentGranularity` | String            | determines how time-based partitioning is done. See [Partitioning by time](../../multi-stage-query/concepts.md#partitioning-by-time). Can specify any of the values as permitted for [PARTITIONED BY](../../multi-stage-query/reference.md#partitioned-by). This property value may be overridden at query time, by specifying the PARTITIONED BY clause. | no       | null    |
| `sealed`             | boolean           | require all columns in the table schema to be fully declared before data is ingested. Setting this to true will cause failure when DML queries attempt to add undefined columns to the table.                                                                                                                                                            | no       | false   |

#### ColumnSpec

| Property     | Type                | Description                                                                                                            | Required | Default |
|--------------|---------------------|------------------------------------------------------------------------------------------------------------------------|----------|---------|
| `name`       | String              | The name of the column                                                                                                 | yes      | null    |
| `dataType`   | String              | The type of the column. Can be any column data type that is available to Druid. Depends on what extensions are loaded. | no       | null    |
| `properties` | Map&lt;String, Object\> | the column's defined properties. Non properties defined at this time.                                                  | no       | null    |

### APIs

#### Create or update a table

Update or create a new table containing the given table specification.

##### URL

`POST` `/druid/coordinator/v1/catalog/schemas/{schema}/tables/{name}`

##### Request body

The request object for this request is a [TableSpec](#tablespec)

##### Query parameters

The endpoint supports a set of optional query parameters to enforce optimistic locking, and to specify that a request
is meant to update a table rather than create a new one. In the default case, with no query parameters set, this request
will return an error if a table of the same name already exists in the schema specified.

| Parameter     | Type    | Description                                                                                                                   |
|---------------|---------|-------------------------------------------------------------------------------------------------------------------------------|
| `version`     | Long    | the expected version of an existing table. The version must match. If not (or if the table does not exist), returns an error. |
| `overwrite`   | boolean | if true, then overwrites any existing table. Otherwise, the operation fails if the table already exists.                      |
| `ifNotExists` | boolean | if true, then leaves an existing table unchanged and reports a version of 0. Otherwise, the operation fails if the table already exists. |

##### Responses

<Tabs>

<TabItem value="1" label="200 SUCCESS">

*Successfully submitted table spec. Returns an object that includes the version of the table created or updated:*

```json
{
    "version": 12345687
}
```

</TabItem>
<TabItem value="2" label="400 BAD REQUEST">

*Error thrown due to bad request. Returns a JSON object detailing the error with the following format:*

```json
{
    "error": "A well-defined error code.",
    "errorMessage": "A message with additional details about the error."
}
```
</TabItem>
<TabItem value="3" label="500 INTERNAL SERVER ERROR">


*Error thrown due to unexpected conditions. Returns a JSON object detailing the error with the following format:*

```json
{
    "error": "A well-defined error code.",
    "errorMessage": "A message with additional details about the error."
}
```

</TabItem>
</Tabs>

##### Sample request

The following example shows how to create a sealed table with several defined columns, and a defined segment granularity of `"P1D"`

```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/coordinator/v1/catalog/schemas/druid/tables/test_table" \
-X 'POST' \
--header 'Content-Type: application/json' \
--data '{
  "type": "datasource",
  "columns": [
    {
      "name": "__time",
      "dataType": "long"
    },
    {
      "name": "double_col",
      "dataType": "double"
    },
    {
      "name": "float_col",
      "dataType": "float"
    },
    {
      "name": "long_col",
      "dataType": "long"
    },
    {
      "name": "string_col",
      "dataType": "string"
    }
  ],
  "properties": {
    "segmentGranularity": "P1D",
    "sealed": true
  }
}'
```

##### Sample response

```json
{
  "version": 1730965026295
}
```

#### Retrieve a table

Retrieve a table

##### URL

`GET` `/druid/coordinator/v1/catalog/schemas/{schema}/tables/{name}`

##### Responses

<Tabs>

<TabItem value="1" label="200 SUCCESS">

*Successfully retrieved corresponding table's [TableSpec](#tablespec)*

</TabItem>
<TabItem value="2" label="400 BAD REQUEST">

*Error thrown due to bad request. Returns a JSON object detailing the error with the following format:*

```json
{
    "error": "A well-defined error code.",
    "errorMessage": "A message with additional details about the error."
}
```
</TabItem>
<TabItem value="3" label="500 INTERNAL SERVER ERROR">

*Error thrown due to unexpected conditions. Returns a JSON object detailing the error with the following format:*

```json
{
    "error": "A well-defined error code.",
    "errorMessage": "A message with additional details about the error."
}
```

</TabItem>
</Tabs>

##### Sample request

The following example shows how to retrieve a table named `test_table` in schema `druid`:

```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/coordinator/v1/catalog/schemas/druid/tables/test_table"
```

##### Sample response

<details>
  <summary>View the response</summary>

```json
{
  "id": {
    "schema": "druid",
    "name": "test_table"
  },
  "creationTime": 1730965026295,
  "updateTime": 1730965026295,
  "state": "ACTIVE",
  "spec": {
    "type": "datasource",
    "properties": {
      "segmentGranularity": "P1D",
      "sealed": true
    },
    "columns": [
      {
        "name": "__time",
        "dataType": "long"
      },
      {
        "name": "double_col",
        "dataType": "double"
      },
      {
        "name": "float_col",
        "dataType": "float"
      },
      {
        "name": "long_col",
        "dataType": "long"
      },
      {
        "name": "string_col",
        "dataType": "string"
      }
    ]
  }
}
```
</details>

#### Delete a table

Delete a table

##### URL

`DELETE` `/druid/coordinator/v1/catalog/schemas/{schema}/tables/{name}`

##### Responses

<Tabs>

<TabItem value="1" label="200 SUCCESS">

*No response body*

</TabItem>
<TabItem value="2" label="400 BAD REQUEST">

*Error thrown due to bad request. Returns a JSON object detailing the error with the following format:*

```json
{
    "error": "A well-defined error code.",
    "errorMessage": "A message with additional details about the error."
}
```
</TabItem>
<TabItem value="3" label="500 INTERNAL SERVER ERROR">

*Error thrown due to unexpected conditions. Returns a JSON object detailing the error with the following format:*

```json
{
    "error": "A well-defined error code.",
    "errorMessage": "A message with additional details about the error."
}
```

</TabItem>
</Tabs>

##### Sample request

The following example shows how to delete the a table named `test_table` in schema `druid`

```shell
curl -X 'DELETE' "http://ROUTER_IP:ROUTER_PORT/druid/coordinator/v1/catalog/schemas/druid/tables/test_table"
```

##### Sample response

No response body

#### Retrieve list of schema names

retrieve list of schema names

##### URL

`GET` `/druid/coordinator/v1/catalog/schemas`

##### Responses

<Tabs>

<TabItem value="1" label="200 SUCCESS">

*Successfully retrieved list of schema names*

</TabItem>
<TabItem value="2" label="400 BAD REQUEST">

*Error thrown due to bad request. Returns a JSON object detailing the error with the following format:*

```json
{
    "error": "A well-defined error code.",
    "errorMessage": "A message with additional details about the error."
}
```
</TabItem>
<TabItem value="3" label="500 INTERNAL SERVER ERROR">

*Error thrown due to unexpected conditions. Returns a JSON object detailing the error with the following format:*

```json
{
    "error": "A well-defined error code.",
    "errorMessage": "A message with additional details about the error."
}
```

</TabItem>
</Tabs>

##### Sample request

The following example shows how to retrieve the list of schema names.

```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/coordinator/v1/catalog/schemas"
```

##### Sample response

```json
[
  "INFORMATION_SCHEMA",
  "druid",
  "ext",
  "lookups",
  "sys",
  "view"
]
```

#### Retrieve list of table names in schema

Retrieve a list of table names in the schema.

##### URL

`GET` `/druid/coordinator/v1/catalog/schemas/{schema}/tables`

##### Responses

<Tabs>

<TabItem value="1" label="200 SUCCESS">

*Successfully retrieved list of table names belonging to schema*

</TabItem>
<TabItem value="2" label="400 BAD REQUEST">

*Error thrown due to bad request. Returns a JSON object detailing the error with the following format:*

```json
{
    "error": "A well-defined error code.",
    "errorMessage": "A message with additional details about the error."
}
```
</TabItem>
<TabItem value="3" label="500 INTERNAL SERVER ERROR">

*Error thrown due to unexpected conditions. Returns a JSON object detailing the error with the following format:*

```json
{
    "error": "A well-defined error code.",
    "errorMessage": "A message with additional details about the error."
}
```

</TabItem>
</Tabs>

##### Sample request

The following example shows how to retrieve all of the table names of tables belonging to the `druid` schema.

```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/coordinator/v1/catalog/schemas/druid/tables"
```

##### Sample response

```json
[
  "test_table"
]
```
