
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

### API Objects

#### TableSpec

A tableSpec defines a table

| Property     | Type                            | Description                                                               | Required | Default |
|--------------|---------------------------------|---------------------------------------------------------------------------|----------|---------|
| `type`       | String                          | the type of table. The only value supported at this time is `datasource`  | yes      | null    |
| `properties` | Map<String, Object>             | the table's defined properties. see [table properties](#table-properties) | no       | null    |
| `columns`    | List<[ColumnSpec](#columnspec)> | the table's defined columns                                               | no       | null    |

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
| `properties` | Map<String, Object> | the column's defined properties. Non properties defined at this time.                                                  | no       | null    |

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

| Parameter   | Type    | Description                                                                                                                   |
|-------------|---------|-------------------------------------------------------------------------------------------------------------------------------|
| `version`   | Long    | the expected version of an existing table. The version must match. If not (or if the table does not exist), returns an error. |
| `overwrite` | boolean | if true, then overwrites any existing table. Otherwise, the operation fails if the table already exists.                      |

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

`GET` `/druid/coordinator/v1/catalog/schemas/{schema}/tables/{name}

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

The following example shows how to retrieve a table named 'test_table' in schema 'druid'

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

`DELETE` `/druid/coordinator/v1/catalog/schemas/{schema}/tables/{name}

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

`GET` `/druid/coordinator/v1/catalog/schemas

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

`GET` `/druid/coordinator/v1/catalog/schemas/{schema}/table

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