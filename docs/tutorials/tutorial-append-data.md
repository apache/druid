---
id: tutorial-append-data
title: Append data
sidebar_label: Append data
description: Learn how to append data to a datasource without changing the existing data in Apache Druid.
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

This tutorial shows you how to use the Apache Druid SQL [INSERT](../multi-stage-query/reference.md#insert) function to append data to a [datasource](../design/storage.md) without changing the existing data.
The examples in the tutorial use the [multi-stage query (MSQ)](../multi-stage-query/index.md) task engine to executes SQL statements.

## Prerequisites

Before you follow the steps in this tutorial, download Druid as described in [Quickstart (local)](index.md) and have it running on your local machine. You don't need to load any data into the Druid cluster.

You should be familiar with data querying in Druid. If you haven't already, go through the [Query data](../tutorials/tutorial-query.md) tutorial first.

## Load sample data

Load a sample dataset using [INSERT](../multi-stage-query/reference.md#insert) and [EXTERN](../multi-stage-query/reference.md#extern-function) functions. The EXTERN function lets you read external data or write to an external location.

In the Druid [web console](../operations/web-console.md), go to the **Query** view and run the following query:

```sql
INSERT INTO "append_tutorial"
SELECT
  TIME_PARSE("timestamp") AS "__time",
  "animal",
  "number"
FROM TABLE(
  EXTERN(
    '{"type":"inline","data":"{\"timestamp\":\"2024-01-01T07:01:35Z\",\"animal\":\"octopus\", \"number\":115}\n{\"timestamp\":\"2024-01-01T05:01:35Z\",\"animal\":\"mongoose\", \"number\":737}\n{\"timestamp\":\"2024-01-01T06:01:35Z\",\"animal\":\"snake\", \"number\":1234}\n{\"timestamp\":\"2024-01-01T01:01:35Z\",\"animal\":\"lion\", \"number\":300}\n{\"timestamp\":\"2024-01-02T07:01:35Z\",\"animal\":\"seahorse\", \"number\":115}\n{\"timestamp\":\"2024-01-02T05:01:35Z\",\"animal\":\"skunk\", \"number\":737}\n{\"timestamp\":\"2024-01-02T06:01:35Z\",\"animal\":\"iguana\", \"number\":1234}\n{\"timestamp\":\"2024-01-02T01:01:35Z\",\"animal\":\"opossum\", \"number\":300}"}',
      '{"type":"json"}'
    )
  ) EXTEND ("timestamp" VARCHAR, "animal" VARCHAR, "number" BIGINT)
PARTITIONED BY DAY
```

The resulting `append_tutorial` datasource contains records for eight animals over two days.
To view the results, open a new tab and run the following query:

```sql
SELECT * FROM "append_tutorial"
```

<details>
<summary> View the results</summary>

| `__time` | `animal` | `number`|
| -- | -- | -- |
| `2024-01-01T01:01:35.000Z`| `lion`| 300 |
| `2024-01-01T05:01:35.000Z`| `mongoose`| 737 |
| `2024-01-01T06:01:35.000Z`| `snake`| 1234 |
| `2024-01-01T07:01:35.000Z`| `octopus`| 115 |
| `2024-01-02T01:01:35.000Z`| `opossum`| 300 |
| `2024-01-02T05:01:35.000Z`| `skunk`| 737 |
| `2024-01-02T06:01:35.000Z`| `iguana`| 1234 |
| `2024-01-02T07:01:35.000Z`| `seahorse`| 115 |

</details>

## Append data

You can use the INSERT function to append data to the datasource without changing the existing data.
In a new tab, run the following query to ingest and append data to the `append_tutorial` datasource:

```sql
INSERT INTO "append_tutorial"
SELECT
  TIME_PARSE("timestamp") AS "__time",
  "animal",
  "number"
FROM TABLE(
  EXTERN(
    '{"type":"inline","data":"{\"timestamp\":\"2024-01-03T01:09:35Z\",\"animal\":\"zebra\", \"number\":233}\n{\"timestamp\":\"2024-01-04T07:01:35Z\",\"animal\":\"bear\", \"number\":577}\n{\"timestamp\":\"2024-01-04T05:01:35Z\",\"animal\":\"falcon\", \"number\":848}\n{\"timestamp\":\"2024-01-04T06:01:35Z\",\"animal\":\"giraffe\", \"number\":113}\n{\"timestamp\":\"2024-01-04T01:01:35Z\",\"animal\":\"rhino\", \"number\":473}"}',
    '{"type":"json"}'
    )
  ) EXTEND ("timestamp" VARCHAR, "animal" VARCHAR, "number" BIGINT)
PARTITIONED BY DAY
```

Druid adds rows for the subsequent days after `seahorse`.
When the task completes, open a new tab and run the following query to view the results:

```sql
SELECT * FROM "append_tutorial"
```

<details>
<summary> View the results</summary>

| `__time` | `animal` | `number`|
| -- | -- | -- |
| `2024-01-01T01:01:35.000Z`| `lion`| 300 |
| `2024-01-01T05:01:35.000Z`| `mongoose`| 737 |
| `2024-01-01T06:01:35.000Z`| `snake`| 1234 |
| `2024-01-01T07:01:35.000Z`| `octopus`| 115 |
| `2024-01-02T01:01:35.000Z`| `opossum`| 300 |
| `2024-01-02T05:01:35.000Z`| `skunk`| 737 |
| `2024-01-02T06:01:35.000Z`| `iguana`| 1234 |
| `2024-01-02T07:01:35.000Z`| `seahorse`| 115 |
| `2024-01-03T01:09:35.000Z`| `zebra`| 233 |
| `2024-01-04T01:01:35.000Z`| `rhino`| 473 |
| `2024-01-04T05:01:35.000Z`| `falcon`| 848 |
| `2024-01-04T06:01:35.000Z`| `giraffe`| 113 |
| `2024-01-04T07:01:35.000Z`| `bear`| 577 |

</details>

## Learn more

See the following topics for more information:

* [SQL-based ingestion reference](../multi-stage-query/reference.md) for a reference on MSQ architecture.
* [SQL-based ingestion query examples](../multi-stage-query/examples.md) for example queries using the MSQ task engine.