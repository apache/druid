---
id: migr-ansi-sql-null
title: "Migration guide: SQL compliant mode"
sidebar_label: SQL compliant mode
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

In Apache Druid 32.0.0, legacy configurations which were incompatible with the ANSI SQL standard were removed. 

These configurations were:
* `druid.generic.useDefaultValueForNull`
* `druid.expressions.useStrictBooleans`
* `druid.generic.useThreeValueLogicForNativeFilters` 

This guide provides strategies for Druid operators who rely on legacy Druid null handling behavior in their applications to transition to Druid 32.0.0 or later.

## SQL compliant null handling

As of Druid 28.0.0, Druid writes segments in an ANSI SQL compatible null handling mode by default, and in Druid 32.0.0 this is no longer configurable.
This is a change of legacy behavior and means that Druid stores null values distinctly from empty strings for string dimensions and distinctly from 0 for numeric dimensions.

This can impact your application behavior because the ANSI SQL standard defines any comparison to null to be unknown.
According to this three-valued logic, `x <> 'some value'` only returns non-null values.

Follow the [Null handling tutorial](../tutorials/tutorial-sql-null.md) to learn how null handling works in Druid.

## Legacy null handling and two-valued filter logic

Prior to Druid 28.0.0, Druid defaulted to a legacy mode which stored default values instead of nulls.
In this mode, Druid created segments with the following characteristics at ingestion time:

- String columns couldn't distinguish an empty string, `''`, from null.
    Therefore, Druid treated them both as interchangeable values.
- Numeric columns couldn't represent null valued rows.
    Therefore, Druid stored `0` instead of `null`.

## Migrate to SQL compliant mode

If your business logic relies on the behavior of legacy mode, you have the following options to operate Druid in an ANSI SQL compatible null handling mode:

- Modify incoming data to either [avoid nulls](#replace-null-values-at-ingestion-time) or [avoid empty strings](#coerce-empty-strings-to-null-at-ingestion-time) to achieve the same query behavior as legacy mode. This means modifying your ingestion SQL queries and ingestion specs to handle nulls or empty strings.
    For example, replacing a null for a string column with an empty string or a 0 for a numeric column.
    However, it means that your existing queries should operate as if Druid were in legacy mode.
    If you do not care about preserving null values, this is a good option for you.

- Preserve null values and [update all of your SQL queries to be ANSI SQL compliant](#rewrite-your-queries-to-be-sql-compliant).
    This means you can preserve the incoming data with nulls intact.
    However, you must rewrite any affected client-side queries to be ANSI SQL compliant.
    If you have a requirement to preserve null values, choose this option.

### Replace null values at ingestion time

If you don't need to preserve null values within Druid, you can use a transform at ingestion time to replace nulls with other values.

Consider the following input data:

```json
{"time":"2024-01-01T00:00:00.000Z","string_example":"my_string","number_example":99}
{"time":"2024-01-02T00:00:00.000Z","string_example":"","number_example":0}
{"time":"2024-01-03T00:00:00.000Z","string_example":null,"number_example":null}
```
 
The following example illustrates how to use COALESCE and NVL at ingestion time to avoid null values in Druid:

<Tabs>

<TabItem value="0" label="SQL-based batch">

```sql
REPLACE INTO "no_nulls_example" OVERWRITE ALL
WITH "ext" AS (
  SELECT *
  FROM TABLE(
    EXTERN(
      '{"type":"inline","data":"{\"time\":\"2024-01-01T00:00:00.000Z\",\"string_example\":\"my_string\",\"number_example\":99}\n{\"time\":\"2024-01-02T00:00:00.000Z\",\"string_example\":\"\",\"number_example\":0}\n{\"time\":\"2024-01-03T00:00:00.000Z\",\"string_example\":null,\"number_example\":null}"}',
      '{"type":"json"}'
    )
  ) EXTEND ("time" VARCHAR, "string_example" VARCHAR, "number_example" BIGINT)
)
SELECT
  TIME_PARSE("time") AS "__time",
  -- Replace any null string values with an empty string
  COALESCE("string_example",'') AS string_example,
  -- Replace any null numeric values with 0
  NVL("number_example",0) AS number_example
FROM "ext"
PARTITIONED BY MONTH
```
</TabItem>

<TabItem value="1" label="JSON-based batch">

```json
{
  "type": "index_parallel",
  "spec": {
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "inline",
        "data": "{\"time\":\"2024-01-01T00:00:00.000Z\",\"string_example\":\"my_string\",\"number_example\":99}\n{\"time\":\"2024-01-02T00:00:00.000Z\",\"string_example\":\"\",\"number_example\":0}\n{\"time\":\"2024-01-03T00:00:00.000Z\",\"string_example\":null,\"number_example\":null}"
      },
      "inputFormat": {
        "type": "json"
      }
    },
    "tuningConfig": {
      "type": "index_parallel",
      "partitionsSpec": {
        "type": "dynamic"
      }
    },
    "dataSchema": {
      "dataSource": "inline_data_native",
      "timestampSpec": {
        "column": "time",
        "format": "iso"
      },
      "dimensionsSpec": {
        "dimensions": [
          "string_example",
          {
            "type": "long",
            "name": "number_example"
          }
        ]
      },
      "granularitySpec": {
        "queryGranularity": "none",
        "rollup": false,
        "segmentGranularity": "MONTH"
      },
      "transformSpec": {
        "transforms": [
          {
            "type": "expression",
            "name": "string_example",
            "expression": "COALESCE(\"string_example\",'')"
          },
          {
            "type": "expression",
            "name": "number_example",
            "expression": "NVL(\"number_example\",0)"
          }
        ]
      }
    }
  }
}
```

</TabItem>
</Tabs>

Druid ingests the data with no null values as follows:

| `__time` | `string_example` | `number_example`|
| -- | -- | -- |
| `2024-01-01T00:00:00.000Z`| `my_string`| 99 |
| `2024-01-02T00:00:00.000Z`| `empty`| 0 |
| `2024-01-03T00:00:00.000Z`| `empty`| 0 |

### Coerce empty strings to null at ingestion time

In legacy mode, Druid recognized empty strings as nulls for equality comparison.
If your queries rely on empty strings to represent nulls, you can coerce empty strings to null at ingestion time using NULLIF.

For example, consider the following sample input data:

```json
{"time":"2024-01-01T00:00:00.000Z","string_example":"my_string"}
{"time":"2024-01-02T00:00:00.000Z","string_example":""}
{"time":"2024-01-03T00:00:00.000Z","string_example":null}
```

In legacy mode, Druid wrote an empty string for the third record.
Therefore the following query returned 2:

```sql
SELECT count(*)
FROM "null_string"
WHERE "string_example" IS NULL
```

In SQL compliant mode, Druid differentiates between empty strings and nulls, so the same query would return 1.
The following example shows how to coerce empty strings into null to accommodate IS NULL comparisons:

<Tabs>

<TabItem value="0" label="SQL-based batcn">

```sql
REPLACE INTO "null_string" OVERWRITE ALL
WITH "ext" AS (
  SELECT *
  FROM TABLE(
    EXTERN(
      '{"type":"inline","data":"{\"time\":\"2024-01-01T00:00:00.000Z\",\"string_example\":\"my_string\"}\n{\"time\":\"2024-01-02T00:00:00.000Z\",\"string_example\":\"\"}\n{\"time\":\"2024-01-03T00:00:00.000Z\",\"string_example\":null}"}',
      '{"type":"json"}'
    )
  ) EXTEND ("time" VARCHAR, "string_example" VARCHAR)
)
SELECT
  TIME_PARSE("time") AS "__time",
  NULLIF("string_example",'') AS "string_example"
FROM "ext"
PARTITIONED BY MONTH
```

</TabItem>

<TabItem value="1" label="JSON-based batch">

```json
{
  "type": "index_parallel",
  "spec": {
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "inline",
        "data": "{\"time\":\"2024-01-01T00:00:00.000Z\",\"string_example\":\"my_string\"}\n{\"time\":\"2024-01-02T00:00:00.000Z\",\"string_example\":\"\"}\n{\"time\":\"2024-01-03T00:00:00.000Z\",\"string_example\":null}"
      },
      "inputFormat": {
        "type": "json"
      }
    },
    "tuningConfig": {
      "type": "index_parallel",
      "partitionsSpec": {
        "type": "dynamic"
      }
    },
    "dataSchema": {
      "dataSource": "null_string",
      "timestampSpec": {
        "column": "time",
        "format": "iso"
      },
      "transformSpec": {
        "transforms": [
          {
            "type": "expression",
            "expression": "case_searched((\"string_example\" == ''),null,\"string_example\")",
            "name": "string_example"
          }
        ]
      },
      "dimensionsSpec": {
        "dimensions": [
          "string_example"
        ]
      },
      "granularitySpec": {
        "queryGranularity": "none",
        "rollup": false,
        "segmentGranularity": "month"
      }
    }
  }
}
```

</TabItem>
</Tabs>

Druid ingests the data with no empty strings as follows:

| `__time` | `string_examle` |
| -- | -- | -- |
| `2024-01-01T00:00:00.000Z`| `my_string`|
| `2024-01-02T00:00:00.000Z`| `null`|
| `2024-01-03T00:00:00.000Z`| `null`|

Therefore `SELECT count(*) FROM "null_string" WHERE "string_example" IS NULL` returns 2.

### Rewrite your queries to be SQL compliant

If you want to maintain null values in your data within Druid, you can use the following ANSI SQL compliant querying strategies to achieve the same results as legacy null handling:

- Modify inequality queries to include null values.
  For example, `x <> 'some value'` becomes `(x <> 'some value' OR x IS NULL)`.
- Use COALESCE or NVL to replace nulls with a value.
  For example, `x + 1` becomes `NVL(numeric_value, 0)+1`

Consider the following Druid datasource `null_example`:

| `__time` | `string_examle` | `number_example`|
| -- | -- | -- |
| `2024-01-01T00:00:00.000Z`| `my_string`| 99 |
| `2024-01-02T00:00:00.000Z`| `empty`| 0 |
| `2024-01-03T00:00:00.000Z`| `null`| null |

Druid excludes null strings from equality comparisons. For example:

```sql
SELECT COUNT(*) AS count_example
FROM "null_example"
WHERE "string_example"<> 'my_string'
```

Druid returns 1 because null is considered unknown: neither equal nor unequal to the value.

To count null values in the result, you can use an OR operator:

```sql
SELECT COUNT(*) AS count_example
FROM "null_example"
WHERE ("string_example"<> 'my_string') OR "string_example" IS NULL
```

Druid returns 2.
To achieve the same result, you can use IS DISTINCT FROM for null-safe comparison:

```sql
SELECT COUNT(*) as count_example
FROM "null_example"
WHERE "string_example" IS DISTINCT FROM 'my_string'
```

Similarly, arithmetic operators on null return null. For example:

```sql
SELECT "number_example" + 1 AS additon_example
FROM "null_example"
```

Druid returns the following because null + any value is null for the ANSI SQL standard:

| `addition_example`|
| -- |
| 100 |
| 1 |
| null |

Use NVL to avoid nulls with arithmetic. For example:

```sql
SELECT NVL("number_example",0) + 1 AS additon_example
FROM "null_example"
```

Druid returns the following:

| `addition_example` |
| -- |
| 100 |
| 1 |
| 1 |

## Learn more

See the following topics for more information:
 - [Null handling tutorial](../tutorials/tutorial-sql-null.md) to learn how the default null handling works in Druid.
 - [Null values](../querying/sql-data-types.md#null-values) for a description of Druid's null values.
 - [Handling null values](../design/segments.md#handling-null-values) for details about how Druid stores null values.