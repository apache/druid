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

In Apache Druid 28.0.0, the default [null handling](../querying/sql-data-types.md#null-values) mode changed to be compliant with the SQL standard.
This guide provides strategies for Druid operators who rely on the legacy Druid null handling behavior in their applications.
It provides strategies to emulate legacy null handling mode while operating Druid in SQL compliant null handling mode.

## SQL compliant null handling in Druid

Now, Druid writes segments in a SQL compatible null handling mode by default.
This means that Druid stores null values distinctly from empty strings for string dimensions and distinctly from 0 for numeric dimensions.

This can impact your application behavior because SQL the standard defines any comparison to null to be unknown.
According to this three-value logic, `x <> 'some value'` only returns non-null values.

The default Druid configurations for SQL compatible null handling mode is as follows:

* `druid.generic.useDefaultValueForNull=false`
* `druid.expressions.useStrictBooleans=true`
* `druid.generic.useThreeValueLogicForNativeFilters=true` 

Follow the [Null handling tutorial](../tutorials/tutorial-sql-null.md) to learn how the default null handling works in Druid.

## Legacy null handling and two-value logic

Prior to Druid 28.0.0, Druid defaulted to a legacy mode which stored default values instead of nulls.
In legacy mode, Druid segments created at ingestion time have the following characteristics:

- String columns can not distinguish an empty string, '', from null.
    Therefore, Druid treats them both as interchangeable values.
- Numeric columns can not represent null valued rows.
    Therefore Druid stores 0 instead of null. 

The Druid configurations for the deprecated legacy mode are as follows:

* `druid.generic.useDefaultValueForNull=true`
* `druid.expressions.useStrictBooleans=false`
* `druid.generic.useThreeValueLogicForNativeFilters=true`

Note that these configurations are deprecated and scheduled for removal.

## Migrate to SQL compliant mode

If your business logic relies on the behavior of legacy mode, you can emulate the null handling behavior while operating Druid in SQL compatible null handling mode.
You can:

* Modify your ingestion SQL and ingestion specs to handle nulls at ingestion time.
    This means that you are modifying incoming data.
    For example, replacing a null for a string column with an empty string or a 0 for a numeric column.
    However, it means that your existing queries should operate as if Druid were in legacy mode.
    If you do not care about preserving null values, this is a good option for you.
* Update your SQL queries to handle the new behavior at query time.
    This means you preserve the incoming data with nulls intact.
    However, you must rewrite any affected client-side queries.
    If you may want to convert to SQL-compliant behavior in the future, or if you have a requirement to preserve null values, choose this option.

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

<TabItem value="0" label="SQL-based batcn">

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

| `__time` | `string_examle` | `number_example`|
| -- | -- | -- |
| `2024-01-01T00:00:00.000Z`| `my_string`| 99 |
| `2024-01-02T00:00:00.000Z`| `empty`| 0 |
| `2024-01-03T00:00:00.000Z`| `empty`| 0 |

### Handle null values at query time

If you want to maintain null values in your data within Druid, you can emulate the legacy null handling mode mode as follows:

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
To achieve the same result, you could use IS DISTINCT FROM for null-safe comparison:

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

Druid returns the following because, according to the SQL standard, null + any value is null:

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
| null |

## Learn more

See the following topics for more information:
 - [Null handling tutorial](../tutorials/tutorial-sql-null.md) to learn how the default null handling works in Druid.
 - [Null values](../querying/sql-data-types.md#null-values) for a description of Druid's behavior with null values.
 - [Handling null values](../design/segments.md#handling-null-values) for details about how Druid stores null values.