---
id: cascading-reindexing
title: "Cascading reindexing"
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

:::info
Cascading reindexing is an experimental feature introduced in Druid 37. Its API may change in future releases. This feature is only for automatic compaction using [compaction supervisors](automatic-compaction.md#auto-compaction-using-compaction-supervisors) with the [MSQ task engine](automatic-compaction.md#use-msq-for-auto-compaction).
:::

Cascading reindexing is a compaction supervisor template that lets you define age-based rules to automatically apply different compaction configurations as data ages. Instead of a single flat compaction configuration for an entire datasource, you define rules that say "for data older than X, apply configuration Y." Reindexing is a more general term than compaction. Reindexing not only can merge segments with the same schema and partitioning, but also can change the segment schema, partitioning, and encoding. Cascading reindexing gives you fine-grained control over how your data evolves over time.

For example, you might want to:
- Keep recent data in hourly segments, but coarsen to daily segments after 90 days to help reduce segment count and storage footprint.
- Delete some unwanted rows from data older than 30 days.
- Change compression settings for older data.
- Roll up older data to a coarser query granularity

Cascading reindexing handles all of this automatically by generating a timeline of compaction intervals and applying the appropriate rules to each interval.

## Prerequisites

Before using cascading reindexing, ensure your cluster meets the following requirements:

- **MSQ compaction engine**: Set `engine` to `msq` in the compaction dynamic config or in the supervisor spec.
- **At least two compaction task slots**: The MSQ task engine requires at least two tasks (one controller, one worker).

## How cascading reindexing works

### Rule-based configuration

Cascading reindexing uses a rule-based system where each rule controls a specific aspect of compaction and specifies an age threshold for when it applies. There are four rule types, each controlling an orthogonal aspect of the compaction config output for an interval:

| Rule type | What it controls | Additive? |
|---|---|---|
| [Partitioning](#partitioning-rules) | Segment granularity, partitions spec, optional virtual columns for range partitioning | No |
| [Deletion](#deletion-rules) | Rows to remove from segments | Yes |
| [Index spec](#index-spec-rules) | Compression and encoding settings | No |
| [Data schema](#data-schema-rules) | Dimensions, metrics, query granularity, rollup, projections | No |

Every rule has an `olderThan` field — an [ISO 8601 period](https://en.wikipedia.org/wiki/ISO_8601#Durations) that defines the age threshold. A rule with `"olderThan": "P30D"` applies to data whose interval ends before the current time minus 30 days.

### Additive vs non-additive rules

**Non-additive rules** (partitioning, index spec, data schema): Only one rule of each type can apply per interval. When multiple rules of the same type match, the rule with the oldest threshold (largest period) takes precedence.

**Additive rules** (deletion): Multiple deletion rules can apply to the same interval. When they do, they combine as `NOT(A OR B OR C)`, where A, B, and C are the `deleteWhere` filters from each rule. In other words, the compacted data retains only the rows that don't match any of the deletion filters.

### Timeline generation

The cascading reindexing template generates a timeline of non-overlapping search intervals, each with its own set of applicable rules. Here is how the timeline is constructed:

1. **Build a base timeline from partitioning rules.** Each partitioning rule defines a segment granularity and an age threshold. The template sorts rules by threshold (oldest first) and creates intervals with boundaries aligned to each rule's segment granularity.

2. **Split at non-partitioning rule thresholds.** If deletion, index spec, or data schema rules have thresholds that fall inside a base interval, the template splits that interval at the threshold (aligned to the interval's segment granularity). This ensures rules are applied as precisely as possible.

3. **Validate granularity ordering.** The template validates that segment granularity stays the same or becomes finer as you move from past to present. For example, DAY for old data and HOUR for recent data is valid, but HOUR for old data and DAY for recent data is not.

#### Timeline generation example

Suppose the current time is `2026-03-26T00:00:00Z` and you configure the following rules:

- **Partitioning rule A**: `olderThan: P7D`, `segmentGranularity: HOUR`
- **Partitioning rule B**: `olderThan: P90D`, `segmentGranularity: DAY`
- **Deletion rule C**: `olderThan: P30D`, `deleteWhere: isRobot = true`

The template generates these search intervals:

| Search interval | Segment granularity | Source | Active rules |
|---|---|---|---|
| `[-inf, 2025-12-26)` | DAY | Partitioning rule B | B, C |
| `[2025-12-26, 2026-02-24)` | DAY | Default (from template) | C |
| `[2026-02-24, 2026-03-19)` | HOUR | Partitioning rule A | A |

How this works step by step:

1. Partitioning rule B (`olderThan: P90D`) creates the interval `[-inf, 2025-12-26)` with DAY granularity. Partitioning rule A (`olderThan: P7D`) creates `[2025-12-26, 2026-03-19)` with HOUR granularity.
2. Deletion rule C (`olderThan: P30D`) has a threshold of `2026-02-24`. This falls inside rule A's interval, so that interval is split at `2026-02-24` (which is already DAY-aligned). The older sub-interval `[2025-12-26, 2026-02-24)` picks up deletion rule C; the newer sub-interval `[2026-02-24, 2026-03-19)` does not.
3. Granularity validation passes because DAY (older) to HOUR (newer) is valid — granularity becomes finer toward the present.

### How defaults work

The template requires `defaultSegmentGranularity` and `defaultPartitionsSpec`. These are used for any interval where no partitioning rule matches. This happens in two scenarios:

1. **No partitioning rules defined at all.** If you only define deletion, index spec, or data schema rules, all intervals use the default granularity and partitions spec.
2. **Non-partitioning rules have a more recent threshold than the newest partitioning rule.** For example, if your only partitioning rule is `olderThan: P90D` but you have a deletion rule with `olderThan: P30D`, intervals between 30 and 90 days old will use the defaults.

## Supervisor spec reference

To submit a cascading reindexing supervisor, wrap the template spec inside a compaction supervisor spec:

```json
{
  "type": "autocompact",
  "spec": {
    "type": "reindexCascade",
    "dataSource": "wikipedia",
    "ruleProvider": { ... },
    "defaultSegmentGranularity": "DAY",
    "defaultPartitionsSpec": {
      "type": "dynamic",
      "maxRowsPerSegment": 5000000
    }
  }
}
```

### Template properties

The following table describes the properties of the `reindexCascade` template:

| Field | Description | Required | Default |
|---|---|---|---|
| `type` | Must be `reindexCascade`. | Yes | |
| `dataSource` | The datasource to compact. | Yes | |
| `ruleProvider` | [Rule provider](#rule-provider-types) configuration that supplies reindexing rules. | Yes | |
| `defaultSegmentGranularity` | Segment granularity used for intervals where no partitioning rule matches. Supported values: `MINUTE`, `FIFTEEN_MINUTE`, `HOUR`, `DAY`, `MONTH`, `QUARTER`, `YEAR`. | Yes | |
| `defaultPartitionsSpec` | Partitions spec used for intervals where no partitioning rule matches. See [MSQ task engine limitations](automatic-compaction.md#msq-task-engine-limitations) for supported partitioning types. | Yes | |
| `defaultPartitioningVirtualColumns` | Optional virtual columns used if your `defaultPartitionsSpec` range partitioning definition references virtual columns | No | |
| `taskPriority` | Priority of compaction tasks. | No | 25 |
| `inputSegmentSizeBytes` | Maximum total input segment size in bytes per compaction task. | No | 100000000000000 |
| `taskContext` | Context map passed to compaction tasks. Use this to set [MSQ context parameters](../multi-stage-query/reference.md#context-parameters) such as `maxNumTasks`. | No | |
| `skipOffsetFromLatest` | ISO 8601 period. Skips data newer than this offset from the end of the latest segment. Mutually exclusive with `skipOffsetFromNow`. | No | |
| `skipOffsetFromNow` | ISO 8601 period. Skips data newer than this offset from the current time. Mutually exclusive with `skipOffsetFromLatest`. | No | |
| `tuningConfig` | [Tuning config](../ingestion/native-batch.md#tuningconfig) for compaction tasks. You cannot set `partitionsSpec` inside `tuningConfig` — partitioning is controlled by rules and supervisor default. | No | |

### Rule provider types

A rule provider supplies the reindexing rules to the template. Druid supports two provider types.

#### Inline provider

The inline provider (`type: inline`) defines rules directly in the supervisor spec. This is currently the only concrete implementation.

```json
{
  "type": "inline",
  "partitioningRules": [ ... ],
  "deletionRules": [ ... ],
  "indexSpecRules": [ ... ],
  "dataSchemaRules": [ ... ]
}
```

| Field | Description | Required | Default |
|---|---|---|---|
| `type` | Must be `inline`. | Yes | |
| `partitioningRules` | List of [partitioning rules](#partitioning-rules). | No | `[]` |
| `deletionRules` | List of [deletion rules](#deletion-rules). | No | `[]` |
| `indexSpecRules` | List of [index spec rules](#index-spec-rules). | No | `[]` |
| `dataSchemaRules` | List of [data schema rules](#data-schema-rules). | No | `[]` |

At least one rule must be defined across all rule lists.

#### Composing provider

The composing provider (`type: composing`) chains multiple rule providers together with first-wins semantics. For each rule type, Druid uses the rules from the first provider that has non-empty rules of that type.

This rule provider exists in anticipation of future community contributed providers, such as a provider that sources rules from the Druid Catalog.

```json
{
  "type": "composing",
  "providers": [
    { "type": "inline", "partitioningRules": [ ... ] },
    ...
  ]
}
```

| Field | Description | Required | Default |
|---|---|---|---|
| `type` | Must be `composing`. | Yes | |
| `providers` | Ordered list of [rule providers](#rule-provider-types). Provider order determines precedence. | Yes | |

The composing provider is ready only when all child providers are ready.

### Reindexing rule types

All rule types share the following common fields:

| Field | Description | Required |
|---|---|---|
| `id` | Unique identifier for the rule. | Yes |
| `description` | Human-readable description. | No |
| `olderThan` | ISO 8601 period defining the age threshold. The rule applies to data older than the current time minus this period. Must be non-negative. | Yes |

#### Partitioning rules

Partitioning rules control how data is physically laid out into segments. This includes the time bucketing (segment granularity) and how data within a time bucket is split (partitions spec).

This is a non-additive rule — only one partitioning rule applies per interval.

| Field | Description | Required |
|---|---|---|
| `id` | Rule identifier. | Yes |
| `description` | Human-readable description. | No |
| `olderThan` | ISO 8601 period. | Yes |
| `segmentGranularity` | Time granularity for segment buckets. Supported values: `MINUTE`, `FIFTEEN_MINUTE`, `HOUR`, `DAY`, `MONTH`, `QUARTER`, `YEAR`. | Yes |
| `partitionsSpec` | Defines how data within each time bucket is split into segments. Supports `dynamic` and `range` types. | Yes |
| `virtualColumns` | Virtual columns for partitioning by nested or derived fields. | No |

Example:

```json
{
  "id": "daily-range-30d",
  "olderThan": "P30D",
  "segmentGranularity": "DAY",
  "partitionsSpec": {
    "type": "range",
    "targetRowsPerSegment": 5000000,
    "partitionDimensions": ["channel", "countryName"]
  },
  "description": "Compact to daily segments with range partitioning for data older than 30 days"
}
```

#### Deletion rules

Deletion rules specify rows to remove during compaction. The `deleteWhere` field defines a [Druid filter](../querying/filters.md) that matches rows to **delete**. During processing, Druid wraps these filters in NOT logic — the compacted data retains rows that do **not** match the filter.

This is an additive rule — multiple deletion rules can apply to the same interval.

| Field | Description | Required |
|---|---|---|
| `id` | Rule identifier. | Yes |
| `description` | Human-readable description. | No |
| `olderThan` | ISO 8601 period. | Yes |
| `deleteWhere` | A [Druid filter](../querying/filters.md) matching rows to remove. | Yes |
| `virtualColumns` | Virtual columns for filtering on nested or derived fields. Virtual column names must be unique and consistent across rule evaluations. | No |

**What you write vs what happens:**

If you define two deletion rules:
- Rule 1: `deleteWhere: isRobot = true`
- Rule 2: `deleteWhere: countryName = null`

Druid applies them as: `NOT(isRobot = true OR countryName = null)`. The compacted segments retain only rows where `isRobot` is not true **and** `countryName` is not null.

Example:

```json
{
  "id": "remove-robots-90d",
  "olderThan": "P90D",
  "deleteWhere": {
    "type": "equals",
    "column": "isRobot",
    "matchValueType": "STRING",
    "matchValue": "true"
  },
  "description": "Remove robot traffic from data older than 90 days"
}
```

#### Index spec rules

Index spec rules control compression and encoding settings for compacted segments, independently of partitioning.

This is a non-additive rule — only one index spec rule applies per interval.

| Field | Description | Required |
|---|---|---|
| `id` | Rule identifier. | Yes |
| `description` | Human-readable description. | No |
| `olderThan` | ISO 8601 period. | Yes |
| `indexSpec` | An [IndexSpec](../ingestion/ingestion-spec.md#indexspec) object defining bitmap type, metric compression, and other encoding settings. | Yes |

Example:

```json
{
  "id": "compressed-90d",
  "olderThan": "P90D",
  "indexSpec": {
    "bitmap": { "type": "roaring" },
    "metricCompression": "lz4"
  },
  "description": "Use roaring bitmaps and lz4 compression for data older than 90 days"
}
```

#### Data schema rules

Data schema rules control the schema of compacted segments, including dimensions, metrics, query granularity, rollup, and projections.

This is a non-additive rule — only one data schema rule applies per interval. At least one of the optional fields must be non-null.

| Field | Description | Required |
|---|---|---|
| `id` | Rule identifier. | Yes |
| `description` | Human-readable description. | No |
| `olderThan` | ISO 8601 period. | Yes |
| `dimensionsSpec` | [Dimensions config](../ingestion/ingestion-spec.md#dimensionsspec) for the compacted segments. | No |
| `metricsSpec` | Array of [aggregator factories](../querying/aggregations.md) for rollup metrics. | No |
| `queryGranularity` | [Query granularity](../querying/granularities.md) for the compacted segments. | No |
| `rollup` | Whether to enable rollup. Set to `true` only when `metricsSpec` is defined. | No |
| `projections` | List of [aggregate projections](../querying/projections.md). | No |

Example:

```json
{
  "id": "rollup-30d",
  "olderThan": "P30D",
  "queryGranularity": "HOUR",
  "rollup": true,
  "metricsSpec": [
    { "type": "longSum", "name": "added", "fieldName": "added" },
    { "type": "longSum", "name": "deleted", "fieldName": "deleted" }
  ],
  "description": "Roll up to hourly query granularity for data older than 30 days"
}
```

## Example

The following example uses the `wikipedia` datasource and demonstrates a cascading reindexing supervisor with a partitioning rule and a deletion rule that does the following:

- Data older than 30 days is compacted into daily range-partitioned segments.
- Rows that have a `isRobot` column with a `true` value are deleted from data older than 90 days. 
- The `skipOffsetFromLatest` setting skips the most recent day of data.

```bash
curl --location --request POST 'http://localhost:8081/druid/indexer/v1/supervisor' \
--header 'Content-Type: application/json' \
--data-raw '{
  "type": "autocompact",
  "spec": {
    "type": "reindexCascade",
    "dataSource": "wikipedia",
    "defaultSegmentGranularity": "HOUR",
    "defaultPartitionsSpec": {
      "type": "dynamic",
      "maxRowsPerSegment": 5000000
    },
    "skipOffsetFromLatest": "P1D",
    "ruleProvider": {
      "type": "inline",
      "partitioningRules": [
        {
          "id": "daily-30d",
          "olderThan": "P30D",
          "segmentGranularity": "DAY",
          "partitionsSpec": {
            "type": "range",
            "targetRowsPerSegment": 5000000,
            "partitionDimensions": ["channel", "countryName"]
          },
          "description": "Compact to daily range-partitioned segments after 30 days"
        }
      ],
      "deletionRules": [
        {
          "id": "remove-bots-90d",
          "olderThan": "P90D",
          "deleteWhere": {
            "type": "equals",
            "column": "isRobot",
            "matchValueType": "STRING",
            "matchValue": "true"
          },
          "description": "Remove robot edits from data older than 90 days"
        }
      ]
    },
    "taskContext": {
      "maxNumTasks": 3
    }
  }
}'
```

This creates three timeline intervals:
- `[-inf, now - 90D)`: DAY granularity, bot edits deleted.
- `[now - 90D, now - 30D)`: DAY granularity, no deletions.
- `[now - 30D, now - 1D)`: HOUR granularity (defaults), no deletions. Data within the last day is skipped.

## Limitations

- **MSQ task engine only.** Cascading reindexing requires the MSQ task engine. The native engine is not supported.
- **Compaction supervisors only.** This feature is not available for auto-compaction using Coordinator duties.
- **No `partitionsSpec` in `tuningConfig`.** Partitioning is controlled exclusively by rules and defaults. Setting `partitionsSpec` inside `tuningConfig` causes a validation error.
- **Granularity must not coarsen toward the present.** Segment granularity must stay the same or become finer as you move from older to newer data. For example, DAY to HOUR is valid; HOUR to DAY is not.
- **`skipOffsetFromLatest` and `skipOffsetFromNow` are mutually exclusive.** You can set one or the other, not both.
- **`ALL` segment granularity is not supported.** This is the same limitation as standard auto-compaction.

## Learn more

See the following topics for more information:

* [Automatic compaction](automatic-compaction.md) for general auto-compaction configuration.
* [Auto-compaction using compaction supervisors](automatic-compaction.md#auto-compaction-using-compaction-supervisors) for supervisor setup and management.
* [MSQ task engine for auto-compaction](automatic-compaction.md#use-msq-for-auto-compaction) for MSQ engine requirements and limitations.
* [Compaction](compaction.md) for an overview of compaction in Druid.
