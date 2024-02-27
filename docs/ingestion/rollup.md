---
id: rollup
title: "Data rollup"
sidebar_label: Rollup
description: Introduces rollup as a concept. Provides suggestions to maximize the benefits of rollup. Differentiates between perfect and best-effort rollup.
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

Druid can roll up data at ingestion time to reduce the amount of raw data to  store on disk. Rollup is a form of summarization or pre-aggregation. Rolling up data can dramatically reduce the size of data to be stored and reduce row counts by potentially orders of magnitude. As a trade-off for the efficiency of rollup, you lose the ability to query individual events.

At ingestion time, you control rollup with the `rollup` setting in the [`granularitySpec`](./ingestion-spec.md#granularityspec). Rollup is enabled by default. This means Druid combines into a single row any rows that have identical [dimension](./schema-model.md#dimensions) values and [timestamp](./schema-model.md#primary-timestamp) values after [`queryGranularity`-based truncation](./ingestion-spec.md#granularityspec).

When you disable rollup, Druid loads each row as-is without doing any form of pre-aggregation. This mode is similar to databases that do not support a rollup feature. Set `rollup` to `false` if you want Druid to store each record as-is, without any rollup summarization.

Use roll-up when creating a table datasource if both:

- You want optimal performance or you have strict space constraints.
- You don't need raw values from [high-cardinality dimensions](schema-design.md#sketches).

Conversely, disable roll-up if either:

- You need results for individual rows.
- You need to execute `GROUP BY` or `WHERE` queries on _any_ column.

If you have conflicting needs for different use cases, you can create multiple tables with different roll-up configurations on each table.

## Maximizing rollup ratio

To measure the rollup ratio of a datasource, compare the number of rows in Druid (`COUNT`) with the number of ingested events. For example, run a [Druid SQL](../querying/sql.md) query where "num_rows" refers to a `count`-type metric generated at ingestion time as follows:

```sql
SELECT SUM("num_rows") / (COUNT(*) * 1.0) FROM datasource
```

The higher the result, the greater the benefit you gain from rollup. See [Counting the number of ingested events](schema-design.md#counting) for more details about how counting works with rollup is enabled.

Tips for maximizing rollup:

- Design your schema with fewer dimensions and lower cardinality dimensions to yield better rollup ratios.
- Use [sketches](schema-design.md#sketches) to avoid storing high cardinality dimensions, which decrease rollup ratios.
- Adjust your `queryGranularity` at ingestion time to increase the chances that multiple rows in Druid having matching timestamps. For example, use five minute query granularity (`PT5M`) instead of one minute (`PT1M`).
- You can optionally load the same data into more than one Druid datasource. For example:
  - Create a "full" datasource that has rollup disabled, or enabled, but with a minimal rollup ratio.
  - Create a second "abbreviated" datasource with fewer dimensions and a higher rollup ratio.
     When queries only involve dimensions in the "abbreviated" set, use the second datasource to reduce query times. Often, this method only requires a small increase in storage footprint because abbreviated datasources tend to be substantially smaller.
- If you use a [best-effort rollup](#perfect-rollup-vs-best-effort-rollup) ingestion configuration that does not guarantee perfect rollup, try one of the following:
  - Switch to a guaranteed perfect rollup option.
  - [Reindex](../data-management/update.md#reindex) or [compact](../data-management/compaction.md) your data in the background after initial ingestion.

## Perfect rollup vs best-effort rollup

Depending on the ingestion method, Druid has the following rollup options:

- Guaranteed _perfect rollup_: Druid perfectly aggregates input data at ingestion time.
- _Best-effort rollup_: Druid may not perfectly aggregate input data. Therefore, multiple segments might contain rows with the same timestamp and dimension values.

In general, ingestion methods that offer best-effort rollup do this for one of the following reasons:

- The ingestion method parallelizes ingestion without a shuffling step required for perfect rollup.
- The ingestion method uses _incremental publishing_ which means it finalizes and publishes segments before all data for a time chunk has been received.
In both of these cases, records that could theoretically be rolled up may end up in different segments. All types of streaming ingestion run in this mode.

Ingestion methods that guarantee perfect rollup use an additional preprocessing step to determine intervals and partitioning before data ingestion. This preprocessing step scans the entire input dataset. While this step increases the time required for ingestion, it provides information necessary for perfect rollup.

The following table shows how each method handles rollup:

|Method|How it works|
|------|------------|
|[Native batch](native-batch.md)|`index_parallel` and `index` type may be either perfect or best-effort, based on configuration.|
|[SQL-based batch](../multi-stage-query/index.md)|Always perfect.|
|[Hadoop](hadoop.md)|Always perfect.|
|[Kafka indexing service](../ingestion/kafka-ingestion.md)|Always best-effort.|
|[Kinesis indexing service](../ingestion/kinesis-ingestion.md)|Always best-effort.|

## Learn more

See the following topic for more information:

- [Rollup tutorial](../tutorials/tutorial-rollup.md) for an example of how to configure rollup, and of how the feature modifies your data.
