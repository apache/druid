---
id: upgrade-notes
title: "Upgrade notes"
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

The upgrade notes assume that you are upgrading from the preceding version of Druid. If you are upgrading across multiple versions, make sure you read the upgrade notes for all the versions.

## 28.0.0

### Upgrade notes

#### Upgrade Druid segments table

Druid 28.0.0 adds a new column to the Druid metadata table that requires an update to the table.

If `druid.metadata.storage.connector.createTables` is set to `true` and the metadata store user has DDL privileges, the segments table gets automatically updated at startup to include the new `used_flag_last_updated` column. No additional work is needed for the upgrade.

If either of those requirements are not met, pre-upgrade steps are required. You must make these updates before you upgrade to Druid 28.0.0, or the Coordinator and Overlord processes fail.

Although you can manually alter your table to add the new `used_flag_last_updated` column, Druid also provides a CLI tool to do it.

[#12599](https://github.com/apache/druid/pull/12599)

In the example commands below:

- `lib` is the Druid lib directory
- `extensions` is the Druid extensions directory
- `base` corresponds to the value of `druid.metadata.storage.tables.base` in the configuration, `druid` by default.
- The `--connectURI` parameter corresponds to the value of `druid.metadata.storage.connector.connectURI`.
- The `--user` parameter corresponds to the value of `druid.metadata.storage.connector.user`.
- The `--password` parameter corresponds to the value of `druid.metadata.storage.connector.password`.
- The `--action` parameter corresponds to the update action you are executing. In this case, it is `add-last-used-to-segments`

##### Upgrade step for MySQL

```bash
cd ${DRUID_ROOT}
java -classpath "lib/*" -Dlog4j.configurationFile=conf/druid/cluster/_common/log4j2.xml -Ddruid.extensions.directory="extensions" -Ddruid.extensions.loadList=[\"mysql-metadata-storage\"] -Ddruid.metadata.storage.type=mysql org.apache.druid.cli.Main tools metadata-update --connectURI="<mysql-uri>" --user USER --password PASSWORD --base druid --action add-used-flag-last-updated-to-segments
```

##### Upgrade step for PostgreSQL

```bash
cd ${DRUID_ROOT}
java -classpath "lib/*" -Dlog4j.configurationFile=conf/druid/cluster/_common/log4j2.xml -Ddruid.extensions.directory="extensions" -Ddruid.extensions.loadList=[\"postgresql-metadata-storage\"] -Ddruid.metadata.storage.type=postgresql org.apache.druid.cli.Main tools metadata-update --connectURI="<postgresql-uri>" --user  USER --password PASSWORD --base druid --action add-used-flag-last-updated-to-segments
```

##### Manual upgrade step

```SQL
ALTER TABLE druid_segments
ADD used_flag_last_updated varchar(255);
```

#### Recommended syntax for SQL UNNEST

The recommended syntax for SQL UNNEST has changed. We recommend using CROSS JOIN instead of commas for most queries to prevent issues with precedence. For example, use:

```sql
SELECT column_alias_name1 FROM datasource CROSS JOIN UNNEST(source_expression1) AS table_alias_name1(column_alias_name1) CROSS JOIN UNNEST(source_expression2) AS table_alias_name2(column_alias_name2), ...
```

Do not use:

```sql
SELECT column_alias_name FROM datasource, UNNEST(source_expression1) AS table_alias_name1(column_alias_name1), UNNEST(source_expression2) AS table_alias_name2(column_alias_name2), ...
```

#### Dynamic parameters

The Apache Calcite version has been upgraded from 1.21 to 1.35. As part of the Calcite upgrade, the behavior of type inference for dynamic parameters has changed. To avoid any type interference issues, explicitly `CAST` all dynamic parameters as a specific data type in SQL queries. For example, use:

```sql
SELECT (1 * CAST (? as DOUBLE))/2 as tmp
```

Do not use:

```sql
SELECT (1 * ?)/2 as tmp
```

#### Nested column format

`json` type columns created with Druid 28.0.0 are not backwards compatible with Druid versions older than 26.0.0.
If you are upgrading from a version prior to Druid 26.0.0 and you use `json` columns, upgrade to Druid 26.0.0 before you upgrade to Druid 28.0.0.
Additionally, to downgrade to a version older than Druid 26.0.0, any new segments created in Druid 28.0.0 should be re-ingested using Druid 26.0.0 or 27.0.0 prior to further downgrading.

When upgrading from a previous version, you can continue to write nested columns in a backwards compatible format (version 4).

In a classic batch ingestion job, include `formatVersion` in the `dimensions` list of the `dimensionsSpec` property. For example:

```json
      "dimensionsSpec": {
        "dimensions": [
          "product",
          "department",
          {
            "type": "json",
            "name": "shipTo",
            "formatVersion": 4
          }
        ]
      },
```

To set the default nested column version, set the desired format version in the common runtime properties. For example:

```java
druid.indexing.formats.nestedColumnFormatVersion=4
```

#### SQL compatibility

Starting with Druid 28.0.0, the default way Druid treats nulls and booleans has changed.

For nulls, Druid now differentiates between an empty string and a record with no data as well as between an empty numerical record and `0`.  
You can revert to the previous behavior by setting `druid.generic.useDefaultValueForNull` to `true`.

This property affects both storage and querying, and must be set on all Druid service types to be available at both ingestion time and query time. Reverting this setting to the old value restores the previous behavior without reingestion.

For booleans, Druid now strictly uses `1` (true) or `0` (false). Previously, true and false could be represented either as `true` and `false` as well as `1` and `0`, respectively. In addition, Druid now returns a null value for boolean comparisons like `True && NULL`.

You can revert to the previous behavior by setting `druid.expressions.useStrictBooleans` to `false`.
This property affects both storage and querying, and must be set on all Druid service types to be available at both ingestion time and query time. Reverting this setting to the old value restores the previous behavior without reingestion.

The following table illustrates some example scenarios and the impact of the changes.

<details><summary>Show the table</summary>

| Query| Druid 27.0.0 and earlier| Druid 28.0.0 and later|
|------|------------------------|----------------------|
| Query empty string| Empty string (`''`) or null| Empty string (`''`)|
| Query null string| Null or empty| Null|
| COUNT(*)| All rows, including nulls| All rows, including nulls|
| COUNT(column)| All rows excluding empty strings| All rows including empty strings but excluding nulls|
| Expression 100 && 11| 11| 1|
| Expression 100 &#124;&#124; 11| 100| 1|
| Null FLOAT/DOUBLE column| 0.0| Null|
| Null LONG column| 0| Null|
| Null `__time` column| 0, meaning 1970-01-01 00:00:00 UTC| 1970-01-01 00:00:00 UTC|
| Null MVD column| `''`| Null|
| ARRAY| Null| Null|
| COMPLEX| none| Null|
</details>

Before upgrading to Druid 28.0.0, update your queries to account for the changed behavior as described in the following sections.

##### NULL filters

If your queries use NULL in the filter condition to match both nulls and empty strings, you should add an explicit filter clause for empty strings. For example, update `s IS NULL` to `s IS NULL OR s = ''`.

##### COUNT functions

`COUNT(column)` now counts empty strings. If you want to continue excluding empty strings from the count, replace `COUNT(column)` with `COUNT(column) FILTER(WHERE column <> '')`.

##### GroupBy queries

GroupBy queries on columns containing null values can now have additional entries as nulls can co-exist with empty strings.

#### Stop Supervisors that ingest from multiple Kafka topics before downgrading

If you have added supervisors that ingest from multiple Kafka topics in Druid 28.0.0 or later, stop those supervisors before downgrading to a version prior to Druid 28.0.0 because the supervisors will fail in versions prior to Druid 28.0.0.

#### `lenientAggregatorMerge` deprecated

`lenientAggregatorMerge` property in segment metadata queries has been deprecated. It will be removed in future releases.
Use `aggregatorMergeStrategy` instead. `aggregatorMergeStrategy` also supports the `latest` and `earliest` strategies in addition to `strict` and `lenient` strategies from `lenientAggregatorMerge`.

[#14560](https://github.com/apache/druid/pull/14560)
[#14598](https://github.com/apache/druid/pull/14598)

#### Broker parallel merge config options

The paths for `druid.processing.merge.pool.*` and `druid.processing.merge.task.*` have been flattened to use `druid.processing.merge.*` instead. The legacy paths for the configs are now deprecated and will be removed in a future release. Migrate your settings to use the new paths because the old paths will be ignored in the future.

[#14695](https://github.com/apache/druid/pull/14695)

#### Ingestion options for ARRAY typed columns

Starting with Druid 28.0.0, the MSQ task engine can detect and ingest arrays as ARRAY typed columns when you set the query context parameter `arrayIngestMode` to `array`.
The `arrayIngestMode` context parameter controls how ARRAY type values are stored in Druid segments.

When you set `arrayIngestMode` to `array` (recommended for SQL compliance), the MSQ task engine stores all ARRAY typed values in [ARRAY typed columns](https://druid.apache.org/docs/latest/querying/arrays) and supports storing both VARCHAR and numeric typed arrays.

For backwards compatibility, `arrayIngestMode` defaults to `mvd`. When `"arrayIngestMode":"mvd"`, Druid only supports VARCHAR typed arrays and stores them as [multi-value string columns](https://druid.apache.org/docs/latest/querying/multi-value-dimensions).

When you set `arrayIngestMode` to `none`, Druid throws an exception when trying to store any type of arrays.

For more information on how to ingest `ARRAY` typed columns with SQL-based ingestion, see [SQL data types](https://druid.apache.org/docs/latest/querying/sql-data-types#arrays) and [Array columns](https://druid.apache.org/docs/latest/querying/arrays).

### Incompatible changes

#### Removed Hadoop 2

Support for Hadoop 2 has been removed.
Migrate to SQL-based ingestion or JSON-based batch ingestion if you are using Hadoop 2.x for ingestion today.
If migrating to Druid's built-in ingestion is not possible, you must upgrade your Hadoop infrastructure to 3.x+ before upgrading to Druid 28.0.0.

[#14763](https://github.com/apache/druid/pull/14763)

#### Removed GroupBy v1 

The GroupBy v1 engine has been removed. Use the GroupBy v2 engine instead, which has been the default GroupBy engine for several releases.
There should be no impact on your queries.

Additionally, `AggregatorFactory.getRequiredColumns` has been deprecated and will be removed in a future release. If you have an extension that implements `AggregatorFactory`, then this method should be removed from your implementation.

[#14866](https://github.com/apache/druid/pull/14866)

#### Removed Coordinator dynamic configs

The `decommissioningMaxPercentOfMaxSegmentsToMove` config has been removed.
The use case for this config is handled by smart segment loading now, which is enabled by default.

[#14923](https://github.com/apache/druid/pull/14923)

#### Removed `cachingCost` strategy

The `cachingCost` strategy for segment loading has been removed.
Use `cost` instead, which has the same benefits as `cachingCost`.

If you have `cachingCost` set, the system ignores this setting and automatically uses `cost`.

[#14798](https://github.com/apache/druid/pull/14798)

#### Removed `InsertCannotOrderByDescending`

The deprecated MSQ fault `InsertCannotOrderByDescending` has been removed.

[#14588](https://github.com/apache/druid/pull/14588)

#### Removed the backward compatibility code for the Handoff API

The backward compatibility code for the Handoff API in `CoordinatorBasedSegmentHandoffNotifier` has been removed.
If you are upgrading from a Druid version older than 0.14.0, upgrade to a newer version of Druid before upgrading to Druid 28.0.0.

[#14652](https://github.com/apache/druid/pull/14652)

## 27.0.0

### Upgrade notes

#### Worker input bytes for SQL-based ingestion

The maximum input bytes for each worker for SQL-based ingestion is now 512 MiB (previously 10 GiB).

[#14307](https://github.com/apache/druid/pull/14307)

#### Parameter execution changes for Kafka

When using the built-in `FileConfigProvider` for Kafka, interpolations are now intercepted by the JsonConfigurator instead of being passed down to the Kafka provider. This breaks existing deployments.

For more information, see [KIP-297](https://cwiki.apache.org/confluence/display/KAFKA/KIP-297%3A+Externalizing+Secrets+for+Connect+Configurations).

[#13023](https://github.com/apache/druid/pull/13023)

#### Hadoop 2 deprecated

Many of the important dependent libraries that Druid uses no longer support Hadoop 2. In order for Druid to stay current and have pathways to mitigate security vulnerabilities, the community has decided to deprecate support for Hadoop 2.x releases starting this release. Starting with Druid 28.x, Hadoop 3.x is the only supported Hadoop version.

Consider migrating to SQL-based ingestion or native ingestion if you are using Hadoop 2.x for ingestion today. If migrating to Druid ingestion is not possible, plan to upgrade your Hadoop infrastructure before upgrading to the next Druid release.

#### GroupBy v1 deprecated

GroupBy queries using the v1 legacy engine has been deprecated. It will be removed in future releases. Use v2 instead. Note that v2 has been the default GroupBy engine.

For more information, see [GroupBy queries](https://druid.apache.org/docs/latest/querying/groupbyquery.html).

#### Push-based real-time ingestion deprecated

Support for push-based real-time ingestion has been deprecated. It will be removed in future releases.

#### `cachingCost` segment balancing strategy deprecated

The `cachingCost` strategy has been deprecated and will be removed in future releases. Use an alternate segment balancing strategy instead, such as `cost`.

#### Segment loading config changes

The following segment related configs are now deprecated and will be removed in future releases: 

* `maxSegmentsInNodeLoadingQueue`
* `maxSegmentsToMove`
* `replicationThrottleLimit`
* `useRoundRobinSegmentAssignment`
* `replicantLifetime`
* `maxNonPrimaryReplicantsToLoad`
* `decommissioningMaxPercentOfMaxSegmentsToMove`

Use `smartSegmentLoading` mode instead, which calculates values for these variables automatically.

Additionally, the defaults for the following Coordinator dynamic configs have changed:

* `maxsegmentsInNodeLoadingQueue` : 500, previously 100
* `maxSegmentsToMove`: 100, previously 5
* `replicationThrottleLimit`: 500, previously 10

These new defaults can improve performance for most use cases.

[#13197](https://github.com/apache/druid/pull/13197)
[#14269](https://github.com/apache/druid/pull/14269)

#### `SysMonitor` support deprecated

Switch to `OshiSysMonitor` as `SysMonitor` is now deprecated and will be removed in future releases.

### Incompatible changes

#### Removed property for setting max bytes for dimension lookup cache

`druid.processing.columnCache.sizeBytes` has been removed since it provided limited utility after a number of internal changes. Leaving this config is harmless, but it does nothing.

[#14500](https://github.com/apache/druid/pull/14500)

#### Removed Coordinator dynamic configs

The following Coordinator dynamic configs have been removed:

* `emitBalancingStats`: Stats for errors encountered while balancing will always be emitted. Other debugging stats will not be emitted but can be logged by setting the appropriate `debugDimensions`.
* `useBatchedSegmentSampler` and `percentOfSegmentsToConsiderPerMove`: Batched segment sampling is now the standard and will always be on.

Use the new [smart segment loading](#smart-segment-loading) mode instead.

[#14524](https://github.com/apache/druid/pull/14524)

## 26.0.0

### Upgrade notes

#### Real-time tasks

Optimized query performance by lowering the default maxRowsInMemory for real-time ingestion, which might lower overall ingestion throughput.

[#13939](https://github.com/apache/druid/pull/13939)

### Incompatible changes

#### Firehose ingestion removed

The firehose/parser specification used by legacy Druid streaming formats is removed.
Firehose ingestion was deprecated in version 0.17, and support for this ingestion was removed in version 24.0.0.

[#12852](https://github.com/apache/druid/pull/12852)

#### Information schema now uses numeric column types

The Druid system table (`INFORMATION_SCHEMA`) now uses SQL types instead of Druid types for columns. This change makes the `INFORMATION_SCHEMA` table behave more like standard SQL. You may need to update your queries in the following scenarios in order to avoid unexpected results if you depend either of the following:

* Numeric fields being treated as strings.
* Column numbering starting at 0. Column numbering is now 1-based.

[#13777](https://github.com/apache/druid/pull/13777)

#### `frontCoded` segment format change

The `frontCoded` type of `stringEncodingStrategy` on `indexSpec` with a new segment format version, which typically has faster read speeds and reduced segment size. This improvement is backwards incompatible with Druid 25.0.0.

## 25.0.0

### Upgrade notes

#### Default HTTP-based segment discovery and task management

The default segment discovery method now uses HTTP instead of ZooKeeper.

This update changes the defaults for the following properties:

|Property|New default|Previous default|
|--------|-----------|----------------|
|`druid.serverview.type` for segment management|http|batch|
|`druid.coordinator.loadqueuepeon.type` for segment management|http| curator|
|`druid.indexer.runner.type` for the Overlord|httpRemote|local|

To use ZooKeeper instead of HTTP, change the values for the properties back to the previous defaults. ZooKeeper-based implementations for these properties are deprecated and will be removed in a subsequent release.

[#13092](https://github.com/apache/druid/pull/13092)

#### Finalizing HLL and quantiles sketch aggregates

The aggregation functions for HLL and quantiles sketches returned sketches or numbers when they are finalized depending on where they were in the native query plan.

Druid no longer finalizes aggregators in the following two cases:

* aggregators appear in the outer level of a query
* aggregators are used as input to an expression or finalizing-field-access post-aggregator

This change aligns the behavior of HLL and quantiles sketches with theta sketches.

To restore old behavior, you can set `sqlFinalizeOuterSketches=true` in the query context.

[#13247](https://github.com/apache/druid/pull/13247)

#### Kill tasks mark segments as unused only if specified

When you issue a kill task, Druid marks the underlying segments as unused only if explicitly specified. For more information, see the [API reference](https://druid.apache.org/docs/latest/operations/api-reference.html#coordinator).

[#13104](https://github.com/apache/druid/pull/13104)

### Incompatible changes

#### Upgrade curator to 5.3.0

Apache Curator upgraded to the latest version, 5.3.0. This version drops support for ZooKeeper 3.4 but Druid has already officially dropped support in 0.22. In 5.3.0, Curator has removed support for Exhibitor so all related configurations and tests have been removed.

[#12939](https://github.com/apache/druid/pull/12939)

#### Fixed Parquet list conversion

The behavior of the parquet reader for lists of structured objects has been changed to be consistent with other parquet logical list conversions. The data is now fetched directly, more closely matching its expected structure.

[#13294](https://github.com/apache/druid/pull/13294)