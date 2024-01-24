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

The upgrade notes assume that you are upgrading from the Druid version that immediately precedes your target version. If you are upgrading across multiple versions, make sure you read the upgrade notes for all the intermediate versions.

For the full release notes for a specific version, see the [releases page](https://github.com/apache/druid/releases).

## 28.0.0

### Upgrade notes

#### Upgrade Druid segments table

Druid 28.0.0 adds a new column to the Druid metadata table that requires an update to the table.

If `druid.metadata.storage.connector.createTables` is set to `true` and the metadata store user has DDL privileges, the segments table gets automatically updated at startup to include the new `used_status_last_updated` column. No additional work is needed for the upgrade.

If either of those requirements are not met, pre-upgrade steps are required. You must make these updates before you upgrade to Druid 28.0.0, or the Coordinator and Overlord processes fail.

Although you can manually alter your table to add the new `used_status_last_updated` column, Druid also provides a [CLI tool](https://druid.apache.org/docs/latest/operations/metadata-migration/#create-druid-tables) to do it.

[#12599](https://github.com/apache/druid/pull/12599) [#14868](https://github.com/apache/druid/pull/14868)

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
java -classpath "lib/*" -Dlog4j.configurationFile=conf/druid/cluster/_common/log4j2.xml -Ddruid.extensions.directory="extensions" -Ddruid.extensions.loadList=[\"postgresql-metadata-storage\"] -Ddruid.metadata.storage.type=postgresql org.apache.druid.cli.Main tools metadata-update --connectURI="<postgresql-uri>" --user  USER --password PASSWORD --base druid --action add-used-flag-last-updated-to-segments
```

##### Manual upgrade step

```SQL
ALTER TABLE druid_segments
ADD used_status_last_updated varchar(255);
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

Use the new [smart segment loading](https://druid.apache.org/docs/latest/configuration/#smart-segment-loading) mode instead.

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

When you issue a kill task, Druid marks the underlying segments as unused only if explicitly specified. For more information, see the [API reference](https://druid.apache.org/docs/latest/api-reference/data-management-api).

[#13104](https://github.com/apache/druid/pull/13104)

### Incompatible changes

#### Upgrade curator to 5.3.0

Apache Curator upgraded to the latest version, 5.3.0. This version drops support for ZooKeeper 3.4 but Druid has already officially dropped support in 0.22. In 5.3.0, Curator has removed support for Exhibitor so all related configurations and tests have been removed.

[#12939](https://github.com/apache/druid/pull/12939)

#### Fixed Parquet list conversion

The behavior of the parquet reader for lists of structured objects has been changed to be consistent with other parquet logical list conversions. The data is now fetched directly, more closely matching its expected structure.

[#13294](https://github.com/apache/druid/pull/13294)

## 24.0.0

### Upgrade notes

#### Permissions for multi-stage query engine

To read external data using the multi-stage query task engine, you must have READ permissions for the EXTERNAL resource type. Users without the correct permission encounter a 403 error when trying to run SQL queries that include EXTERN.

The way you assign the permission depends on your authorizer. For example, with [basic security](https://github.com/apache/druid/blob/druid-24.0.0/docs/operations/security-user-auth.md) in Druid, add the EXTERNAL READ permission by sending a POST request to the [roles API](https://github.com/apache/druid/blob/druid-24.0.0/docs/development/extensions-core/druid-basic-security.md#permissions).

The example adds permissions for users with the admin role using a basic authorizer named MyBasicMetadataAuthorizer. The following permissions are granted:

* DATASOURCE READ
* DATASOURCE WRITE
* CONFIG READ
* CONFIG WRITE
* STATE READ
* STATE WRITE
* EXTERNAL READ

```
curl --location --request POST 'http://localhost:8081/druid-ext/basic-security/authorization/db/MyBasicMetadataAuthorizer/roles/admin/permissions' \
--header 'Content-Type: application/json' \
--data-raw '[
{
  "resource": {
    "name": ".*",
    "type": "DATASOURCE"
  },
  "action": "READ"
},
{
  "resource": {
    "name": ".*",
    "type": "DATASOURCE"
  },
  "action": "WRITE"
},
{
  "resource": {
    "name": ".*",
    "type": "CONFIG"
  },
  "action": "READ"
},
{
  "resource": {
    "name": ".*",
    "type": "CONFIG"
  },
  "action": "WRITE"
},
{
  "resource": {
    "name": ".*",
    "type": "STATE"
  },
  "action": "READ"
},
{
  "resource": {
    "name": ".*",
    "type": "STATE"
  },
  "action": "WRITE"
},
{
  "resource": {
    "name": "EXTERNAL",
    "type": "EXTERNAL"
  },
  "action": "READ"
}
]'
```

#### Behavior for unused segments

Druid automatically retains any segments marked as unused. Previously, Druid permanently deleted unused segments from metadata store and deep storage after their duration to retain passed. This behavior was reverted from 0.23.0.

[#12693](https://github.com/apache/druid/pull/12693)

#### Default for `druid.processing.fifo`

The default for `druid.processing.fifo` is now true. This means that tasks of equal priority are treated in a FIFO manner. For most use cases, this change can improve performance on heavily loaded clusters.

[#12571](https://github.com/apache/druid/pull/12571)

#### Update to JDBC statement closure

In previous releases, Druid automatically closed the JDBC Statement when the ResultSet was closed. Druid closed the ResultSet on EOF. Druid closed the statement on any exception. This behavior is, however, non-standard.
In this release, Druid's JDBC driver follows the JDBC standards more closely:
The ResultSet closes automatically on EOF, but does not close the Statement or PreparedStatement. Your code must close these statements, perhaps by using a try-with-resources block.
The PreparedStatement can now be used multiple times with different parameters. (Previously this was not true since closing the ResultSet closed the PreparedStatement.)
If any call to a Statement or PreparedStatement raises an error, the client code must still explicitly close the statement. According to the JDBC standards, statements are not closed automatically on errors. This allows you to obtain information about a failed statement before closing it.
If you have code that depended on the old behavior, you may have to change your code to add the required close statement.

[#12709](https://github.com/apache/druid/pull/12709)

## 0.23.0

### Upgrade notes

#### Auto-killing of segments

In 0.23.0, Auto killing of segments is now enabled by default [(#12187)](https://github.com/apache/druid/pull/12187). The new defaults should kill all unused segments older than 90 days. If users do not want this behavior on an upgrade, they should explicitly disable the behavior. This is a risky change since depending on the interval, segments will be killed immediately after being marked unused. this behavior will be reverted or changed in the next druid release. Please see [(#12693)](https://github.com/apache/druid/pull/12693) for more details.

#### Other changes

# Other changes

- Kinesis ingestion requires `listShards` API access on the stream.
- Kafka clients libraries have been upgraded to 3.0.0 [(#11735)](https://github.com/apache/druid/pull/11735)
- The dynamic coordinator config, `percentOfSegmentsToConsiderPerMove` has been deprecated and will be removed in a future release of Druid. It is being replaced by a new segment picking strategy introduced in [(#11257)](https://github.com/apache/druid/pull/11257). This new strategy is currently toggled off by default, but can be toggled on if you set the dynamic coordinator config `useBatchedSegmentSampler` to true. Setting this as such, will disable the use of the deprecated `percentOfSegmentsToConsiderPerMove`. In a future release, `useBatchedSegmentSampler` will become permanently true. [(#11960)](https://github.com/apache/druid/pull/11960)

## 0.22.0

### Upgrade notes

#### Dropped support for Apache ZooKeeper 3.4

Following up to 0.21, which officially deprecated support for ZooKeeper 3.4, [which has been end-of-life for a while](https://lists.apache.org/thread/xckr6nnsg9rxchkbvltkvt7hr2d0mhbo), support for ZooKeeper 3.4 is now removed in 0.22.0. Be sure to upgrade your ZooKeeper cluster prior to upgrading your Druid cluster to 0.22.0.

[#10780](https://github.com/apache/druid/issues/10780)
[#11073](https://github.com/apache/druid/pull/11073)

#### Native batch ingestion segment allocation fix

Druid 0.22.0 includes an important bug-fix in native batch indexing where transient failures of indexing sub-tasks can result in non-contiguous partitions in the result segments, which will never become queryable due to logic which checks for the 'complete' set. This issue has been resolved in the latest version of Druid, but required a change in the protocol which batch tasks use to allocate segments, and this change can cause issues during rolling downgrades if you decide to roll back from Druid 0.22.0 to an earlier version.

To avoid task failure during a rolling-downgrade, set

```
druid.indexer.task.default.context={ "useLineageBasedSegmentAllocation" : false }
```

in the overlord runtime properties, and wait for all tasks which have `useLineageBasedSegmentAllocation` set to true to complete before initiating the downgrade. After these tasks have all completed the downgrade shouldn't have any further issue and the setting can be removed from the overlord configuration (recommended, as you will want this setting enabled if you are running Druid 0.22.0 or newer).

[#11189](https://github.com/apache/druid/pull/11189)

#### SQL timeseries no longer skip empty buckets with all granularity

Prior to Druid 0.22, an SQL group by query which is using a single universal grouping key (e.g. only aggregators) such as `SELECT COUNT(*), SUM(x) FROM y WHERE z = 'someval'` would produce an empty result set instead of `[0, null]` that might be expected from this query matching no results. This was because underneath this would plan into a timeseries query with 'ALL' granularity, and skipEmptyBuckets set to true in the query context. This latter option caused the results of such a query to return no results, as there are no buckets with values to aggregate and so they are skipped, making an empty result set instead of a 'nil' result set. This behavior has been changed to behave in line with other SQL implementations, but the previous behavior can be obtained by explicitly setting `skipEmptyBuckets` on the query context.

[#11188](https://github.com/apache/druid/pull/11188)

#### Druid reingestion incompatible changes

Batch tasks using a 'Druid' input source to reingest segment data will no longer accept the 'dimensions' and 'metrics' sections of their task spec, and now will internally use a new columns filter to specify which columns from the original segment should be retained. Additionally, timestampSpec is no longer ignored, allowing the __time column to be modified or replaced with a different column. These changes additionally fix a bug where transformed columns would be ignored and unavailable on the new segments.

[#10267](https://github.com/apache/druid/pull/10267)

#### Druid web-console no longer supports IE11 and other older browsers

Some things might still work, but it is no longer officially supported so that newer Javascript features can be used to develop the web-console.

[#11357](https://github.com/apache/druid/pull/11357)

#### Changed default maximum segment loading queue size

Druid coordinator `maxSegmentsInNodeLoadingQueue` dynamic configuration has been changed from unlimited (`0`) to `100`. This should make the coordinator behave in a much more relaxed manner during periods of cluster volatility, such as a rolling upgrade, but caps the total number of segments that will be loaded in any given coordinator cycle to 100 per server, which can slow down the speed at which a completely stopped cluster is started and loaded from deep storage.

[#11540](https://github.com/apache/druid/pull/11540)

## 0.21.0

#### Improved HTTP status codes for query errors

Before this release, Druid returned the "internal error (500)" for most of the query errors. Now Druid returns different error codes based on their cause. The following table lists the errors and their corresponding codes that has changed:

| Exception | Description| Old code | New code |
|-----|-|--|-----|
| SqlParseException and ValidationException from Calcite | Query planning failed | 500 | 400 |
| QueryTimeoutException | Query execution didn't finish in timeout | 500 | 504 |
| ResourceLimitExceededException | Query asked more resources than configured threshold | 500 | 400 |
| InsufficientResourceException | Query failed to schedule because of lack of merge buffers available at the time when it was submitted | 500 | 429, merged to QueryCapacityExceededException |
| QueryUnsupportedException | Unsupported functionality | 400 | 501 |

[#10464](https://github.com/apache/druid/pull/10464)
[#10746](https://github.com/apache/druid/pull/10746)

####  Query interrupted metric
`query/interrupted/count` no longer counts the queries that timed out. These queries are counted by `query/timeout/count`.

#### context dimension in query metrics

`context` is now a default dimension emitted for all query metrics. `context` is a JSON-formatted string containing the query context for the query that the emitted metric refers to. The addition of a dimension that was not previously alters some metrics emitted by Druid. You should plan to handle this new `context` dimension in your metrics pipeline. Since the dimension is a JSON-formatted string, a common solution is to parse the dimension and either flatten it or extract the bits you want and discard the full JSON-formatted string blob.

[#10578](https://github.com/apache/druid/pull/10578)

#### Deprecated support for Apache ZooKeeper 3.4

As [ZooKeeper 3.4 has been end-of-life for a while](https://mail-archives.apache.org/mod_mbox/zookeeper-user/202004.mbox/%3C41A7EC67-D8F4-4C3A-B2DB-C2741C2EECA3%40apache.org%3E), support for ZooKeeper 3.4 is deprecated in 0.21.0 and will be removed in the near future.

[#10780](https://github.com/apache/druid/issues/10780)

#### Consistent serialization format and column naming convention for the sys.segments table

All columns in the `sys.segments` table are now serialized in the JSON format to make them consistent with other system tables. Column names now use the same "snake case" convention.

[#10481](https://github.com/apache/druid/pull/10481)
