---
id: clean-metadata-store
title: "Automated cleanup for metadata records"
sidebar_label: Automated metadata cleanup
description: "Defines a strategy to maintain Druid metadata store performance by automatically removing leftover records for deleted entities: datasources, supervisors, rules, compaction configuration, audit records, etc. Most applicable to databases with 'high-churn' datasources."
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

Apache Druid relies on [metadata storage](../design/metadata-storage.md) to track information on data storage, operations, and system configuration.
The metadata store includes the following:

- Segment records
- Audit records
- Supervisor records
- Rule records
- Compaction configuration records
- Datasource records created by supervisors
- Indexer task logs

When you delete some entities from Apache Druid, records related to the entity may remain in the metadata store.
If you have a high datasource churn rate, meaning you frequently create and delete many short-lived datasources or other related entities like compaction configuration or rules, the leftover records can fill your metadata store and cause performance issues.
To maintain metadata store performance, you can configure Apache Druid to automatically remove records associated with deleted entities from the metadata store.

By default, Druid automatically cleans up metadata older than 90 days.
This applies to all metadata entities in this topic except compaction configuration records and indexer task logs, for which cleanup is disabled by default.
You can configure the retention period for each metadata type, when available, through the record's `durationToRetain` property.
Certain records may require additional conditions be satisfied before clean up occurs.

See the [example](#example) for how you can customize the automated metadata cleanup for a specific use case.


## Automated cleanup strategies

There are several cases when you should consider automated cleanup of the metadata related to deleted datasources:
- If you know you have many high-churn datasources, for example, you have scripts that create and delete supervisors regularly.
- If you have issues with the hard disk for your metadata database filling up.
- If you run into performance issues with the metadata database. For example, API calls are very slow or fail to execute.

If you have compliance requirements to keep audit records and you enable automated cleanup for audit records, use alternative methods to preserve audit metadata, for example, by periodically exporting audit metadata records to external storage.

## Configure automated metadata cleanup

You can configure cleanup for each entity separately, as described in this section.
Define the properties in the `coordinator/runtime.properties` file.

The cleanup of one entity may depend on the cleanup of another entity as follows:
- You have to configure a [kill task for segment records](#kill-task) before you can configure automated cleanup for [rules](#rules-records) or [compaction configuration](#compaction-configuration-records).
- You have to schedule the metadata management tasks to run at the same or higher frequency as your most frequent cleanup job. For example, if your most frequent cleanup job is every hour, set the metadata store management period to one hour or less: `druid.coordinator.period.metadataStoreManagementPeriod=P1H`.

For details on configuration properties, see [Metadata management](../configuration/index.md#metadata-management).
If you want to skip the details, check out the [example](#example) for configuring automated metadata cleanup.

<a name="kill-task"></a>
### Segment records and segments in deep storage (kill task)

:::info
 The kill task is the only configuration in this topic that affects actual data in deep storage and not simply metadata or logs.
:::

Segment records and segments in deep storage become eligible for deletion when both of the following conditions hold:

- When they meet the eligibility requirement of kill task datasource configuration according to `killDataSourceWhitelist` set in the Coordinator dynamic configuration. See [Dynamic configuration](../configuration/index.md#dynamic-configuration).
- When the `durationToRetain` time has passed since their creation.

Kill tasks use the following configuration:
- `druid.coordinator.kill.on`: When `true`, enables the Coordinator to submit a kill task for unused segments, which deletes them completely from metadata store and from deep storage.
Only applies to the specified datasources in the dynamic configuration parameter `killDataSourceWhitelist`.
If `killDataSourceWhitelist` is not set or empty, then kill tasks can be submitted for all datasources.
- `druid.coordinator.kill.period`: Defines the frequency in [ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601#Durations) for the cleanup job to check for and delete eligible segments. Defaults to `P1D`. Must be greater than `druid.coordinator.period.indexingPeriod`. 
- `druid.coordinator.kill.durationToRetain`: Defines the retention period in [ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601#Durations) after creation that segments become eligible for deletion.
- `druid.coordinator.kill.ignoreDurationToRetain`: A way to override `druid.coordinator.kill.durationToRetain`. When enabled, the coordinator considers all unused segments as eligible to be killed.
- `druid.coordinator.kill.bufferPeriod`: Defines the amount of time that a segment must be unused before it can be permanently removed from metadata and deep storage. This serves as a buffer period to prevent data loss if data ends up being needed after being marked unused.
- `druid.coordinator.kill.maxSegments`: Defines the maximum number of segments to delete per kill task.

### Audit records

All audit records become eligible for deletion when the `durationToRetain` time has passed since their creation.

Audit cleanup uses the following configuration:
 - `druid.coordinator.kill.audit.on`: When `true`, enables cleanup for audit records.
 - `druid.coordinator.kill.audit.period`: Defines the frequency in [ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601#Durations) for the cleanup job to check for and delete eligible audit records. Defaults to `P1D`.
 - `druid.coordinator.kill.audit.durationToRetain`: Defines the retention period in [ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601#Durations) after creation that audit records become eligible for deletion.

### Supervisor records

Supervisor records become eligible for deletion when the supervisor is terminated and the `durationToRetain` time has passed since their creation.

Supervisor cleanup uses the following configuration:
 - `druid.coordinator.kill.supervisor.on`: When `true`, enables cleanup for supervisor records.
 - `druid.coordinator.kill.supervisor.period`: Defines the frequency in [ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601#Durations) for the cleanup job to check for and delete eligible supervisor records. Defaults to `P1D`.
 - `druid.coordinator.kill.supervisor.durationToRetain`: Defines the retention period in [ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601#Durations) after creation that supervisor records become eligible for deletion.

### Rules records

Rule records become eligible for deletion when all segments for the datasource have been killed by the kill task and the `durationToRetain` time has passed since their creation. Automated cleanup for rules requires a [kill task](#kill-task).

Rule cleanup uses the following configuration:
 - `druid.coordinator.kill.rule.on`: When `true`, enables cleanup for rules records.
 - `druid.coordinator.kill.rule.period`: Defines the frequency in [ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601#Durations) for the cleanup job to check for and delete eligible rules records. Defaults to `P1D`.
 - `druid.coordinator.kill.rule.durationToRetain`: Defines the retention period in [ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601#Durations) after creation that rules records become eligible for deletion.

### Compaction configuration records

Druid retains all compaction configuration records by default, which should be suitable for most use cases.
If you create and delete short-lived datasources with high frequency, and you set auto compaction configuration on those datasources, then consider turning on automated cleanup of compaction configuration records.

:::info
 With automated cleanup of compaction configuration records, if you create a compaction configuration for some datasource before the datasource exists, for example if initial ingestion is still ongoing, Druid may remove the compaction configuration.
To prevent the configuration from being prematurely removed, wait for the datasource to be created before applying the compaction configuration to the datasource.
:::

Unlike other metadata records, compaction configuration records do not have a retention period set by `durationToRetain`. Druid deletes compaction configuration records at every cleanup cycle for inactive datasources, which do not have segments either used or unused.

Compaction configuration records in the `druid_config` table become eligible for deletion after all segments for the datasource have been killed by the kill task. Automated cleanup for compaction configuration requires a [kill task](#kill-task).

Compaction configuration cleanup uses the following configuration:
 - `druid.coordinator.kill.compaction.on`: When `true`, enables cleanup for compaction configuration records.
 - `druid.coordinator.kill.compaction.period`: Defines the frequency in [ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601#Durations) for the cleanup job to check for and delete eligible compaction configuration records. Defaults to `P1D`.


:::info
If you already have an extremely large compaction configuration, you may not be able to delete compaction configuration due to size limits with the audit log. In this case you can set `druid.audit.manager.maxPayloadSizeBytes` and `druid.audit.manager.skipNullField` to avoid the auditing issue. See [Audit logging](../configuration/index.md#audit-logging).
:::

### Datasource records created by supervisors

Datasource records created by supervisors become eligible for deletion when the supervisor is terminated or does not exist in the `druid_supervisors` table and the `durationToRetain` time has passed since their creation.

Datasource cleanup uses the following configuration:
 - `druid.coordinator.kill.datasource.on`: When `true`, enables cleanup datasources created by supervisors.
 - `druid.coordinator.kill.datasource.period`: Defines the frequency in [ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601#Durations) for the cleanup job to check for and delete eligible datasource records. Defaults to `P1D`.
 - `druid.coordinator.kill.datasource.durationToRetain`: Defines the retention period in [ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601#Durations) after creation that datasource records become eligible for deletion.

### Indexer task logs

You can configure the Overlord to periodically delete indexer task logs and associated metadata. During cleanup, the Overlord removes the following:
* Indexer task logs from deep storage.
* Indexer task log metadata from the tasks and tasklogs tables in [metadata storage](../configuration/index.md#metadata-storage) (named `druid_tasks` and `druid_tasklogs` by default). Druid no longer uses the tasklogs table, and the table is always empty.

To configure cleanup of task logs by the Overlord, set the following properties in the `overlord/runtime.properties` file.

Indexer task log cleanup on the Overlord uses the following configuration:
- `druid.indexer.logs.kill.enabled`: When `true`, enables cleanup of task logs.
- `druid.indexer.logs.kill.durationToRetain`: Defines the length of time in milliseconds to retain task logs.
- `druid.indexer.logs.kill.initialDelay`: Defines the length of time in milliseconds after the Overlord starts before it executes its first job to kill task logs.
- `druid.indexer.logs.kill.delay`: The length of time in milliseconds between jobs to kill task logs.

For more detail, see [Task logging](../configuration/index.md#task-logging).


## Disable automated metadata cleanup

Druid automatically cleans up metadata records, excluding compaction configuration records and indexer task logs.
To disable automated metadata cleanup, set the following properties in the `coordinator/runtime.properties` file:

```properties
# Keep unused segments
druid.coordinator.kill.on=false

# Keep audit records
druid.coordinator.kill.audit.on=false

# Keep supervisor records
druid.coordinator.kill.supervisor.on=false

# Keep rules records
druid.coordinator.kill.rule.on=false

# Keep datasource records created by supervisors
druid.coordinator.kill.datasource.on=false
```

<a name="example"></a>
## Example configuration for automated metadata cleanup

Consider a scenario where you have scripts to create and delete hundreds of datasources and related entities a day. You do not want to fill your metadata store with leftover records. The datasources and related entities tend to persist for only one or two days. Therefore, you want to run a cleanup job that identifies and removes leftover records that are at least four days old after a seven day buffer period in case you want to recover the data. The exception is for audit logs, which you need to retain for 30 days:

```properties
...
# Schedule the metadata management store task for every hour:
druid.coordinator.period.metadataStoreManagementPeriod=PT1H

# Set a kill task to poll every day to delete segment records and segments
# in deep storage > 4 days old after a 7-day buffer period. When druid.coordinator.kill.on is set to true,
# you can set killDataSourceWhitelist in the dynamic configuration to limit
# the datasources that can be killed.
# Required also for automated cleanup of rules and compaction configuration.

druid.coordinator.kill.on=true
druid.coordinator.kill.period=P1D
druid.coordinator.kill.durationToRetain=P4D
druid.coordinator.kill.bufferPeriod=P7D
druid.coordinator.kill.maxSegments=1000

# Poll every day to delete audit records > 30 days old
druid.coordinator.kill.audit.on=true
druid.coordinator.kill.audit.period=P1D
druid.coordinator.kill.audit.durationToRetain=P30D

# Poll every day to delete supervisor records > 4 days old
druid.coordinator.kill.supervisor.on=true
druid.coordinator.kill.supervisor.period=P1D
druid.coordinator.kill.supervisor.durationToRetain=P4D

# Poll every day to delete rules records > 4 days old
druid.coordinator.kill.rule.on=true
druid.coordinator.kill.rule.period=P1D
druid.coordinator.kill.rule.durationToRetain=P4D

# Poll every day to delete compaction configuration records
druid.coordinator.kill.compaction.on=true
druid.coordinator.kill.compaction.period=P1D

# Poll every day to delete datasource records created by supervisors > 4 days old
druid.coordinator.kill.datasource.on=true
druid.coordinator.kill.datasource.period=P1D
druid.coordinator.kill.datasource.durationToRetain=P4D
...
```

## Learn more
See the following topics for more information:
- [Metadata management](../configuration/index.md#metadata-management) for metadata store configuration reference.
- [Metadata storage](../design/metadata-storage.md) for an overview of the metadata storage database.

