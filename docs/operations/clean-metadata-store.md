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
When you delete some entities from Apache Druid, records related to the entity may remain in the metadata store including:

- segments records
- audit records
- supervisor records
- rule records
- compaction configuration records
- datasource records created by supervisors

If you have a high datasource churn rate, meaning you frequently create and delete many short-lived datasources or other related entities like compaction configuration or rules, the leftover records can start to fill your metadata store and cause performance issues.

To maintain metadata store performance in this case, you can configure Apache Druid to automatically remove records associated with deleted entities from the metadata store.

## Automated cleanup strategies
There are several cases when you should consider automated cleanup of the metadata related to deleted datasources:
- If you know you have many high-churn datasources,  for example, you have scripts that create and delete supervisors regularly.
- If you have issues with the hard disk for your metadata database filling up.
- If you run into performance issues with the metadata database. For example, API calls are very slow or fail to execute.

If you have compliance requirements to keep audit records and you enable automated cleanup for audit records, use alternative methods to preserve audit metadata, for example, by periodically exporting audit metadata records to external storage.

## Configure automated metadata cleanup
By default, automatic cleanup for metadata is disabled. See [Metadata storage](../configuration/index.md#metadata-storage) for the default configuration settings after you enable the feature.

You can configure cleanup on a per-entity basis with the following constraints:
- You have to configure a [kill task for segment records](#kill-task) before you can configure automated cleanup for [rules](#rules-records) or [compaction configuration](#compaction-configuration-records).
- You have to configure the scheduler for the cleanup jobs to run at the same frequency or more frequently than your most frequent cleanup job. For example, if your most frequent cleanup job is every hour, set the scheduler metadata store management period to one hour or less: `druid.coordinator.period.metadataStoreManagementPeriod=P1H`.

For details on configuration properties, see [Metadata management](../configuration/index.md#metadata-management).

<a name="kill-task">
### Segment records and segments in deep storage (kill task)
Segment records and segments in deep storage become eligible for deletion:

- When they meet the eligibility requirement of kill task datasource configuration according to `killDataSourceWhitelist` and `killAllDataSources` set in the Coordinator dynamic configuration. See [Dynamic configuration](../configuration/index.md#dynamic-configuration).
- The `durationToRetain` time has passed since their creation.

Kill tasks use the following configuration:
- `druid.coordinator.kill.on`: When `True`, enables the Coordinator to submit kill task for unused segments, which deletes them completely from metadata store and from deep storage. Only applies `dataSources` according to the dynamic configuration: allowed datasources (`killDataSourceWhitelist`) or all datasources (`killAllDataSources`).
- `druid.coordinator.kill.period`: Defines the frequency in [ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601#Durations) for the cleanup job to check for and delete eligible segments. Defaults to `P1D`. Must be greater than `druid.coordinator.period.indexingPeriod`. 
- `druid.coordinator.kill.durationToRetain`: Defines the retention period in [ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601#Durations) after creation that segments become eligible for deletion.
- `druid.coordinator.kill.maxSegments`: Defines the maximum number of segments to delete per kill task.
>The kill task is the only configuration in this topic that affects actual data in deep storage and not simply metadata or logs.

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
Compaction configuration records in the `druid_config` table become eligible for deletion after all segments for the datasource have been killed by the kill task. Automated cleanup for compaction configuration requires a [kill task](#kill-task).

Compaction configuration cleanup uses the following configuration:
 - `druid.coordinator.kill.compaction.on`: When `true`, enables cleanup for compaction  configuration records.
 - `druid.coordinator.kill.compaction.period`: Defines the frequency in [ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601#Durations) for the cleanup job to check for and delete eligible compaction configuration records. Defaults to `P1D`.

>If you already have an extremely large compaction configuration, you may not be able to delete compaction configuration due to size limits with the audit log. In this case you can set `druid.audit.manager.maxPayloadSizeBytes` and `druid.audit.manager.skipNullField` to avoid the auditing issue. See [Audit logging](../configuration/index.md#audit-logging).

### Datasource records created by supervisors
Datasource records created by supervisors become eligible for deletion when the supervisor is terminated or does not exist in the `druid_supervisors` table and the `durationToRetain` time has passed since their creation.

Datasource cleanup uses the following configuration:
 - `druid.coordinator.kill.datasource.on`: When `true`, enables cleanup datasources created by supervisors.
 - `druid.coordinator.kill.datasource.period`: Defines the frequency in [ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601#Durations) for the cleanup job to check for and delete eligible datasource records. Defaults to `P1D`.
 - `druid.coordinator.kill.datasource.durationToRetain`: Defines the retention period in [ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601#Durations) after creation that datasource records become eligible for deletion.

### Indexer task logs
You can configure the Overlord to delete indexer task log metadata and the indexer task logs from local disk or from cloud storage.

Indexer task log cleanup on the Overlord uses the following configuration:
- `druid.indexer.logs.kill.enabled`: When `true`, enables cleanup of task logs.
- `druid.indexer.logs.kill.durationToRetain`: Defines the length of time in milliseconds to retain task logs.
- `druid.indexer.logs.kill.initialDelay`: Defines the length of time in milliseconds after the Overlord starts before it executes its first job to kill task logs.
- `druid.indexer.logs.kill.delay`: The length of time in milliseconds between jobs to kill task logs.

For more detail, see [Task logging](../configuration/index.md#task-logging).

## Automated metadata cleanup example configuration
Consider a scenario where you have scripts to create and delete hundreds of datasources and related entities a day. You do not want to fill your metadata store with leftover records. The datasources and related entities tend to persist for only one or two days. Therefore, you want to run a cleanup job that identifies and removes leftover records that are at least four days old. The exception is for audit logs, which you need to retain for 30 days:

```
...
# Schedule the metadata management store task for every hour:
druid.coordinator.period.metadataStoreManagementPeriod=P1H

# Set a kill task to poll every day to delete Segment records and segments
# in deep storage > 4 days old. When druid.coordinator.kill.on is set to true,
# you must set either killAllDataSources or killDataSourceWhitelist in the dynamic
# configuration. For this example, assume killAllDataSources is set to true.
# Required also for automated cleanup of rules and compaction configuration.

druid.coordinator.kill.on=true
druid.coordinator.kill.period=P1D 
druid.coordinator.kill.durationToRetain=P4D
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
- [Metadata storage](../dependencies/metadata-storage.md) for an overview of the metadata storage database.