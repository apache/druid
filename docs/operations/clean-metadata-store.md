---
id: clean-metadata-store
title: "Automated cleanup for metadata records"
sidebar_label: Automated metadata cleanup
description: "Defines a strategy to maintain Druid metadata store performance by automatically removing leftover records for deleted entities: datasources, supervisors, rulles, compaction configuration, audit records, etc. Most applicable to databases with 'high-churn' datasources."
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

If you have a high datasource churn rate, meaning you frequently create and delete many short-lived datasources or other releated entities like compaction configuration or rules, the leftover records can start to fill your metadata store and cause performance issues.

To maintain metadata store performance in this case, you can configure Apache Druid to automatically remove records associated with deleted datasources from the metadata store.

## Automated cleanup strategies
There are several cases when you should consider automated cleanup of the metadata related to deleted datasources:
- Proactively, if you know you have many high-churn datasources. For example you have scripts that create and delete supervisors regularly.
- If you have issues with the hard disk for your metadata database filling up.
- If you run into performance issues with the metadata database. For example API calls are very slow or fail to execute.

Do not use the metadata store automated cleanup features if you have requirements to retain metadata records. For example, you have compliance requirements to keep audit records. In these cases, you should come up with an alternate method to preserve the audit metadata while freeing up your active metadata store.

## Configure automated metadata cleanup

You can configure cleanup on a per-entity basis with the following constraints:
- You have to configure a kill task for Segment records before you can configure automated cleanup for rules or compaction configuration.
- You have to configure the scheduler for the cleanup jobs to run at a the same frequency or more frequently than your most frequent cleanup job. For examlple, if your most frequent cleanup job is every hour, set it to one hour or less: `druid.coordinator.period.metadataStoreManagementPeriod=P1H`.

For details on configuration properties, see [Metadata management](../configuration/index.md#metadata-management).

### Segment records and segements in deep storage(kill task)
Segment records and segements in deep storage become eligible for deletion:

- When they meet the elegibility requirement of kill task datasource configuration according to `killDataSourceWhitelist` and `killAllDataSources` set in the Coordinator dynamic configuration. See [Dynamic configuration](../configuration/index.md#ynamic-configuration).
- `durationToRetain` time has passed since their creation.

Kill tasks use the following configuration:
- `druid.coordinator.kill.on`: When `True`, enables the Coordinator to submit kill task for unused Segments which deletes them completely from metadata store and from deep storage. Only applies dataSources according to allowed datasources or all datasources.
- `druid.coordinator.kill.period`: Defines the frequency in [ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601#Durations) for the cleanup job to check for and delete eligible Segments. Must be greater than `druid.coordinator.period.indexingPeriod`. 
- `druid.coordinator.kill.durationToRetain`: Defines the retention period in [ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601#Durations) after creation that Segments become eligible for deletion.
- `druid.coordinator.kill.maxSegments`: Defines the maximum number of Segments to delete per kill task.
>The kill task is the only configuration in this topic that affects actual data in deep storage and not simply metadata.

### Audit records
All audit records become eligible for deletion when the `durationToRetain` time has passed since their creation. Audit cleanup uses the following configuration:
 - `druid.coordinator.kill.druid_audit.on`: When `True`, enables cleanup for audit records.
 - `druid.coordinator.kill.druid_audit.period`: Defines the frequency in [ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601#Durations) for the cleanup job to check for and delete eligible audit records.
 - `druid.coordinator.kill.druid_audit.durationToRetain`: Defines the retention period in [ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601#Durations) after creation that audit recores become eligible for deletion.

### Supervisor records
Supervisor records become eligible for deletion when the supervisor is terminated and the `durationToRetain` time has passed since their creation. Supervisor cleanup uses the following configuration:
 - `druid.coordinator.kill.supervisor.on`: When `True`, enables cleanup for supervisor records.
 - `druid.coordinator.kill.supervisor.period`: Defines the frequency in [ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601#Durations) for the cleanup job to check for and delete eligible audit records.
 - `druid.coordinator.kill.supervisor.durationToRetain`: Defines the retention period in [ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601#Durations) after creation that audit recores become eligible for deletion.

### Rules
Rule records become eligible for deletion when all Segments for the datasource have been killed by the kill task and the `durationToRetain` time has passed since their creation. Rule cleanup uses the following configuration:
 - `druid.coordinator.kill.druid_rules.on`: When `True`, enables cleanup for a particular metadata table.
 - `druid.coordinator.kill.druid_rules.period`: Defines the frequency in [ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601#Durations) for the cleanup job to check for and delete eligible audit records.
 - `druid.coordinator.kill.druid_rules.durationToRetain`: Defines the retention period in [ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601#Durations) after creation that rules records become eligible for deletion.

 ### Compaction configuration
Compaction configuration records in the records in the `druid_config` table become eligible for deletion all Segments for the datasource have been killed by the kill task.Compaction cleanup uses the following configuration:
 - `druid.coordinator.kill.compaction.on`: When `True`, enables cleanup for compaction records.
 - `druid.coordinator.kill.compaction.period`: Defines the frequency in [ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601#Durations) for the cleanup job to check for and delete eligible compaction configuration records.

>If you already have an extremely large compaction configuration, you may not be able to delete compaction configuration due to size limits with the audit log. In this case you can set `druid.audit.manager.maxPayloadSizeBytes` and `druid.audit.manager.skipNullField` to avoid the auditing issue. See [Audit logging](../configuration/index.md#audit-logging).

### Datasources created by supervisors
Datasource records created by supervisors become eligible for deletion when the supervisor is terminated or does not exist in the `druid_supervisors` table and the `durationToRetain` time has passed since their creation. Datasource cleanup uses the following configuration:
 - `druid.coordinator.kill.datasource.on`: When `True`, enables cleanup datasources created by supervisors.
 - `druid.coordinator.kill.datasource.period`: Defines the frequency in [ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601#Durations) for the cleanup job to check for and delete eligible datasource records.
 - `druid.coordinator.kill.druid_rules.durationToRetain`: Defines the retention period in [ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601#Durations) after creation that datasource records become eligible for deletion.

>The table names listed here assume the default value for of `druid` for table name prefix. This is configurable according to `druid.metadata.storage.tables.base`. See [Metadata storage](../configuration/index.md#metadata-storage).

## Automated metadata cleanup example configuration
Consider a scenario where you have scripts to create and delete hundreds of datasources and related entities a day. You do not want to fill your metadata store with leftover records. The datasources and related entities tend to persist for only one or two days. Therefore, you want to run a cleanup job that identifies and removes leftover records that are at least four days old. The exception is for audit logs, which you need to retain for 30 days:

```
...
# Schedule the metadata management task for every hour:
druid.coordinator.period.metadataStoreManagementPeriod=P1H

# Set a kill task to poll every day to delete Segment records
# and Segments in deep storage > 4 days old.
# Required also for automated cleanup of rules
# and compaction configuration.

druid.coordinator.kill.on=True
druid.coordinator.kill.period=P1D 
druid.coordinator.kill.durationToRetain=P4D
druid.coordinator.kill.maxSegments=1000

# Poll every day to delete audit records > 30 days old
druid.coordinator.kill.druid_audit.on=True
druid.coordinator.kill.druid_audit.period=P1D
druid.coordinator.kill.druid_audit.durationToRetain=P30D

# Poll every day to delete supervisor records > 4 days old
druid.coordinator.kill.druid_supervisors.on=True
druid.coordinator.kill.druid_supervisors.period=P1D
druid.coordinator.kill.druid_supervisors.durationToRetain=P4D

# Poll every day to delete rules records > 4 days old
druid.coordinator.kill.druid_rules.on=True
druid.coordinator.kill.druid_rules.period=P1D
druid.coordinator.kill.druid_rules.durationToRetain=P4D

# Poll every day to delete compaction configuration records > 4 days old
druid.coordinator.kill.compaction.on=True
druid.coordinator.kill.compaction.period=P1D

# Poll every day to delete datasource records created by supervisors > 4 days old
druid.coordinator.kill.druid_datasources.on=True
druid.coordinator.kill.druid_datasources.period=P1D
druid.coordinator.kill.druid_datasources.durationToRetain=P4D
...
```

## Learn more
See the following topics for more information:
- [Metadata management](../configuration/index.md#metadata-management) for metadata store configuration reference.
- [Metadata storage](../dependencies/metadata-storage.md) for an overview of the metadata storage database.
