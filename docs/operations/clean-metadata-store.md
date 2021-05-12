---
id: clean-metadata-store
title: "Automated cleanup for metadata records related to deleted datasources"
sidebar_label: Automated metadata store cleanup
description: "Defines a strategy to maintain Druid metadata store performance by automatically removing leftover records for deleted datasources. Most applicable to databases with 'high-churn' datasources."
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
When you delete a datasource from Apache Druid, some records related to the datasource may remain in the metadata store including:

- audit records
- supervisor records
- rule records
- compaction configuration records
- datasource records created by supervisors

If you have a high datasource churn rate, meaning you frequently create and delete many short-lived datasources, the leftover records can start to fill your metadata store and cause performance issues. To maintain metadata store performance in this case, you can configure Apache Druid to automatically remove records associated with deleted datasources from the metadata store.

## Automated cleanup strategies
There are several cases when you should consider automated cleanup of the metadata related to deleted datasources:
- Proactively, if you know you have many high-churn datasources. For example you have scripts that create and delete supervisors regularly.
- If you have issues with the hard disk for your metadata database filling up.
- If you run into performance issues with the metadata database. For example API calls are very slow or fail to execute.

Do not use the metadata store automated cleanup features if you have requirements to retain metadata records. For example, you have compliance requirements to keep audit records. In these cases, you should come up with an alternate method to preserve the audit metadata while freeing up your active metadata store.

## Configure automated metadata cleanup
Automated cleanup only removes records for deleted datasources. You can configure cleanup on a per-table basis as follows:
 - `druid.coordinator.kill.*.on` enables cleanup for a particular metadata table.
 - `druid.coordinator.kill.*.period` defines the frequency in [ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601#Durations) for the cleanup job to check for and delete eligible records.
 - `druid.coordinator.kill.*.durationToRetain` defines the retention period in [ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601#Durations) after creation that the entity becomes eligible for deletion. For example to retain records for 30 days from their creation date: `P30D`.

 For full configuration details, see [Metadata management](../configuration/index.md#metadata-management).

The following are eligible for automatic cleanup:
- `druid_audit`: Audit records for deleted datasources become eligible for deletion when the `durationToRetain` time has passed since their creation.
- `druid_supervisors`: Supervisor records become eligible for deletion when:

    - the supervisor is terminated.
    - the `durationToRetain` time has passed since their creation.
- `druid_rules`: Rule records become eligible for deletion when:

    - no segments exist for the datasource.
    - the `durationToRetain` time has passed since their creation.
- `compaction`: Compaction configuration records in the records in the `druid_config` table become eligible for deletion when no segments exist for the datasource.
     >If you already have an extremely large compaction configuration, you may not be able to delete compaction configuration due to size limits with the audit log. In this case you can set `druid.audit.manager.maxPayloadSizeBytes` and `druid.audit.manager.skipNullField` to avoid the auditing issue. See [Audit logging](../configuration/index.md#audit-logging).
- `druid_datasources`: Datasource records become eligible for deletion when:

    - the supervisor is terminated or does not exist in the `druid_supervisors` table.
    - the `durationToRetain` time has passed since their creation.

## Automated metadata cleanup example configuration
Consider a scenario where you have scripts to create and delete supervisors for hundreds of datasources a day. You do not want to fill your metadata store with records for deleted datasources. Datasources tend to persist for only one or two days. Therefore, you want to run a cleanup job that identifies and removes leftover records that are at least four days old. The exception is for audit logs, which you need to retain for 30 days:

```
...
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
druid.coordinator.kill.compaction.durationToRetain=P4D

# Poll every day to delete datasource records > 4 days old
druid.coordinator.kill.druid_datasources.on=True
druid.coordinator.kill.druid_datasources.period=P1D
druid.coordinator.kill.druid_datasources.durationToRetain=P4D
...
```

## Learn more
See the following topics for more information:
- [Metadata management](../configuration/index.md#metadata-management) for metadata store configuration reference.
- [Metadata storage](../dependencies/metadata-storage.md) for an overview of the metadata storage database.
