---
id: azure
title: "Microsoft Azure"
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

## Azure extension

This extension allows you to do the following:

* [Ingest data](#ingest-data-from-azure) from objects stored in Azure Blob Storage.
* [Write segments](#store-segments-in-azure) to Azure Blob Storage for deep storage.
* [Persist task logs](#persist-task-logs-in-azure) to Azure Blob Storage for long-term storage.

:::info

To use this Apache Druid extension, [include](../../configuration/extensions.md#loading-extensions) `druid-azure-extensions` in the extensions load list.

:::

### Ingest data from Azure

Ingest data using either [MSQ](../../multi-stage-query/index.md) or a native batch [parallel task](../../ingestion/native-batch.md) with an [Azure input source](../../ingestion/input-sources.md#azure-input-source) (`azureStorage`) to read objects directly from Azure Blob Storage.

### Store segments in Azure

:::info

To use Azure for deep storage, set `druid.storage.type=azure`.

:::

#### Configure location

Configure where to store segments using the following properties:

| Property | Description | Default |
|---|---|---|
| `druid.azure.account` | The Azure Storage account name. | Must be set. |
| `druid.azure.container` | The Azure Storage container name. | Must be set. |
| `druid.azure.prefix` | A prefix string that will be prepended to the blob names for the segments published. | "" |
| `druid.azure.maxTries` | Number of tries before canceling an Azure operation. | 3 |
| `druid.azure.protocol` | The protocol to use to connect to the Azure Storage account. Either `http` or `https`. | `https` |
| `druid.azure.storageAccountEndpointSuffix` | The Storage account endpoint to use. Override the default value to connect to [Azure Government](https://learn.microsoft.com/en-us/azure/azure-government/documentation-government-get-started-connect-to-storage#getting-started-with-storage-api) or storage accounts with [Azure DNS zone endpoints](https://learn.microsoft.com/en-us/azure/storage/common/storage-account-overview#azure-dns-zone-endpoints-preview).<br/><br/>Do _not_ include the storage account name prefix in this config value.<br/><br/>Examples: `ABCD1234.blob.storage.azure.net`, `blob.core.usgovcloudapi.net`. | `blob.core.windows.net` |

#### Configure authentication

Authenticate access to Azure Blob Storage using one of the following methods:

* [SAS token](https://learn.microsoft.com/en-us/azure/storage/common/storage-sas-overview)
* [Shared Key](https://learn.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key)
* Default Azure credentials chain ([`DefaultAzureCredential`](https://learn.microsoft.com/en-us/java/api/overview/azure/identity-readme#defaultazurecredential)).

Configure authentication using the following properties:

| Property | Description | Default |
|---|---|---|
| `druid.azure.sharedAccessStorageToken` | The SAS (Shared Storage Access) token. |  |
| `druid.azure.key` | The Shared Key. |  |
| `druid.azure.useAzureCredentialsChain` | If `true`, use `DefaultAzureCredential` for authentication. | `false` |
| `druid.azure.managedIdentityClientId` | To use managed identity authentication in the `DefaultAzureCredential`, set `useAzureCredentialsChain` to `true` and provide the client ID here. |  |

### Persist task logs in Azure

:::info

To persist task logs in Azure Blob Storage, set `druid.indexer.logs.type=azure`.

:::

Druid stores task logs using the storage account and authentication method configured for storing segments. Use the following configuration to set up where to store the task logs:

| Property | Description | Default |
|---|---|---|
| `druid.indexer.logs.container` | The Azure Blob Store container to write logs to. | Must be set. |
| `druid.indexer.logs.prefix` | The path to prepend to logs. | Must be set. |

For general options regarding task retention, see [Log retention policy](../../configuration/index.md#log-retention-policy).
