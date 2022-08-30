---
id: security
title: Security for the multi-stage query architecture
sidebar_label: Security
---

> SQL-based ingestion using the multi-stage query task engine is our recommended solution starting in Druid 24.0. Alternative ingestion solutions, such as native batch and Hadoop-based ingestion systems, will still be supported. We recommend you read all [known issues](./msq-known-issues.md) and test the feature in a development environment before rolling it out in production. Using the multi-stage query task engine with `SELECT` statements that do not write to a datasource is experimental.

All authenticated users can use the multi-stage query task engine (MSQ task engine) through the UI and API if the extension is loaded. However, without additional permissions, users are not able to issue queries that read or write Druid datasources or external data. The permission you need depends on what you are trying to do.

The permission required to submit a query depends on the type of query:

  - SELECT from a Druid datasource requires the READ DATASOURCE permission on that
  datasource.
  - INSERT or REPLACE into a Druid datasource requires the WRITE DATASOURCE permission on that
  datasource.
  - EXTERN references to external data require READ permission on the resource name "EXTERNAL" of the resource type "EXTERNAL". Users without the correct permission encounter a 403 error when trying to run queries that include EXTERN.

Query tasks that you submit to the MSQ task engine are Overlord tasks, so they follow the Overlord's (indexer) model. This means that users with access to the Overlord API can perform some actions even if they didn't submit the query. The actions include retrieving the status or canceling a query. For more information about the Overlord API and the task API, see [APIs for SQL-based ingestion](./msq-api.md).

To interact with a query through the Overlord API, you need the following permissions:

- INSERT or REPLACE queries: You must have READ DATASOURCE permission on the output datasource.
- SELECT queries: You must have read permissions on the `__query_select` datasource, which is a stub datasource that gets created.
