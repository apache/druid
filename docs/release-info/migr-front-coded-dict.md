---
id: migr-front-coded-dict
title: "Migration guide: front-coded dictionaries"
sidebar_label: Front-coded dictionaries
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
Front coding is an [experimental feature](../development/experimental.md) introduced in Druid 25.0.0.
:::

Apache Druid encodes string columns into dictionaries for better compression.
Front coding is an incremental encoding strategy that lets you store STRING and [COMPLEX&lt;json&gt;](../querying/nested-columns.md) columns in Druid with minimal performance impact.
Front-coded dictionaries reduce storage and improve performance by optimizing for strings where the front part looks similar.
For example, if you are tracking website visits, most URLs start with `https://domain.xyz/`, and front coding is able to exploit this pattern for more optimal compression when storing such datasets.
Druid performs the optimization automatically, which means that the performance of string columns is generally not affected when they don't match the front-coded pattern.
Consequently, you can enable this feature universally without having to know the underlying data shapes of the columns.

You can use front coding with all types of ingestion.

## Enable front coding

To enable front coding, set `indexSpec.stringDictionaryEncoding.type` to `frontCoded` in the `tuningConfig` object of your [ingestion spec](../ingestion/ingestion-spec.md).

You can specify the following optional properties:

* `bucketSize`: Number of values to place in a bucket to perform delta encoding. Setting this property instructs indexing tasks to write segments using compressed dictionaries of the specified bucket size. You can set it to any power of 2 less than or equal to 128. `bucketSize` defaults to 4.
* `formatVersion`: Specifies which front coding version to use. Options are 0 and 1 (supported for Druid versions 26.0.0 and higher). `formatVersion` defaults to 0.

For example:

```json
"tuningConfig": {
  "indexSpec": {
    "stringDictionaryEncoding": {
      "type":"frontCoded",
      "bucketSize": 4,
      "formatVersion": 0
    }
  }
}
```

For SQL based ingestion, you can add the `indexSpec` to your query context.
In the Web Console, select *Edit context* from the context from the *Engine:* menu and enter the `indexSpec`. For example:

```json
{
...
"indexSpec": {
  "stringDictionaryEncoding": {
  "type": "frontCoded",
  "bucketSize": 4,
  "formatVersion": 1
  }
}
}
```

For API calls to the SQL-based ingestion API, include the `indexSpec` in the context in the request payload. For example:

```json
{
"query": ...
"context": {
  "maxNumTasks": 3
  "indexSpec": {
  "stringDictionaryEncoding": {
    "type": "frontCoded",
    "bucketSize": 4,
    "formatVersion": 1}
    }
  }
}
```

## Upgrade from Druid 25.0.0

Druid 26.0.0 introduced a new version of the front-coded dictionary, version 1, offering typically faster read speeds and smaller storage sizes.
When upgrading to versions Druid 26.0.0 and higher, Druid continues to default front coding settings to version 0.
This default enables seamless downgrades to Druid 25.0.0.

To use the newer version, set the `formatVersion` property to 1:

```
"tuningConfig": {
  "indexSpec": {
    "stringDictionaryEncoding": {
      "type":"frontCoded",
      "bucketSize": 4,
      "formatVersion": 1
    }
  }
}
```

## Downgrade to Druid 25.0.0

After upgrading to version 1, you can no longer downgrade to Druid 25.0.0 seamlessly.
To downgrade to Druid 25.0.0, re-ingest your data with the `stringDictionaryEncoding.formatVersion` property set to 0.

## Downgrade to a version preceding Druid 25.0.0

Druid versions preceding 25.0.0 can't read segments with front-coded dictionaries. To downgrade to an older version, you must either delete the segments containing front-coded dictionaries or re-ingest them with `stringDictionaryEncoding.type` set to `utf8`.
