---
id: orc
title: "ORC Extension"
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

## ORC extension

This Apache Druid extension enables Druid to ingest and understand the Apache ORC data format.

The extension provides the [ORC input format](../../ingestion/data-formats.md#orc) and the [ORC Hadoop parser](../../ingestion/data-formats.md#orc-hadoop-parser)
for [native batch ingestion](../../ingestion/native-batch.md) and [Hadoop batch ingestion](../../ingestion/hadoop.md), respectively.
Please see corresponding docs for details.

To use this extension, make sure to [include](../../development/extensions.md#loading-extensions) `druid-orc-extensions`.

### Migration from 'contrib' extension
This extension, first available in version 0.15.0, replaces the previous 'contrib' extension which was available until
0.14.0-incubating. While this extension can index any data the 'contrib' extension could, the JSON spec for the
ingestion task is *incompatible*, and will need modified to work with the newer 'core' extension.

To migrate to 0.15.0+:

* In `inputSpec` of `ioConfig`, `inputFormat` must be changed from `"org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat"` to
`"org.apache.orc.mapreduce.OrcInputFormat"`
* The 'contrib' extension supported a `typeString` property, which provided the schema of the
ORC file, of which was essentially required to have the types correct, but notably _not_ the column names, which
facilitated column renaming. In the 'core' extension, column renaming can be achieved with
[`flattenSpec`](../../ingestion/index.md#flattenspec). For example, `"typeString":"struct<time:string,name:string>"`
with the actual schema `struct<_col0:string,_col1:string>`, to preserve Druid schema would need replaced with:

```json
"flattenSpec": {
  "fields": [
    {
      "type": "path",
      "name": "time",
      "expr": "$._col0"
    },
    {
      "type": "path",
      "name": "name",
      "expr": "$._col1"
    }
  ]
  ...
}
```

* The 'contrib' extension supported a `mapFieldNameFormat` property, which provided a way to specify a dimension to
 flatten `OrcMap` columns with primitive types. This functionality has also been replaced with
 [`flattenSpec`](../../ingestion/index.md#flattenspec). For example: `"mapFieldNameFormat": "<PARENT>_<CHILD>"`
 for a dimension `nestedData_dim1`, to preserve Druid schema could be replaced with

 ```json
"flattenSpec": {
  "fields": [
    {
      "type": "path",
      "name": "nestedData_dim1",
      "expr": "$.nestedData.dim1"
    }
  ]
  ...
}
```
