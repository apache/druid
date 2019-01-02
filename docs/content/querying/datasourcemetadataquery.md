---
layout: doc_page
title: "Data Source Metadata Queries"
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

# Data Source Metadata Queries

Data Source Metadata queries return metadata information for a dataSource.  These queries return information about:

* The timestamp of latest ingested event for the dataSource. This is the ingested event without any consideration of rollup.

The grammar for these queries is:

```json
{
    "queryType" : "dataSourceMetadata",
    "dataSource": "sample_datasource"
}
```

There are 2 main parts to a Data Source Metadata query:

|property|description|required?|
|--------|-----------|---------|
|queryType|This String should always be "dataSourceMetadata"; this is the first thing Druid looks at to figure out how to interpret the query|yes|
|dataSource|A String or Object defining the data source to query, very similar to a table in a relational database. See [DataSource](../querying/datasource.html) for more information.|yes|
|context|See [Context](../querying/query-context.html)|no|

The format of the result is:

```json
[ {
  "timestamp" : "2013-05-09T18:24:00.000Z",
  "result" : {
    "maxIngestedEventTime" : "2013-05-09T18:24:09.007Z"
  }
} ]
```
