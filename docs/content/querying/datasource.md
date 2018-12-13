---
layout: doc_page
title: "Datasources"
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

# Datasources

A data source is the Druid equivalent of a database table. However, a query can also masquerade as a data source, providing subquery-like functionality. Query data sources are currently supported only by [GroupBy](../querying/groupbyquery.html) queries.

### Table Data Source
The table data source is the most common type. It's represented by a string, or by the full structure:

```json
{
	"type": "table",
	"name": "<string_value>"
}
```

### Union Data Source

This data source unions two or more table data sources.

```json
{
       "type": "union",
       "dataSources": ["<string_value1>", "<string_value2>", "<string_value3>", ... ]
}
```

Note that the data sources being unioned should have the same schema.
Union Queries should be always sent to the broker/router node and are *NOT* supported directly by the historical nodes. 

### Query Data Source

This is used for nested groupBys and is only currently supported for groupBys.

```json
{
	"type": "query",
	"query": {
		"type": "groupBy",
		...
	}
}
```
