---
id: joins
title: "Joins"
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

Druid has two features related to joining of data:

1. [Join](datasource.md#join) operators. These are available using a [join datasource](datasource.md#join) in native
queries, or using the [JOIN operator](sql.md#query-syntax) in Druid SQL. Refer to the
[join datasource](datasource.md#join) documentation for information about how joins work in Druid.
2. [Query-time lookups](lookups.md), simple key-to-value mappings. These are preloaded on all servers that are involved
in queries and can be accessed with or without an explicit join operator. Refer to the [lookups](lookups.md)
documentation for more details.

Whenever possible, for best performance it is good to avoid joins at query time. Often this can be accomplished by
joining data before it is loaded into Druid. However, there are situations where joins or lookups are the best solution
available despite the performance overhead, including:

- The fact-to-dimension (star and snowflake schema) case: you need to change dimension values after initial ingestion,
and aren't able to reingest to do this. In this case, you can use lookups for your dimension tables.
- Your workload requires joins or filters on subqueries.
