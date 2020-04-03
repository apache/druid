---
id: queryoverview
title: "Druid querying overview"
sidebar_label: "Overview"
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

<!--
  The format of the tables that describe the functions and operators
  should not be changed without updating the script create-sql-function-doc
  in web-console/script/create-sql-function-doc, because the script detects
  patterns in this markdown file and parse it to TypeScript file for web console
-->


Apache Druid supports two query languages: Druid SQL and [native queries](querying.md), which
SQL queries are planned into, and which end users can also issue directly. 

Druid SQL is a built-in SQL layer and an alternative to Druid's native JSON-based query language, and is powered by a
parser and planner based on [Apache Calcite](https://calcite.apache.org/). Druid SQL translates SQL into native Druid
queries on the query Broker (the first process you query), which are then passed down to data processes as native Druid
queries. Other than the (slight) overhead of translating SQL on the Broker, there isn't an additional performance
penalty versus native queries.

If you only perform investigative, ad hoc querying in the UI or from the DSQL 
command line, you probably don't need to know that much about native queries. However, when working with 
complex queries where troubleshooting and query performance becomes a factor, it can pay to know something about
the underlying execution layer. 

By viewing the execution plan for any query in the Druid console, you can see 
the native query that composes the Druid SQL query. 

Here is how this documentation is organized: 

- For basics about Druid SQL and its syntax, see ... 
- For a deeper look at how Druid SQL maps to native query, see ...
- For information on common elements shared between Druid SQL and native queries, see: 
   - Query functions
   - Query datasources 
- For a look under the covers at important concepts and behaviors relevant to native querying, see ....



