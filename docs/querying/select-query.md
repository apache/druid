---
id: select-query
title: "Select queries"
sidebar_label: "Select"
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

 
Older versions of Apache Druid included a Select query type. Since Druid 0.17.0, it has been removed and replaced by the [Scan query](../querying/scan-query.md), which offers improved memory usage and performance. This solves issues that users had with Select queries causing Druid to run out of memory or slow down.

The Scan query has a different syntax, but supports many of the features of the Select query, including time ordering and limiting. Scan does not include the Select query's pagination feature; however, in many cases pagination is unnecessary with Scan due to its ability to return a virtually unlimited number of results in one call.
