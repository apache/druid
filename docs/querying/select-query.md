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


> The native Druid Select query has been removed in favor of the [Scan query](../querying/scan-query.md), which should
> be used instead.
> In situations involving larger numbers of segments, the Select query had very high memory and performance overhead.
> The Scan query does not have this issue.
> The major difference between the two is that the Scan query does not support pagination.
> However, the Scan query type is able to return a virtually unlimited number of results even without pagination, 
> making it unnecessary in many cases.

