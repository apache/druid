---
id: json-querying-api
title: JSON querying API
sidebar_label: JSON querying
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

This document describes the API endpoints to submit JSON-based [native queries](../querying/querying.md) to Apache Druid.

## Queries

`POST /druid/v2/`

The endpoint for submitting queries. Accepts an option `?pretty` that pretty prints the results.

`POST /druid/v2/candidates/`

Returns segment information lists including server locations for the given query.