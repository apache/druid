---
layout: doc_page
title: "MiddleManager Node"
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

# MiddleManager Node

### Configuration

For Middlemanager Node Configuration, see [Indexing Service Configuration](../configuration/index.html#middlemanager-and-peons).

### HTTP Endpoints

For a list of API endpoints supported by the MiddleManager, please see the [API reference](../operations/api-reference.html#middlemanager).

### Overview

The middle manager node is a worker node that executes submitted tasks. Middle Managers forward tasks to peons that run in separate JVMs.
The reason we have separate JVMs for tasks is for resource and log isolation. Each [Peon](../design/peons.html) is capable of running only one task at a time, however, a middle manager may have multiple peons.

### Running

```
org.apache.druid.cli.Main server middleManager
```
