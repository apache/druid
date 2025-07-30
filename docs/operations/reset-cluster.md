---
id: reset-cluster
title: "reset-cluster tool"
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


In older versions of Apache Druid, `reset-cluster` was a tool that could wipe out Apache Druid cluster state stored in
metadata and deep storage, intended primarily for use in dev and test environments. However, this tool was prone to
becoming out of sync with the codebase since it was not used in practice during dev and testing, and could not cover
all cleanup cases when extensions were involved. It was removed in Druid 35.0.0.
