---
layout: doc_page
title: "Druid Plumbers"
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

# Druid Plumbers

The plumber handles generated segments both while they are being generated and when they are "done". This is also technically a pluggable interface and there are multiple implementations. However, plumbers handle numerous complex details, and therefore an advanced understanding of Druid is recommended before implementing your own.

Available Plumbers
------------------

#### YeOldePlumber

This plumber creates single historical segments.

#### RealtimePlumber

This plumber creates real-time/mutable segments.
