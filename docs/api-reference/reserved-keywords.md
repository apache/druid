---
id: sql-reserved-keywords
title: SQL reserved keywords
sidebar_label: Reserved keywords
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';


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

This topic lists reserved keywords in Apache Druid which cannot be used unless in queries unless they are quoted.

Apache Druid inherits all of the keywords and reserved keywords from [Apache Calcite](https://calcite.apache.org/docs/) which are [documented here](https://calcite.apache.org/docs/reference.html#keywords). In addition to these the following are keywords unique to Apache Druid, reserved keywords are in bold:

* **CLUSTERED**
* OVERWRITE
* **PARTITIONED**
* EXTERN