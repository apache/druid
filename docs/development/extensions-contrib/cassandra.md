---
id: cassandra
title: "Apache Cassandra"
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


To use this Apache Druid extension, [include](../../development/extensions.md#loading-extensions) `druid-cassandra-storage` in the extensions load list.

[Apache Cassandra](http://www.datastax.com/what-we-offer/products-services/datastax-enterprise/apache-cassandra) can also
be leveraged for deep storage.  This requires some additional Druid configuration as well as setting up the necessary
schema within a Cassandra keystore.
