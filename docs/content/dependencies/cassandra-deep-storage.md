---
layout: doc_page
title: "Cassandra Deep Storage"
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

# Cassandra Deep Storage

## Introduction

Apache Druid (incubating) can use Apache Cassandra as a deep storage mechanism. Segments and their metadata are stored in Cassandra in two tables:
`index_storage` and `descriptor_storage`.  Underneath the hood, the Cassandra integration leverages Astyanax.  The
index storage table is a [Chunked Object](https://github.com/Netflix/astyanax/wiki/Chunked-Object-Store) repository. It contains
compressed segments for distribution to Historical processes.  Since segments can be large, the Chunked Object storage allows the integration to multi-thread
the write to Cassandra, and spreads the data across all the processes in a cluster.  The descriptor storage table is a normal C* table that
stores the segment metadatak.

## Schema
Below are the create statements for each:

```sql
CREATE TABLE index_storage(key text,
                           chunk text,
                           value blob,
                           PRIMARY KEY (key, chunk)) WITH COMPACT STORAGE;

CREATE TABLE descriptor_storage(key varchar,
                                lastModified timestamp,
                                descriptor varchar,
                                PRIMARY KEY (key)) WITH COMPACT STORAGE;
```

## Getting Started
First create the schema above. I use a new keyspace called `druid` for this purpose, which can be created using the
[Cassandra CQL `CREATE KEYSPACE`](http://www.datastax.com/documentation/cql/3.1/cql/cql_reference/create_keyspace_r.html) command.

Then, add the following to your Historical and realtime runtime properties files to enable a Cassandra backend.

```properties
druid.extensions.loadList=["druid-cassandra-storage"]
druid.storage.type=c*
druid.storage.host=localhost:9160
druid.storage.keyspace=druid
```
