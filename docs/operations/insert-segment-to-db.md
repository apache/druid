---
id: insert-segment-to-db
title: "insert-segment-to-db tool"
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


In older versions of Apache Druid, `insert-segment-to-db` was a tool that could scan deep storage and
insert data from there into Druid metadata storage. It was intended to be used to update the segment table in the
metadata storage after manually migrating segments from one place to another, or even to recover lost metadata storage
by telling it where the segments are stored.

In Druid 0.14.x and earlier, Druid wrote segment metadata to two places: the metadata store's `druid_segments` table, and
`descriptor.json` files in deep storage. This practice was stopped in Druid 0.15.0 as part of
[consolidated metadata management](https://github.com/apache/druid/issues/6849), for the following reasons:

1. If any segments are manually dropped or re-enabled by cluster operators, this information is not reflected in
deep storage. Restoring metadata from deep storage would undo any such drops or re-enables.
2. Ingestion methods that allocate segments optimistically (such as native Kafka or Kinesis stream ingestion, or native
batch ingestion in 'append' mode) can write segments to deep storage that are not meant to actually be used by the
Druid cluster. There is no way, while purely looking at deep storage, to differentiate the segments that made it into
the metadata store originally (and therefore _should_ be used) from the segments that did not (and therefore
_should not_ be used).
3. Nothing in Druid other than the `insert-segment-to-db` tool read the `descriptor.json` files.

After this change, Druid stopped writing `descriptor.json` files to deep storage, and now only writes segment metadata
to the metadata store. This meant the `insert-segment-to-db` tool is no longer useful, so it was removed in Druid 0.15.0.

It is highly recommended that you take regular backups of your metadata store, since it is difficult to recover Druid
clusters properly without it.
