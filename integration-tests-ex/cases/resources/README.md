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

# Test Data

This directory contains resources, such as test data, mounted into
Druid indexer container at `/resources`.
That name is chosen for backward compatibility with the previous IT
version that mounted `/src/test/resources` to that mount point.
See the `indexer` node in `docker-compose.yaml` for `BatchIndex`
for the actual volume mount.

The many indexer specs expect data to be at
`/resources/data/batch_index/json/spec.json`, where `spec` is the
name of some ingest spec. The structure here was chosen to allow
the existing specs to be reused without change.

Put data for the Indexer in this folder. Put files to be used by
tests in `/src/test/resources`. That way, we only mount into the container
the data which the server needs. Resources which tests need, such as
queries, specs and other resources, are on the class path of the test,
but are not visible to the server.

Paths within this folder are the same as the former
`/src/test/resources` folder so that the many indexer specs don't
have to change.
