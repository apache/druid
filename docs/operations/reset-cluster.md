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


The `reset-cluster` tool can be used to completely wipe out Apache Druid (incubating) cluster state stored on Metadata and Deep storage. This is
intended to be used in dev/test environments where you typically want to reset the cluster before running
the test suite.
`reset-cluster` automatically figures out necessary information from Druid cluster configuration. So the java classpath
used in the command must have all the necessary druid configuration files.

It can be run in one of the following ways.

```
java org.apache.druid.cli.Main tools reset-cluster [--metadataStore] [--segmentFiles] [--taskLogs] [--hadoopWorkingPath]
```

or

```
java org.apache.druid.cli.Main tools reset-cluster --all
```

Usage documentation can be printed by running following command.

```
$ java org.apache.druid.cli.Main help tools reset-cluster

NAME
        druid tools reset-cluster - Cleanup all persisted state from metadata
        and deep storage.

SYNOPSIS
        druid tools reset-cluster [--all] [--hadoopWorkingPath]
                [--metadataStore] [--segmentFiles] [--taskLogs]

OPTIONS
        --all
            delete all state stored in metadata and deep storage

        --hadoopWorkingPath
            delete hadoopWorkingPath

        --metadataStore
            delete all records in metadata storage

        --segmentFiles
            delete all segment files from deep storage

        --taskLogs
            delete all tasklogs
```
