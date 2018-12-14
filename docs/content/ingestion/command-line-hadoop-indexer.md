---
layout: doc_page
title: "Command Line Hadoop Indexer"
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

# Command Line Hadoop Indexer

To run:

```
java -Xmx256m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -classpath lib/*:<hadoop_config_dir> org.apache.druid.cli.Main index hadoop <spec_file>
```

## Options

- "--coordinate" - provide a version of Hadoop to use. This property will override the default Hadoop coordinates. Once specified, Druid will look for those Hadoop dependencies from the location specified by `druid.extensions.hadoopDependenciesDir`.
- "--no-default-hadoop" - don't pull down the default hadoop version

## Spec file

The spec file needs to contain a JSON object where the contents are the same as the "spec" field in the Hadoop index task. See [Hadoop Batch Ingestion](../ingestion/hadoop.html) for details on the spec format. 

In addition, a `metadataUpdateSpec` and `segmentOutputPath` field needs to be added to the ioConfig:

```
      "ioConfig" : {
        ...
        "metadataUpdateSpec" : {
          "type":"mysql",
          "connectURI" : "jdbc:mysql://localhost:3306/druid",
          "password" : "diurd",
          "segmentTable" : "druid_segments",
          "user" : "druid"
        },
        "segmentOutputPath" : "/MyDirectory/data/index/output"
      },
```    

and a `workingPath` field needs to be added to the tuningConfig:

```
  "tuningConfig" : {
   ...
    "workingPath": "/tmp",
    ...
  }
```    

#### Metadata Update Job Spec

This is a specification of the properties that tell the job how to update metadata such that the Druid cluster will see the output segments and load them.

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|type|String|"metadata" is the only value available.|yes|
|connectURI|String|A valid JDBC url to metadata storage.|yes|
|user|String|Username for db.|yes|
|password|String|password for db.|yes|
|segmentTable|String|Table to use in DB.|yes|

These properties should parrot what you have configured for your [Coordinator](../design/coordinator.html).

#### segmentOutputPath Config

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|segmentOutputPath|String|the path to dump segments into.|yes|

#### workingPath Config

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|workingPath|String|the working path to use for intermediate results (results between Hadoop jobs).|no (default == '/tmp/druid-indexing')|
 
Please note that the command line Hadoop indexer doesn't have the locking capabilities of the indexing service, so if you choose to use it, 
you have to take caution to not override segments created by real-time processing (if you that a real-time pipeline set up).
