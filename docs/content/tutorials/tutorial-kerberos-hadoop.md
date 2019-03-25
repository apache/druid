---
layout: doc_page
title: "Configuring Druid to use a Kerberized Hadoop as Deep Storage"
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
  
# Configuring Druid to use a Kerberized Hadoop as Deep Storage

## Hadoop Setup

Following are the configurations files required to be copied over to Druid conf folders:

1. For HDFS as a deep storage, hdfs-site.xml, core-site.xml
2. For ingestion, mapred-site.xml, yarn-site.xml

### HDFS Folders and permissions

1. Choose any folder name for the druid deep storage, for example 'druid'
2. Create the folder in hdfs under the required parent folder. For example,
`hdfs dfs -mkdir /druid`
OR
`hdfs dfs -mkdir /apps/druid`

3. Give druid processes appropriate permissions for the druid processes to access this folder. This would ensure that druid is able to create necessary folders like data and indexing_log in HDFS.
For example, if druid processes run as user 'root', then

    `hdfs dfs -chown root:root /apps/druid`

    OR

    `hdfs dfs -chmod 777 /apps/druid`

Druid creates necessary sub-folders to store data and index under this newly created folder.

## Druid Setup

Edit common.runtime.properties at conf/druid/_common/common.runtime.properties to include the HDFS properties. Folders used for the location are same as the ones used for example above.

### common_runtime_properties

```properties
# Deep storage
#
# For HDFS:
 druid.storage.type=hdfs
 druid.storage.storageDirectory=/druid/segments
# OR
# druid.storage.storageDirectory=/apps/druid/segments

#
# Indexing service logs
#

# For HDFS:
druid.indexer.logs.type=hdfs
druid.indexer.logs.directory=/druid/indexing-logs
# OR
# druid.storage.storageDirectory=/apps/druid/indexing-logs
```

Note: Comment out Local storage and S3 Storage parameters in the file

Also include hdfs-storage core extension to conf/druid/_common/common.runtime.

```properties
#
# Extensions
#

druid.extensions.directory=dist/druid/extensions
druid.extensions.hadoopDependenciesDir=dist/druid/hadoop-dependencies
druid.extensions.loadList=["druid-parser-route", "mysql-metadata-storage", "druid-hdfs-storage", "druid-kerberos"]
```
### Hadoop Jars

Ensure that Druid has necessary jars to support the Hadoop version.

Find the hadoop version using command, `hadoop version` 

In case there are other softwares used with hadoop, like WanDisco, ensure that 
1. the necessary libraries are available
2. add the requisite extensions to `druid.extensions.loadlist` in `conf/druid/_common/common.runtime.properties`

### Kerberos setup

Create a headless keytab which would have access to the druid data and index.

Edit conf/druid/_common/common.runtime.properties and add the following properties:

```properties
druid.hadoop.security.kerberos.principal
druid.hadoop.security.kerberos.keytab
```

For example

```properties
druid.hadoop.security.kerberos.principal=hdfs-test@EXAMPLE.IO
druid.hadoop.security.kerberos.keytab=/etc/security/keytabs/hdfs.headless.keytab
```

### Restart Druid Services

With the above changes, restart Druid. This would ensure that Druid works with Kerberized Hadoop
