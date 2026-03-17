---
id: hdfs
title: "HDFS"
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

To use this Apache Druid extension, [include](../../configuration/extensions.md#loading-extensions) `druid-hdfs-storage` in the extensions load list and run druid processes with `GOOGLE_APPLICATION_CREDENTIALS=/path/to/service_account_keyfile` in the environment.

## Deep Storage

### Configuration for HDFS

|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.storage.type`|hdfs||Must be set.|
|`druid.storage.storageDirectory`||Directory for storing segments.|Must be set.|
|`druid.hadoop.security.kerberos.principal`|`druid@EXAMPLE.COM`| Principal user name |empty|
|`druid.hadoop.security.kerberos.keytab`|`/etc/security/keytabs/druid.headlessUser.keytab`|Path to keytab file|empty|

Besides the above settings, you also need to include all Hadoop configuration files (such as `core-site.xml`, `hdfs-site.xml`)
in the Druid classpath. One way to do this is copying all those files under `${DRUID_HOME}/conf/_common`.

If you want to eagerly authenticate against a secured hadoop/hdfs cluster you must set `druid.hadoop.security.kerberos.principal` and `druid.hadoop.security.kerberos.keytab`, this is an alternative to the cron job method that runs `kinit` command periodically.
