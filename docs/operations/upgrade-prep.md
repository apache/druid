---
id: upgrade-prep
title: "Upgrade Prep"
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
  
## Upgrade to `0.24+` from `0.23` and earlier

### Altering segments table

**If you have set `druid.metadata.storage.connector.createTables` to `true` (which is the default), and your metadata connect user has DDL privileges, you can disregard this section.**

**The coordinator and overlord services will fail if you do not execute this change prior to the upgrade**

A new column, `used_flag_last_updated`, is needed in the segments table to support new
segment killing functionality. You can manually alter the table, or you can use
a CLI tool to perform the update.

#### CLI tool

Druid provides a `metadata-update` tool for updating Druid's metadata tables.

In the example commands below:

- `lib` is the Druid lib directory
- `extensions` is the Druid extensions directory
- `base` corresponds to the value of `druid.metadata.storage.tables.base` in the configuration, `druid` by default.
- The `--connectURI` parameter corresponds to the value of `druid.metadata.storage.connector.connectURI`.
- The `--user` parameter corresponds to the value of `druid.metadata.storage.connector.user`.
- The `--password` parameter corresponds to the value of `druid.metadata.storage.connector.password`.
- The `--action` parameter corresponds to the update action you are executing. In this case it is: `add-last-used-to-segments`

##### MySQL

```bash
cd ${DRUID_ROOT}
java -classpath "lib/*" -Dlog4j.configurationFile=conf/druid/cluster/_common/log4j2.xml -Ddruid.extensions.directory="extensions" -Ddruid.extensions.loadList=[\"mysql-metadata-storage\"] -Ddruid.metadata.storage.type=mysql org.apache.druid.cli.Main tools metadata-update --connectURI="<mysql-uri>" --user <user> --password <pass> --base druid --action add-used-flag-last-updated-to-segments
```

##### PostgreSQL

```bash
cd ${DRUID_ROOT}
java -classpath "lib/*" -Dlog4j.configurationFile=conf/druid/cluster/_common/log4j2.xml -Ddruid.extensions.directory="extensions" -Ddruid.extensions.loadList=[\"postgresql-metadata-storage\"] -Ddruid.metadata.storage.type=postgresql org.apache.druid.cli.Main tools metadata-update --connectURI="<postgresql-uri>" --user <user> --password <pass> --base druid --action add-used-flag-last-updated-to-segments
```


#### Manual ALTER TABLE

```SQL
ALTER TABLE druid_segments
ADD used_flag_last_updated varchar(255);
```
