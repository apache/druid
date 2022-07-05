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

**If you have set `druid.metadata.storage.connector.createTables` to `true` (which is the default), and your metadata connect user has DDL privileges, you can disregard this section. You are urged to still evaluate the optional section below**

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

### Populating `used_flag_last_updated` column of the segments table after upgrade (Optional)

This is an optional step to take **after** you upgrade the Overlord and Coordinator to `0.24+` (from `0.23` and earlier). If you do not take this action and are also using `druid.coordinator.kill.on=true`, the logic to identify segments that can be killed will not honor `druid.coordinator.kill.bufferPeriod` for the rows in the segments table where `used_flag_last_updated == null`.

#### CLI tool

Druid provides a `metadata-update` tool for updating Druid's metadata tables. Note that this tool will update `used_flag_last_updated` for all rows that match `used = false` in one transaction.

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
java -classpath "lib/*" -Dlog4j.configurationFile=conf/druid/cluster/_common/log4j2.xml -Ddruid.extensions.directory="extensions" -Ddruid.extensions.loadList=[\"mysql-metadata-storage\"] -Ddruid.metadata.storage.type=mysql org.apache.druid.cli.Main tools metadata-update --connectURI="<mysql-uri>" --user <user> --password <pass> --base druid --action populate-used-flag-last-updated-column-in-segments
```

##### PostgreSQL

```bash
cd ${DRUID_ROOT}
java -classpath "lib/*" -Dlog4j.configurationFile=conf/druid/cluster/_common/log4j2.xml -Ddruid.extensions.directory="extensions" -Ddruid.extensions.loadList=[\"postgresql-metadata-storage\"] -Ddruid.metadata.storage.type=postgresql org.apache.druid.cli.Main tools metadata-update --connectURI="<postgresql-uri>" --user <user> --password <pass> --base druid --action populate-used-flag-last-updated-column-in-segments
```


#### Manual UPDATE

Note that we choose a random date string for this example. We recommend using the current UTC time when you invoke the command. If you have lots of rows that match the conditional `used = false`, you may want to incrementally update the table using a limit clause.

```SQL
UPDATE druid_segment
SET used_flag_last_updated = '2022-01-01T00:00:00.000Z'
where used = false;
```
