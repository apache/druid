---
id: logging
title: "Logging"
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


Apache Druid processes will emit logs that are useful for debugging to the console. Druid processes also emit periodic metrics about their state. For more about metrics, see [Configuration](../configuration/index.md#enabling-metrics). Metric logs are printed to the console by default, and can be disabled with `-Ddruid.emitter.logging.logLevel=debug`.

Druid uses [log4j2](http://logging.apache.org/log4j/2.x/) for logging. Its configuration file locates at conf/druid/{config}/_common/log4j2.xml.

By default, Druid uses `RollingRandomAccessFile` for rollover daily, and keeps log files up to 7 days. 
If that's not suitable in your case, you could modify the log4j2.xml to meet your need. 

An example log4j2.xml ships with Druid under conf/druid/{config}/_common/log4j2.xml, and a sample file is also shown below:

```
<?xml version="1.0" encoding="UTF-8" ?>
<Configuration status="WARN">
  <Appenders>
    <Console name="Console" target="SYSTEM_OUT">
      <PatternLayout pattern="%d{ISO8601} %p [%t] %c - %m%n"/>
    </Console>
  </Appenders>
  <Loggers>
    <Root level="info">
      <AppenderRef ref="Console"/>
    </Root>

    <!-- Uncomment to enable logging of all HTTP requests
    <Logger name="org.apache.druid.jetty.RequestLog" additivity="false" level="DEBUG">
        <AppenderRef ref="Console"/>
    </Logger>
    -->
  </Loggers>
</Configuration>
```

## How to change log directory
By default, Druid outputs the logs to a directory `log` under the directory where you launch Druid.
For example, if Druid is started from its `bin` directory, there will a subdirectory `log` generated under `bin` directory to hold the log files.
If you want to change the log directory, set environment variable `DRUID_LOG_DIR` to the right directory before you start Druid.


## My logs are really chatty, can I set them to asynchronously write?

Yes, using a `log4j2.xml` similar to the following causes some of the more chatty classes to write asynchronously:

```
<?xml version="1.0" encoding="UTF-8" ?>
<Configuration status="WARN">
  <Appenders>
    <Console name="Console" target="SYSTEM_OUT">
      <PatternLayout pattern="%d{ISO8601} %p [%t] %c - %m%n"/>
    </Console>
  </Appenders>
  <Loggers>
    <AsyncLogger name="org.apache.druid.curator.inventory.CuratorInventoryManager" level="debug" additivity="false">
      <AppenderRef ref="Console"/>
    </AsyncLogger>
    <AsyncLogger name="org.apache.druid.client.BatchServerInventoryView" level="debug" additivity="false">
      <AppenderRef ref="Console"/>
    </AsyncLogger>
    <!-- Make extra sure nobody adds logs in a bad way that can hurt performance -->
    <AsyncLogger name="org.apache.druid.client.ServerInventoryView" level="debug" additivity="false">
      <AppenderRef ref="Console"/>
    </AsyncLogger>
    <AsyncLogger name ="org.apache.druid.java.util.http.client.pool.ChannelResourceFactory" level="info" additivity="false">
      <AppenderRef ref="Console"/>
    </AsyncLogger>
    <Root level="info">
      <AppenderRef ref="Console"/>
    </Root>
  </Loggers>
</Configuration>
```
