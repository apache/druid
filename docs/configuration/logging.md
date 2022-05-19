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


Apache Druid processes will emit logs that are useful for debugging to log files. 
These processes also emit periodic [metrics](../configuration/index.md#enabling-metrics) about their state.
Metric info logs can be disabled with `-Ddruid.emitter.logging.logLevel=debug`.

Druid uses [log4j2](http://logging.apache.org/log4j/2.x/) for logging. 
The default configuration file log4j2.xml ships with Druid under conf/druid/{config}/_common/log4j2.xml .

By default, Druid uses `RollingRandomAccessFile` for rollover daily, and keeps log files up to 7 days. 
If that's not suitable in your case, you could modify the log4j2.xml to meet your need.

An example log4j2.xml file is shown below:

```
<?xml version="1.0" encoding="UTF-8" ?>
<Configuration status="WARN">
  <Console name="Console" target="SYSTEM_OUT">
    <PatternLayout pattern="%d{ISO8601} %p [%t] %c - %m%n"/>
  </Console>
    
  <RollingRandomAccessFile name="FileAppender"
                           fileName="${sys:druid.log.path}/${sys:druid.node.type}.log"
                           filePattern="${sys:druid.log.path}/${sys:druid.node.type}.%d{yyyyMMdd}.log">
    <PatternLayout pattern="%d{ISO8601} %p [%t] %c - %m%n"/>
    <Policies>
        <TimeBasedTriggeringPolicy interval="1" modulate="true"/>
    </Policies>
    <DefaultRolloverStrategy>
      <Delete basePath="${sys:druid.log.path}/" maxDepth="1">
        <IfFileName glob="*.log" />
        <IfLastModified age="7d" />
      </Delete>
    </DefaultRolloverStrategy>
  </RollingRandomAccessFile>
  <Loggers>
    <Root level="info">
      <AppenderRef ref="FileAppender"/>
    </Root>

    <!-- Uncomment to enable logging of all HTTP requests
    <Logger name="org.apache.druid.jetty.RequestLog" additivity="false" level="DEBUG">
        <AppenderRef ref="FileAppender"/>
    </Logger>
    -->
  </Loggers>
</Configuration>
```

> NOTE:
> Although the log4j configuration file is shared with Druid's peon processes, 
> the appenders in this file DO NOT take effect for peon processes for they always output logs to console.
> And middle managers are responsible to redirect the console output to task log files.
>
> But the logging levels settings take effect for these peon processes 
> which means you can still configure loggers at different logging level for peon processes in this file.
> 

## How to change log directory
By default, Druid outputs the logs to a directory `log` under the directory where Druid is launched from.
For example, if Druid is started from its `bin` directory, there will be a subdirectory `log` generated under `bin` directory to hold the log files.
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
    <!-- AsyncLogger instead of Logger -->
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
