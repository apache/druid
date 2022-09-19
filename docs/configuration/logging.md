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
> Although the log4j configuration file is shared with Druid's task peon processes,
> the appenders in this file DO NOT take effect for peon processes, which always output logs to standard output.
> Middle Managers redirect task logs from standard output to [long-term storage](index.md#log-long-term-storage).
>
> However, log level settings do take effect for these task peon processes,
> which means you can still configure loggers at different logging level for task logs using `log4j2.xml`.

## Log directory
The included log4j2.xml configuration for Druid and ZooKeeper will output logs to the `log` directory at the root of the distribution.

If you want to change the log directory, set the environment variable `DRUID_LOG_DIR` to the right directory before you start Druid.

## All-in-one start commands

If you use one of the all-in-one start commands, such as `bin/start-micro-quickstart`, then in the default configuration
each service has two kind of log files. The main log file (for example, `log/historical.log`) is written by log4j2 and
is rotated periodically.

The secondary log file (for example, `log/historical.stdout.log`) contains anything that is written by the component
directly to standard output or standard error without going through log4j2. This consists mainly of messages from the
Java runtime itself. This file is not rotated, but it is generally small due to the low volume of messages. If
necessary, you can truncate it using the Linux command `truncate --size 0 log/historical.stdout.log`.

## Avoid reflective access warnings in logs

On Java 11, you may see warnings like this in log files:

```
WARNING: An illegal reflective access operation has occurred
WARNING: Use --illegal-access=warn to enable warnings of further illegal reflective access operations
WARNING: All illegal access operations will be denied in a future release
```

These messages do not cause harm, but you can avoid them by adding the following lines to your `jvm.config` files. These
lines are not part of the default JVM configs that ship with Druid, because Java 8 will not recognize these options and
will fail to start up.

```
--add-exports=java.base/jdk.internal.ref=ALL-UNNAMED
--add-exports=java.base/jdk.internal.perf=ALL-UNNAMED
--add-opens=java.base/java.lang=ALL-UNNAMED
--add-opens=jdk.management/com.sun.management.internal=ALL-UNNAMED
```

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
