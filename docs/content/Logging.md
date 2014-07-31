---
layout: doc_page
---
Logging
==========================

Druid nodes will emit logs that are useful for debugging to the console. Druid nodes also emit periodic metrics about their state. For more about metrics, see [Configuration](Configuration.html). Metric logs are printed to the console by default, and can be disabled with `-Ddruid.emitter.logging.logLevel=debug`.

Druid uses [log4j](http://logging.apache.org/log4j/2.x/) for logging, and console logs can be configured by adding a log4j.xml file. Add this xml file to your classpath if you want to override default Druid log configuration.

An example log4j.xml file is shown below:

```
<?xml version="1.0" encoding="UTF-8" ?>
<!DOCTYPE log4j:configuration SYSTEM "log4j.dtd">

<log4j:configuration xmlns:log4j="http://jakarta.apache.org/log4j/">

  <appender name="ConsoleAppender" class="org.apache.log4j.ConsoleAppender">
    <layout class="org.apache.log4j.PatternLayout">
      <param name="ConversionPattern" value="%d{ISO8601} %-5p [%t] %c - %m%n"/>
    </layout>
  </appender>

  <!-- ServerView-related stuff is way too chatty -->
  <logger name="io.druid.client.BatchServerInventoryView">
    <level value="warn"/>
  </logger>
  <logger name="io.druid.curator.inventory.CuratorInventoryManager">
    <level value="warn"/>
  </logger>

  <root>
    <priority value="info" />
    <appender-ref ref="ConsoleAppender"/>
  </root>

</log4j:configuration>
```