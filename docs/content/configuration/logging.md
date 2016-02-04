---
layout: doc_page
---
Logging
==========================

Druid nodes will emit logs that are useful for debugging to the console. Druid nodes also emit periodic metrics about their state. For more about metrics, see [Configuration](../configuration/index.html). Metric logs are printed to the console by default, and can be disabled with `-Ddruid.emitter.logging.logLevel=debug`.

Druid uses [log4j2](http://logging.apache.org/log4j/2.x/) for logging. Logging can be configured with a log4j2.xml file. Add the path to the directory containing the log4j2.xml file (e.g. the _common/ dir) to your classpath if you want to override default Druid log configuration. Note that this directory should be earlier in the classpath than the druid jars. The easiest way to do this is to prefix the classpath with the config dir.

To enable java logging to go through log4j2, set the `-Djava.util.logging.manager=org.apache.logging.log4j.jul.LogManager` server parameter.

An example log4j2.xml ships with Druid under config/_common/log4j2.xml, and a sample file is also shown below:

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
    <Logger name="io.druid.jetty.RequestLog" additivity="false" level="DEBUG">
        <AppenderRef ref="Console"/>
    </Logger>
    -->
  </Loggers>
</Configuration>
```
