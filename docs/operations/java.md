---
id: java
title: "Java runtime"
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

Apache Druid is written in Java and requires a Java runtime. This page provides details about obtaining and configuring
a Java runtime for Druid.

## Selecting a Java runtime

Druid fully supports Java 8u92+, Java 11, and Java 17. The project team recommends Java 17.

The project team recommends using an OpenJDK-based Java distribution. There are many free and actively-supported
distributions available, including
[Amazon Corretto](https://docs.aws.amazon.com/corretto/latest/corretto-17-ug/what-is-corretto-17.html),
[Azul Zulu](https://www.azul.com/downloads/?version=java-17-lts&package=jdk), and
[Eclipse Temurin](https://adoptium.net/temurin/releases?version=17).
The project team does not recommend any specific distribution over any other.

Druid relies on the environment variables `JAVA_HOME` or `DRUID_JAVA_HOME` to find Java on the machine. You can set
`DRUID_JAVA_HOME` if there is more than one instance of Java. To verify Java requirements for your environment, run the
`bin/verify-java` script.

## Garbage collection

In general, the project team recommends using the G1 collector with default settings. This is the default collector in
Java 11 and 17. To enable G1 on Java 8, use `-XX:+UseG1GC`. There is no harm in explicitly specifying this on Java 11
or 17 as well.

Garbage collector selection and tuning is a form of sport in the Java community. There may be situations where adjusting
garbage collection configuration improves or worsens performance. The project team's guidance is that most people do
not need to stray away from G1 with default settings.

## Strong encapsulation

Java 9 and beyond (including Java 11 and 17) include the capability for
[strong encapsulation](https://dev.java/learn/strong-encapsulation-\(of-jdk-internals\)/) of internal JDK APIs. Druid
uses certain internal JDK APIs for functionality- and performance-related reasons. Therefore, to avoid warning messages
in Java 11, and errors in Java 17, certain packages must be made openable.

Druid's out-of-box configuration does this transparently when you use the bundled `bin/start-druid` or similar commands.
In this case, there is nothing special you need to do to run successfully on Java 11 or 17.

However, if you have customized your Druid service launching system, you will need to add the following lines to your
`jvm.config` files on Java 11 or 17:

```
--add-exports=java.base/jdk.internal.misc=ALL-UNNAMED \
--add-exports=java.base/jdk.internal.ref=ALL-UNNAMED \
--add-opens=java.base/java.nio=ALL-UNNAMED \
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED \
--add-opens=java.base/java.io=ALL-UNNAMED \
--add-opens=java.base/java.lang=ALL-UNNAMED \
--add-opens=jdk.management/com.sun.management.internal=ALL-UNNAMED \
```

Additionally, tasks run by [MiddleManagers](../design/architecture.md) execute in separate JVMs. The command line for
these JVMs is given by `druid.indexer.runner.javaOptsArray` or `druid.indexer.runner.javaOpts` in
`middleManager/runtime.properties`. Java command line parameters for tasks must be specified here. For example, use
a line like the following:

```
druid.indexer.runner.javaOptsArray=["-server","-Xms1g","-Xmx1g","-XX:MaxDirectMemorySize=1g","-Duser.timezone=UTC","-Dfile.encoding=UTF-8","-XX:+ExitOnOutOfMemoryError","-Djava.util.logging.manager=org.apache.logging.log4j.jul.LogManager","--add-exports=java.base/jdk.internal.misc=ALL-UNNAMED","--add-exports=java.base/jdk.internal.ref=ALL-UNNAMED","--add-opens=java.base/java.nio=ALL-UNNAMED","--add-opens=java.base/sun.nio.ch=ALL-UNNAMED","--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED","--add-opens=java.base/java.io=ALL-UNNAMED","--add-opens=java.base/java.lang=ALL-UNNAMED","--add-opens=jdk.management/com.sun.management.internal=ALL-UNNAMED"]
```

The `Xms`, `Xmx`, and `MaxDirectMemorySize` parameters in the line above are merely an example. You may use different
values in your specific environment.

On Java 11, if these parameters are not included, you will see warnings like the following:

```
WARNING: An illegal reflective access operation has occurred
WARNING: Use --illegal-access=warn to enable warnings of further illegal reflective access operations
WARNING: All illegal access operations will be denied in a future release
```

On Java 17, if these parameters are not included, you will see errors on startup like:

```
Exception in thread "main" java.lang.ExceptionInInitializerError
```
