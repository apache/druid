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

Druid fully supports Java 8 and 11, and has experimental support for [Java 17](#java-17).
The project team recommends Java 11.

The project team recommends using an OpenJDK-based Java distribution. There are many free and actively-supported
distributions available, including
[Amazon Corretto](https://docs.aws.amazon.com/corretto/latest/corretto-11-ug/what-is-corretto-11.html),
[Azul Zulu](https://www.azul.com/downloads/?version=java-11-lts&package=jdk), and
[Eclipse Temurin](https://adoptium.net/temurin/releases?version=11).
The project team does not recommend any specific distribution over any other.

Druid relies on the environment variables `JAVA_HOME` or `DRUID_JAVA_HOME` to find Java on the machine. You can set
`DRUID_JAVA_HOME` if there is more than one instance of Java. To verify Java requirements for your environment, run the
`bin/verify-java` script.

## Garbage collection

In general, the project team recommends using the G1 collector with default settings. This is the default collector in
Java 11. To enable G1 on Java 8, use `-XX:+UseG1GC`. There is no harm in explicitly specifying this on Java 11 as well.

Garbage collector selection and tuning is a form of sport in the Java community. There may be situations where adjusting
garbage collection configuration improves or worsens performance. The project team's guidance is that most people do
not need to stray away from G1 with default settings.

## Strong encapsulation

Java 9 and beyond (including Java 11) include the capability for
[strong encapsulation](https://dev.java/learn/strong-encapsulation-\(of-jdk-internals\)/) of internal JDK APIs. Druid
uses certain internal JDK APIs for functionality- and performance-related reasons. In Java 11, this leads to log
messages like the following:

```
WARNING: An illegal reflective access operation has occurred
WARNING: Use --illegal-access=warn to enable warnings of further illegal reflective access operations
WARNING: All illegal access operations will be denied in a future release
```

These warning messages are harmless, and can be ignored. However, you can avoid them entirely if you wish by adding the
following Java command line parameters. These parameters are not part of the default configurations that ship with
Druid, because Java 8 does not recognize these parameters and fails to start up if they are provided.

To do this, add the following lines to your `jvm.config` files:

```
--add-exports=java.base/jdk.internal.ref=ALL-UNNAMED
--add-exports=java.base/jdk.internal.misc=ALL-UNNAMED
--add-opens=java.base/java.lang=ALL-UNNAMED
--add-opens=java.base/java.io=ALL-UNNAMED
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
```

Additionally, tasks run by [MiddleManagers](../design/architecture.md) execute in separate JVMs. The command line for
these JVMs is given by `druid.indexer.runner.javaOptsArray` or `druid.indexer.runner.javaOpts` in
`middleManager/runtime.properties`. Java command line parameters for tasks must be specified here. For example, use
a line like the following:

```
druid.indexer.runner.javaOptsArray=["-server","-Xms1g","-Xmx1g","-XX:MaxDirectMemorySize=1g","-Duser.timezone=UTC","-Dfile.encoding=UTF-8","-XX:+ExitOnOutOfMemoryError","-Djava.util.logging.manager=org.apache.logging.log4j.jul.LogManager","--add-exports=java.base/jdk.internal.ref=ALL-UNNAMED","--add-exports=java.base/jdk.internal.misc=ALL-UNNAMED","--add-opens=java.base/java.lang=ALL-UNNAMED","--add-opens=java.base/java.io=ALL-UNNAMED","--add-opens=java.base/java.nio=ALL-UNNAMED","--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED","--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"]
```

The `Xms`, `Xmx`, and `MaxDirectMemorySize` parameters in the line above are merely an example. You may use different
values in your specific environment.

## Java 17

Druid has experimental support for Java 17.

An important change in Java 17 is that [strong encapsulation](#strong-encapsulation) is enabled by default. The various
`--add-opens` and `--add-exports` parameters listed in the [strong encapsulation](#strong-encapsulation) section are
required in all `jvm.config` files and in `druid.indexer.runner.javaOpts` or `druid.indexer.runner.javaOptsArray` on
MiddleManagers. Failure to include these parameters leads to failure of various operations.

In addition, Druid's launch scripts detect Java 17 and log the following message rather than starting up:

```
Druid requires Java 8 or 11. Your current version is: 17.X.Y.
```

You can skip this check with an environment variable:

```
export DRUID_SKIP_JAVA_CHECK=1
```
