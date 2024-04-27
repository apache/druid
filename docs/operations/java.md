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
uses certain internal JDK APIs, which must be added to `--add-exports` and `--add-opens` on the Java command line.

On Java 11, if these parameters are not included, you will see warnings like the following:

```
WARNING: An illegal reflective access operation has occurred
WARNING: Use --illegal-access=warn to enable warnings of further illegal reflective access operations
WARNING: All illegal access operations will be denied in a future release
```

On Java 17, if these parameters are not included, you will see errors on startup like the following:

```
Exception in thread "main" java.lang.ExceptionInInitializerError
```

Druid's out-of-box configuration adds these parameters transparently when you use the bundled `bin/start-druid` or
similar commands. In this case, there is nothing special you need to do to run successfully on Java 11 or 17. However,
if you have customized your Druid service launching system, you will need to ensure the required Java parameters are
added. There are many ways of doing this. Choose the one that works best for you.

1. The simplest approach: use Druid's bundled `bin/start-druid` script to launch Druid.

2. If you launch Druid using `bin/supervise -c <config>`, ensure your config file uses `bin/run-druid`. This
   script uses `bin/run-java` internally, and automatically adds the proper flags.

3. If you launch Druid using a `java` command, replace `java` with `bin/run-java`. Druid's bundled
   `bin/run-java` script automatically adds the proper flags.

4. If you launch Druid without using its bundled scripts, ensure the following parameters are added to your Java
   command line:

```
--add-exports=java.base/jdk.internal.misc=ALL-UNNAMED \
--add-exports=java.base/jdk.internal.ref=ALL-UNNAMED \
--add-opens=java.base/java.nio=ALL-UNNAMED \
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED \
--add-opens=java.base/java.io=ALL-UNNAMED \
--add-opens=java.base/java.lang=ALL-UNNAMED \
--add-opens=jdk.management/com.sun.management.internal=ALL-UNNAMED
```
