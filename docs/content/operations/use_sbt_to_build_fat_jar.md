---
layout: doc_page
---

Content for build.sbt
---------------------
```scala
libraryDependencies ++= Seq(
  "com.amazonaws" % "aws-java-sdk" % "1.9.23" exclude("common-logging", "common-logging"),
  "org.joda" % "joda-convert" % "1.7",
  "joda-time" % "joda-time" % "2.7",
  "org.apache.druid" % "druid" % "0.8.1" excludeAll (
    ExclusionRule("org.ow2.asm"),
    ExclusionRule("com.fasterxml.jackson.core"),
    ExclusionRule("com.fasterxml.jackson.datatype"),
    ExclusionRule("com.fasterxml.jackson.dataformat"),
    ExclusionRule("com.fasterxml.jackson.jaxrs"),
    ExclusionRule("com.fasterxml.jackson.module")
  ),
  "org.apache.druid" % "druid-services" % "0.8.1" excludeAll (
    ExclusionRule("org.ow2.asm"),
    ExclusionRule("com.fasterxml.jackson.core"),
    ExclusionRule("com.fasterxml.jackson.datatype"),
    ExclusionRule("com.fasterxml.jackson.dataformat"),
    ExclusionRule("com.fasterxml.jackson.jaxrs"),
    ExclusionRule("com.fasterxml.jackson.module")
  ),
  "org.apache.druid" % "druid-indexing-service" % "0.8.1" excludeAll (
    ExclusionRule("org.ow2.asm"),
    ExclusionRule("com.fasterxml.jackson.core"),
    ExclusionRule("com.fasterxml.jackson.datatype"),
    ExclusionRule("com.fasterxml.jackson.dataformat"),
    ExclusionRule("com.fasterxml.jackson.jaxrs"),
    ExclusionRule("com.fasterxml.jackson.module")
  ),
  "org.apache.druid" % "druid-indexing-hadoop" % "0.8.1" excludeAll (
    ExclusionRule("org.ow2.asm"),
    ExclusionRule("com.fasterxml.jackson.core"),
    ExclusionRule("com.fasterxml.jackson.datatype"),
    ExclusionRule("com.fasterxml.jackson.dataformat"),
    ExclusionRule("com.fasterxml.jackson.jaxrs"),
    ExclusionRule("com.fasterxml.jackson.module")
  ),
  "org.apache.druid.extensions" % "mysql-metadata-storage" % "0.8.1" excludeAll (
    ExclusionRule("org.ow2.asm"),
    ExclusionRule("com.fasterxml.jackson.core"),
    ExclusionRule("com.fasterxml.jackson.datatype"),
    ExclusionRule("com.fasterxml.jackson.dataformat"),
    ExclusionRule("com.fasterxml.jackson.jaxrs"),
    ExclusionRule("com.fasterxml.jackson.module")
  ),
  "org.apache.druid.extensions" % "druid-s3-extensions" % "0.8.1" excludeAll (
    ExclusionRule("org.ow2.asm"),
    ExclusionRule("com.fasterxml.jackson.core"),
    ExclusionRule("com.fasterxml.jackson.datatype"),
    ExclusionRule("com.fasterxml.jackson.dataformat"),
    ExclusionRule("com.fasterxml.jackson.jaxrs"),
    ExclusionRule("com.fasterxml.jackson.module")
  ),
  "org.apache.druid.extensions" % "druid-histogram" % "0.8.1" excludeAll (
    ExclusionRule("org.ow2.asm"),
    ExclusionRule("com.fasterxml.jackson.core"),
    ExclusionRule("com.fasterxml.jackson.datatype"),
    ExclusionRule("com.fasterxml.jackson.dataformat"),
    ExclusionRule("com.fasterxml.jackson.jaxrs"),
    ExclusionRule("com.fasterxml.jackson.module")
  ),
  "org.apache.druid.extensions" % "druid-hdfs-storage" % "0.8.1" excludeAll (
    ExclusionRule("org.ow2.asm"),
    ExclusionRule("com.fasterxml.jackson.core"),
    ExclusionRule("com.fasterxml.jackson.datatype"),
    ExclusionRule("com.fasterxml.jackson.dataformat"),
    ExclusionRule("com.fasterxml.jackson.jaxrs"),
    ExclusionRule("com.fasterxml.jackson.module")
  ),
  "com.fasterxml.jackson.core" % "jackson-annotations" % "2.3.0",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.3.0",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.3.0",
  "com.fasterxml.jackson.datatype" % "jackson-datatype-guava" % "2.3.0",
  "com.fasterxml.jackson.datatype" % "jackson-datatype-joda" % "2.3.0",
  "com.fasterxml.jackson.jaxrs" % "jackson-jaxrs-base" % "2.3.0",
  "com.fasterxml.jackson.jaxrs" % "jackson-jaxrs-json-provider" % "2.3.0",
  "com.fasterxml.jackson.jaxrs" % "jackson-jaxrs-smile-provider" % "2.3.0",
  "com.fasterxml.jackson.module" % "jackson-module-jaxb-annotations" % "2.3.0",
  "com.sun.jersey" % "jersey-servlet" % "1.17.1",
  "mysql" % "mysql-connector-java" % "5.1.34",
  "org.scalatest" %% "scalatest" % "2.2.3" % "test",
  "org.mockito" % "mockito-core" % "1.10.19" % "test"
)

assemblyMergeStrategy in assembly := {
  case path if path contains "pom." => MergeStrategy.first
  case path if path contains "javax.inject.Named" => MergeStrategy.first
  case path if path contains "mime.types" => MergeStrategy.first
  case path if path contains "org/apache/commons/logging/impl/SimpleLog.class" => MergeStrategy.first
  case path if path contains "org/apache/commons/logging/impl/SimpleLog$1.class" => MergeStrategy.first
  case path if path contains "org/apache/commons/logging/impl/NoOpLog.class" => MergeStrategy.first
  case path if path contains "org/apache/commons/logging/LogFactory.class" => MergeStrategy.first
  case path if path contains "org/apache/commons/logging/LogConfigurationException.class" => MergeStrategy.first
  case path if path contains "org/apache/commons/logging/Log.class" => MergeStrategy.first
  case path if path contains "META-INF/jersey-module-version" => MergeStrategy.first
  case path if path contains ".properties" => MergeStrategy.first
  case path if path contains ".class" => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
```
