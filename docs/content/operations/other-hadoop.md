---
layout: doc_page
---
# Working with different versions of Hadoop

Druid can interact with Hadoop in two ways:

1. [Use HDFS for deep storage](../development/extensions-core/hdfs.html) using the druid-hdfs-storage extension.
2. [Batch-load data from Hadoop](../ingestion/batch-ingestion.html) using Map/Reduce jobs.

These are not necessarily linked together; you can load data with Hadoop jobs into a non-HDFS deep storage (like S3),
and you can use HDFS for deep storage even if you're loading data from streams rather than using Hadoop jobs.

For best results, use these tips when configuring Druid to interact with your favorite Hadoop distribution.

## Tip #1: Place Hadoop XMLs on Druid classpath

Place your Hadoop configuration XMLs (core-site.xml, hdfs-site.xml, yarn-site.xml, mapred-site.xml) on the classpath
of your Druid nodes. You can do this by copying them into `conf/druid/_common/core-site.xml`,
`conf/druid/_common/hdfs-site.xml`, and so on. This allows Druid to find your Hadoop cluster and properly submit jobs.

## Tip #2: Classloader modification on Hadoop (Map/Reduce jobs only)

Druid uses a number of libraries that are also likely present on your Hadoop cluster, and if these libraries conflict,
your Map/Reduce jobs can fail. This problem can be avoided by enabling classloader isolation using the Hadoop job
property `mapreduce.job.classloader = true`. This instructs Hadoop to use a separate classloader for Druid dependencies
and for Hadoop's own dependencies.

If your version of Hadoop does not support this functionality, you can also try setting the property
`mapreduce.job.user.classpath.first = true`. This instructs Hadoop to prefer loading Druid's version of a library when
there is a conflict.

Generally, you should only set one of these parameters, not both.

These properties can be set in either one of the following ways:

- Using the task definition, e.g. add `"mapreduce.job.classloader": "true"` to the `jobProperties` of the `tuningConfig` of your indexing task (see the [batch ingestion documentation](../ingestion/batch-ingestion.html)).
- Using system properties, e.g. on the middleManager set `druid.indexer.runner.javaOpts=... -Dhadoop.mapreduce.job.classloader=true`.

### Overriding specific classes

When `mapreduce.job.classloader = true`, it is also possible to specifically define which classes should be loaded from the hadoop system classpath and which should be loaded from job-supplied JARs.

This is controlled by defining class inclusion/exclusion patterns in the `mapreduce.job.classloader.system.classes` property in the `jobProperties` of `tuningConfig`.

For example, some community members have reported version incompatibility errors with the Validator class:

```
Error: java.lang.ClassNotFoundException: javax.validation.Validator
```

The following `jobProperties` excludes `javax.validation.` classes from being loaded from the system classpath, while including those from `java.,javax.,org.apache.commons.logging.,org.apache.log4j.,org.apache.hadoop.`.

```
"jobProperties": {
  "mapreduce.job.classloader": "true",
  "mapreduce.job.classloader.system.classes": "-javax.validation.,java.,javax.,org.apache.commons.logging.,org.apache.log4j.,org.apache.hadoop."
}
```

[mapred-default.xml](https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/mapred-default.xml) documentation contains more information about this property.

## Tip #3: Use specific versions of Hadoop libraries

Druid loads Hadoop client libraries from two different locations. Each set of libraries is loaded in an isolated
classloader.

1. HDFS deep storage uses jars from `extensions/druid-hdfs-storage/` to read and write Druid data on HDFS.
2. Batch ingestion uses jars from `hadoop-dependencies/` to submit Map/Reduce jobs (location customizable via the
`druid.extensions.hadoopDependenciesDir` runtime property; see [Configuration](../configuration/index.html)).

`hadoop-client:2.8.3` is the default version of the Hadoop client bundled with Druid for both purposes. This works with
many Hadoop distributions (the version does not necessarily need to match), but if you run into issues, you can instead
have Druid load libraries that exactly match your distribution. To do this, either copy the jars from your Hadoop
cluster, or use the `pull-deps` tool to download the jars from a Maven repository.

### Preferred: Load using Druid's standard mechanism

If you have issues with HDFS deep storage, you can switch your Hadoop client libraries by recompiling the
druid-hdfs-storage extension using an alternate version of the Hadoop client libraries. You can do this by editing
the main Druid pom.xml and rebuilding the distribution by running `mvn package`.

If you have issues with Map/Reduce jobs, you can switch your Hadoop client libraries without rebuilding Druid. You can
do this by adding a new set of libraries to the `hadoop-dependencies/` directory (or another directory specified by
druid.extensions.hadoopDependenciesDir) and then using `hadoopDependencyCoordinates` in the
[Hadoop Index Task](../ingestion/batch-ingestion.html) to specify the Hadoop dependencies you want Druid to load.

Example:

Suppose you specify `druid.extensions.hadoopDependenciesDir=/usr/local/druid_tarball/hadoop-dependencies`, and you have downloaded
`hadoop-client` 2.3.0 and 2.4.0, either by copying them from your Hadoop cluster or by using `pull-deps` to download
the jars from a Maven repository. Then underneath `hadoop-dependencies`, your jars should look like this:

```
hadoop-dependencies/
└── hadoop-client
    ├── 2.3.0
    │   ├── activation-1.1.jar
    │   ├── avro-1.7.4.jar
    │   ├── commons-beanutils-1.7.0.jar
    │   ├── commons-beanutils-core-1.8.0.jar
    │   ├── commons-cli-1.2.jar
    │   ├── commons-codec-1.4.jar
    ..... lots of jars
    └── 2.4.0
        ├── activation-1.1.jar
        ├── avro-1.7.4.jar
        ├── commons-beanutils-1.7.0.jar
        ├── commons-beanutils-core-1.8.0.jar
        ├── commons-cli-1.2.jar
        ├── commons-codec-1.4.jar
    ..... lots of jars
```

As you can see, under `hadoop-client`, there are two sub-directories, each denotes a version of `hadoop-client`.

Next, use `hadoopDependencyCoordinates` in [Hadoop Index Task](../ingestion/batch-ingestion.html) to specify the Hadoop dependencies you want Druid to load.

For example, in your Hadoop Index Task spec file, you can write:

`"hadoopDependencyCoordinates": ["org.apache.hadoop:hadoop-client:2.4.0"]`

This instructs Druid to load hadoop-client 2.4.0 when processing the task. What happens behind the scene is that Druid first looks for a folder
called `hadoop-client` underneath `druid.extensions.hadoopDependenciesDir`, then looks for a folder called `2.4.0`
underneath `hadoop-client`, and upon successfully locating these folders, hadoop-client 2.4.0 is loaded.

### Alternative: Append your Hadoop jars to the Druid classpath

You can also load Hadoop client libraries in Druid's main classloader, rather than an isolated classloader. This
mechanism is relatively easy to reason about, but it also means that you have to ensure that all dependency jars on the
classpath are compatible. That is, Druid makes no provisions while using this method to maintain class loader isolation
so you must make sure that the jars on your classpath are mutually compatible.

1. Set `druid.indexer.task.defaultHadoopCoordinates=[]`. By setting this to an empty list, Druid will not load any other Hadoop dependencies except the ones specified in the classpath.
2. Append your Hadoop jars to Druid's classpath. Druid will load them into the system.

## Notes on specific Hadoop distributions

If the tips above do not solve any issues you are having with HDFS deep storage or Hadoop batch indexing, you may
have luck with one of the following suggestions contributed by the Druid community.

### CDH

Members of the community have reported dependency conflicts between the version of Jackson used in CDH and Druid when running a Mapreduce job like:

```
java.lang.VerifyError: class com.fasterxml.jackson.datatype.guava.deser.HostAndPortDeserializer overrides final method deserialize.(Lcom/fasterxml/jackson/core/JsonParser;Lcom/fasterxml/jackson/databind/DeserializationContext;)Ljava/lang/Object;
```

**Preferred workaround**

First, try the tip under "Classloader modification on Hadoop" above. More recent versions of CDH have been reported to
work with the classloader isolation option (`mapreduce.job.classloader = true`).

**Alternate workaround - 1**

You can try editing Druid's pom.xml dependencies to match the version of Jackson in your Hadoop version and recompile Druid.

For more about building Druid, please see [Building Druid](../development/build.html).

**Alternate workaround - 2**

Another workaround solution is to build a custom fat jar of Druid using [sbt](http://www.scala-sbt.org/), which manually excludes all the conflicting Jackson dependencies, and then put this fat jar in the classpath of the command that starts overlord indexing service. To do this, please follow the following steps.

(1) Download and install sbt.

(2) Make a new directory named 'druid_build'.

(3) Cd to 'druid_build' and create the build.sbt file with the content [here](./use_sbt_to_build_fat_jar.html).

You can always add more building targets or remove the ones you don't need.

(4) In the same directory create a new directory named 'project'.

(5) Put the druid source code into 'druid_build/project'.

(6) Create a file 'druid_build/project/assembly.sbt' with content as follows.
```
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.13.0")
```

(7) In the 'druid_build' directory, run 'sbt assembly'.

(8) In the 'druid_build/target/scala-2.10' folder, you will find the fat jar you just build.

(9) Make sure the jars you've uploaded has been completely removed. The HDFS directory is by default '/tmp/druid-indexing/classpath'.

(10) Include the fat jar in the classpath when you start the indexing service. Make sure you've removed 'lib/*' from your classpath because now the fat jar includes all you need.

**Alternate workaround - 3**

If sbt is not your choice, you can also use `maven-shade-plugin` to make a fat jar: relocation all jackson packages will resolve it too. In this way, druid will not be affected by jackson library embedded in hadoop. Please follow the steps below:

(1) Add all extensions you needed to `services/pom.xml` like

 ```xml
 <dependency>
      <groupId>io.druid.extensions</groupId>
      <artifactId>druid-avro-extensions</artifactId>
      <version>${project.parent.version}</version>
  </dependency>

  <dependency>
      <groupId>io.druid.extensions.contrib</groupId>
      <artifactId>druid-parquet-extensions</artifactId>
      <version>${project.parent.version}</version>
  </dependency>

  <dependency>
      <groupId>io.druid.extensions</groupId>
      <artifactId>druid-hdfs-storage</artifactId>
      <version>${project.parent.version}</version>
  </dependency>

  <dependency>
      <groupId>io.druid.extensions</groupId>
      <artifactId>mysql-metadata-storage</artifactId>
      <version>${project.parent.version}</version>
  </dependency>
 ```

(2) Shade jackson packages and assemble a fat jar.

```xml
<plugin>
     <groupId>org.apache.maven.plugins</groupId>
     <artifactId>maven-shade-plugin</artifactId>
     <executions>
         <execution>
             <phase>package</phase>
             <goals>
                 <goal>shade</goal>
             </goals>
             <configuration>
                 <outputFile>
                     ${project.build.directory}/${project.artifactId}-${project.version}-selfcontained.jar
                 </outputFile>
                 <relocations>
                     <relocation>
                         <pattern>com.fasterxml.jackson</pattern>
                         <shadedPattern>shade.com.fasterxml.jackson</shadedPattern>
                     </relocation>
                 </relocations>
                 <artifactSet>
                     <includes>
                         <include>*:*</include>
                     </includes>
                 </artifactSet>
                 <filters>
                     <filter>
                         <artifact>*:*</artifact>
                         <excludes>
                             <exclude>META-INF/*.SF</exclude>
                             <exclude>META-INF/*.DSA</exclude>
                             <exclude>META-INF/*.RSA</exclude>
                         </excludes>
                     </filter>
                 </filters>
                 <transformers>
                     <transformer implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"/>
                 </transformers>
             </configuration>
         </execution>
     </executions>
 </plugin>
```

Copy out `services/target/xxxxx-selfcontained.jar` after `mvn install` in project root for further usage.

(3) run hadoop indexer (post an indexing task is not possible now) as below. `lib` is not needed anymore. As hadoop indexer is a standalone tool, you don't have to replace the jars of your running services:

```bash
java -Xmx32m \
  -Dfile.encoding=UTF-8 -Duser.timezone=UTC \
  -classpath config/hadoop:config/overlord:config/_common:$SELF_CONTAINED_JAR:$HADOOP_DISTRIBUTION/etc/hadoop \
  -Djava.security.krb5.conf=$KRB5 \
  io.druid.cli.Main index hadoop \
  $config_path
```
