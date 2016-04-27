---
layout: doc_page
---
# Working with different versions of Hadoop

## Download Hadoop Dependencies

```hadoop-client:2.3.0``` is already bundled in the Druid release tarball. You can get it by downloading the tarball at [druid.io](http://druid.io/downloads.html).
Unpack the tarball; You will see a ```hadoop-dependencies``` folder that contains all the Hadoop dependencies. Each dependency will have its own folder
that contains Hadoop jars.

You can also use the `pull-deps` tool to download other Hadoop dependencies you want. 
See [pull-deps](../operations/pull-deps.html) for a complete example.
Another way is using a minimal `pom.xml` only contains your version of `hadoop-client` and then run `mvn dependency:copy-dependency` to get required libraries.

## Load Hadoop dependencies

There are two different ways to let Druid pick up your Hadoop version, choose the one that fits your need.

### Load Hadoop dependencies from Hadoop dependencies directory

You can create a Hadoop dependency directory and tell Druid to load your Hadoop dependencies from there.

To make this work, follow the steps below

**Tell Druid where your Hadoop dependencies are**

Specify `druid.extensions.hadoopDependenciesDir` (root directory for Hadoop related dependencies) See [Configuration](../configuration/index.html).

The value for this property should be set to the absolute path of the folder that contains all the Hadoop dependencies. 
In general, you should simply reuse the release tarball's ```hadoop-dependencies``` directory.

Example:

Suppose you specify `druid.extensions.hadoopDependenciesDir=/usr/local/druid_tarball/hadoop-dependencies`, and you have downloaded 
`hadoop-client` 2.3.0 and 2.4.0.

Then underneath ```hadoop-dependencies```, it should look like this:

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

As you can see, under ```hadoop-client```, there are two sub-directories, each denotes a version of ```hadoop-client```. 

**Tell Druid what version of Hadoop to load**

Use `hadoopDependencyCoordinates` in [Hadoop Index Task](../ingestion/batch-ingestion.html) to specify the Hadoop dependencies you want Druid to load.

For example, in your Hadoop Index Task spec file, you have

`"hadoopDependencyCoordinates": ["org.apache.hadoop:hadoop-client:2.4.0"]`

This instructs Druid to load hadoop-client 2.4.0 when processing the task. What happens behind the scene is that Druid first looks for a folder 
called ```hadoop-client``` underneath `druid.extensions.hadoopDependenciesDir`, then looks for a folder called ```2.4.0``` 
underneath ```hadoop-client```, upon successfully locating these folders, hadoop-client 2.4.0 is loaded.

### Append your Hadoop jars to the Druid classpath

If you don't like the way above, and you just want to use one specific Hadoop version, and don't want Druid to work with different Hadoop versions, you can

(1) Set `druid.indexer.task.defaultHadoopCoordinates=[]`.  `druid.indexer.task.defaultHadoopCoordinates` specifies the default Hadoop coordinates that Druid uses. Its default value is `["org.apache.hadoop:hadoop-client:2.3.0"]`. By setting it to an empty list, Druid will not load any other Hadoop dependencies except the ones specified in the classpath.

(2) Append your Hadoop jars to the classpath, Druid will load them into the system. This mechanism is relatively easy to reason about, but it also means that you have to ensure that all dependency jars on the classpath are compatible. That is, Druid makes no provisions while using this method to maintain class loader isolation so you must make sure that the jars on your classpath are mutually compatible.

#### Hadoop 2.x

The default version of Hadoop bundled with Druid is 2.3.

To override the default Hadoop version, both the Hadoop Index Task and the standalone Hadoop indexer support the parameter `hadoopDependencyCoordinates`(See [Index Hadoop Task](../ingestion/tasks.html)). You can pass another set of Hadoop coordinates through this parameter (e.g. You can specify coordinates for Hadoop 2.4.0 as `["org.apache.hadoop:hadoop-client:2.4.0"]`), which will overwrite the default Hadoop coordinates Druid uses.

The Hadoop Index Task takes this parameter has part of the task JSON and the standalone Hadoop indexer takes this parameter as a command line argument.

If you are still having problems, include all relevant hadoop jars at the beginning of the classpath of your indexing or historical nodes.

#### CDH

Members of the community have reported dependency conflicts between the version of Jackson used in CDH and Druid when running a Mapreduce job like:
```
java.lang.VerifyError: class com.fasterxml.jackson.datatype.guava.deser.HostAndPortDeserializer overrides final method deserialize.(Lcom/fasterxml/jackson/core/JsonParser;Lcom/fasterxml/jackson/databind/DeserializationContext;)Ljava/lang/Object;
```

In order to use the Cloudera distribution of Hadoop, you must configure Mapreduce to
[favor Druid classpath over Hadoop]((https://hadoop.apache.org/docs/r2.7.1/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduce_Compatibility_Hadoop1_Hadoop2.html))
(i.e. use Jackson version provided with Druid).

This can be achieved by either:
- adding `"mapreduce.job.user.classpath.first": "true"` to the `jobProperties` property of the `tuningConfig` of your indexing task (see the [Job properties section for the Batch Ingestion using the HadoopDruidIndexer page](../ingestion/batch-ingestion.html)).
- configuring the Druid Middle Manager to add the following property when creating a new Peon: `druid.indexer.runner.javaOpts=... -Dhadoop.mapreduce.job.user.classpath.first=true`
- edit Druid's pom.xml dependencies to match the version of Jackson in your Hadoop version and recompile Druid

Members of the community have reported dependency conflicts between the version of Jackson used in CDH and Druid.

**Workaround - 1**

Currently, our best workaround is to edit Druid's pom.xml dependencies to match the version of Jackson in your Hadoop version and recompile Druid.

For more about building Druid, please see [Building Druid](../development/build.html).

**Workaround - 2**

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

**Workaround - 3**

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

## Working with Hadoop 1.x and older

We recommend recompiling Druid with your particular version of Hadoop by changing the dependencies in Druid's pom.xml files. Make sure to also either override the default `hadoopDependencyCoordinates` in the code or pass your Hadoop version in as part of indexing.
