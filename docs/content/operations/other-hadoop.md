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

Members of the community have reported dependency conflicts between the version of Jackson used in CDH and Druid. Currently, our best workaround is to edit Druid's pom.xml dependencies to match the version of Jackson in your Hadoop version and recompile Druid.

For more about building Druid, please see [Building Druid](../development/build.html).

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

## Working with Hadoop 1.x and older

We recommend recompiling Druid with your particular version of Hadoop by changing the dependencies in Druid's pom.xml files. Make sure to also either override the default `hadoopDependencyCoordinates` in the code or pass your Hadoop version in as part of indexing.
