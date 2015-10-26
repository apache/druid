---
layout: doc_page
---
# Work with different versions of Hadoop

## Include Hadoop dependencies

There are two different ways to let Druid pick up your Hadoop version, choose the one that fits your need.

### Add your Hadoop dependencies to the Hadoop dependencies directory

You can create a Hadoop dependency directory and tell Druid to load your Hadoop jars from there.

To make this work, follow the steps below

(1) Specify `druid.extensions.hadoopDependenciesDir` (root directory for Hadoop related dependencies). If you don't specify it, Druid will use its default value, see [Configuration](../configuration/index.html).

(2) Set-up Hadoop dependencies directories under root Hadoop dependency directory. Under the root directory, you should create sub-directories for each Hadoop dependencies.  Inside each sub-directory, created a sub-sub-directory whose name is the version of Hadoop it contains, and inside that sub-sub-directory, put Hadoop jars in it. This file structure is almost same as normal Druid extensions described in [Including-Extensions](../including-extensions.html), except that there is an extra layer of folder that specifies the version of Hadoop. (If you don't want to manually setup this directory, Druid also provides a [pull-deps](../pull-deps.html) tool that can help you generate these directories automatically)

Example:

Suppose you specify `druid.extensions.hadoopDependenciesDir=/usr/local/druid/hadoop_druid_dependencies`, and you want to prepare both `hadoop-client` 2.3.0 and 2.4.0 for Druid,

Then you can either use [pull-deps](../pull-deps.html) or manually set up Hadoop dependencies directories such that under ```hadoop_druid_dependencies```, it looks like this,

```
hadoop_druid_dependencies/
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

As you can see, under ```hadoop-client```, there are two sub-directories, each denotes a version of ```hadoop-client```. During runtime, Druid will look for these directories and load appropriate ```hadoop-client``` based on `hadoopDependencyCoordinates` passed to [Hadoop Index Task](../misc/tasks.html).

### Append your Hadoop jars to the Druid classpath

If you really don't like the way above, and you just want to use one specific Hadoop version, and don't want Druid to work with different Hadoop versions, then you can

(1) Set `druid.indexer.task.defaultHadoopCoordinates=[]`.  `druid.indexer.task.defaultHadoopCoordinates` specifies the default Hadoop coordinates that Druid uses. Its default value is `["org.apache.hadoop:hadoop-client:2.3.0"]`. By setting it to an empty list, Druid will not load any other Hadoop dependencies except the ones specified in the classpath.

(2) Append your Hadoop jars to the classpath, Druid will load them into the system. This mechanism is relatively easy to reason about, but it also means that you have to ensure that all dependency jars on the classpath are compatible. That is, Druid makes no provisions while using this method to maintain class loader isolation so you must make sure that the jars on your classpath are mutually compatible.

## Working with Hadoop 2.x

The default version of Hadoop bundled with Druid is 2.3.

To override the default Hadoop version, both the Hadoop Index Task and the standalone Hadoop indexer support the parameter `hadoopDependencyCoordinates`(See [Index Hadoop Task](../misc/tasks.html). You can pass another set of Hadoop coordinates through this parameter (e.g. You can specify coordinates for Hadoop 2.4.0 as `["org.apache.hadoop:hadoop-client:2.4.0"]`), which will overwrite the default Hadoop coordinates Druid uses.

The Hadoop Index Task takes this parameter has part of the task JSON and the standalone Hadoop indexer takes this parameter as a command line argument.

If you are still having problems, include all relevant hadoop jars at the beginning of the classpath of your indexing or historical nodes.

## Working with CDH

Members of the community have reported dependency conflicts between the version of Jackson used in CDH and Druid. Currently, our best workaround is to edit Druid's pom.xml dependencies to match the version of Jackson in your Hadoop version and recompile Druid.

For more about building Druid, please see [Building Druid](../development/build.html).

Another workaround solution is to build a custom fat jar of Druid using [sbt](http://www.scala-sbt.org/), which manually excludes all the conflicting Jackson dependencies, and then put this fat jar in the classpath of the command that starts overlord indexing service. To do this, please follow the following steps.

(1) Download and install sbt.

(2) Make a new directory named 'druid_build'.

(3) Cd to 'druid_build' and create the build.sbt file with the content [here](./use_sbt_to_build_fat_jar.md).

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
