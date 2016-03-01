---
layout: doc_page
---
# Including Extensions

Druid uses a module system that allows for the addition of extensions at runtime. Core extensions are bundled with the Druid tarball. 
Community extensions be download locally via the [pull-deps](../operations/pull-deps.html) tool. 

## Download extensions

Core Druid extensions are already bundled in the Druid release tarball. You can get them by downloading the tarball at [druid.io](http://druid.io/downloads.html).
Unpack the tarball; You will see an ```extensions``` folder that contains all the core extensions, along with a ```hadoop-dependencies``` folder
where it contains all the hadoop extensions. Each extension will have its own folder that contains extension jars. However, because of licensing
we didn't package the mysql-metadata-storage extension in the extensions folder. In order to get it, you can download it from [druid.io](http://druid.io/downloads.html),
then unpack and move it into ```extensions``` directory.

Optionally, you can use the `pull-deps` tool to download extensions you want. 
See [pull-deps](../operations/pull-deps.html) for a complete example.

## Load extensions

There are two ways to let Druid load extensions.

### Load from classpath

If you add your extension jar to the classpath at runtime, Druid will load it into the system.  This mechanism is relatively easy to reason about, 
but it also means that you have to ensure that all dependency jars on the classpath are compatible.  That is, Druid makes no provisions while using 
this method to maintain class loader isolation so you must make sure that the jars on your classpath are mutually compatible.

### Load from extension directory

If you don't want to fiddle with classpath, you can tell Druid to load extensions from an extension directory.

To let Druid load your extensions, follow the steps below

**Tell Druid where your extensions are**

Specify `druid.extensions.directory` (the root directory that contains Druid extensions). See [Configuration](../configuration/index.html)

The value for this property should be set to the absolute path of the folder that contains all the extensions. 
In general, you should simply reuse the release tarball's extensions directory (i.e., ```extensions```).

Example:

Suppose you specify `druid.extensions.directory=/usr/local/druid_tarball/extensions`

Then underneath ```extensions```, it should look like this,

```
extensions/
├── druid-kafka-eight
│   ├── druid-kafka-eight-0.7.3.jar
│   ├── jline-0.9.94.jar
│   ├── jopt-simple-3.2.jar
│   ├── kafka-clients-0.8.2.1.jar
│   ├── kafka_2.10-0.8.2.1.jar
│   ├── log4j-1.2.16.jar
│   ├── lz4-1.3.0.jar
│   ├── metrics-core-2.2.0.jar
│   ├── netty-3.7.0.Final.jar
│   ├── scala-library-2.10.4.jar
│   ├── slf4j-log4j12-1.6.1.jar
│   ├── snappy-java-1.1.1.6.jar
│   ├── zkclient-0.3.jar
└── mysql-metadata-storage
    ├── mysql-connector-java-5.1.34.jar
    └── mysql-metadata-storage-0.9.0.jar
```

As you can see, underneath ```extensions``` there are two sub-directories ```druid-kafka-eight``` and ```mysql-metadata-storage```.
Each sub-directory denotes an extension that Druid could load.

**Tell Druid what extensions to load**

Use `druid.extensions.loadList`(See [Configuration](../configuration/index.html) ) to specify a
list of names of extensions that should be loaded by Druid.

For example, `druid.extensions.loadList=["druid-kafka-eight", "mysql-metadata-storage"]` instructs Druid to load `druid-kafka-eight` 
and `mysql-metdata-storage` extensions. That is, the name you specified in the list should be the same as its extension folder's name.

If you specify `druid.extensions.loadList=[]`, Druid won't load any extensions from the file system.

If you don't specify `druid.extensions.loadList`, Druid will load all the extensions under the directory specified by `druid.extensions.directory`.
