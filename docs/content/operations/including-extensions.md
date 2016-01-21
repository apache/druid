---
layout: doc_page
---
# Including Extensions

Druid uses a module system that allows for the addition of extensions at runtime.

## Specifying extensions

Druid extensions can be specified in the `common.runtime.properties`. There are two ways of adding druid extensions currently.

### Add to the classpath

If you add your extension jar to the classpath at runtime, Druid will load it into the system.  This mechanism is relatively easy to reason about, but it also means that you have to ensure that all dependency jars on the classpath are compatible.  That is, Druid makes no provisions while using this method to maintain class loader isolation so you must make sure that the jars on your classpath are mutually compatible.

### Add to the extension directory

If you don't want to fiddle with classpath, you can create an extension directory and tell Druid to load extensions from there.

To let Druid load your extensions, follow the steps below

1) Specify `druid.extensions.directory` (root directory for normal Druid extensions). If you don't specify it, Druid will use the default value, see [Configuration](../configuration/index.html).

2) Under the root extension directory, create sub-directories for each extension you might want to load.  Inside each sub-directory, put extension-related files.  (If you don't want to manually set up the extension directory, you can use Druid's [pull-deps](../pull-deps.html) tool to help you generate these directories automatically.)

Example:

Suppose you specify `druid.extensions.directory=/usr/local/druid/extensions`, and want Druid to load normal extensions ```druid-examples```, ```druid-kafka-eight``` and ```mysql-metadata-storage```.

Then under ```extensions```, it should look like this,

```
extensions/
├── druid-examples
│   ├── commons-beanutils-1.8.3.jar
│   ├── commons-digester-1.8.jar
│   ├── commons-logging-1.1.1.jar
│   ├── commons-validator-1.4.0.jar
│   ├── druid-examples-0.8.0-rc1.jar
│   ├── twitter4j-async-3.0.3.jar
│   ├── twitter4j-core-3.0.3.jar
│   └── twitter4j-stream-3.0.3.jar
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
│   └── zookeeper-3.4.7.jar
└── mysql-metadata-storage
    ├── jdbi-2.32.jar
    ├── mysql-connector-java-5.1.34.jar
    └── mysql-metadata-storage-0.8.0-rc1.jar
```

As you can see, under ```extensions``` there are three sub-directories ```druid-examples```, ```druid-kafka-eight``` and ```mysql-metadata-storage```, each sub-directory denotes an extension that Druid might load.

3) To have Druid load a specific list of extensions present under the root extension directory, set `druid.extensions.loadList` to the list of extensions to load. Using the example above, if you want Druid to load ```druid-kafka-eight``` and ```mysql-metadata-storage```, you can specify `druid.extensions.loadList=["druid-kafka-eight", "mysql-metadata-storage"]`.

If you specify `druid.extensions.loadList=[]`, Druid won't load any extensions from the file system.

If you don't specify `druid.extensions.loadList`, Druid will load all the extensions under the root extension directory.
