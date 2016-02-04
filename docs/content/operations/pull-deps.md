---
layout: doc_page
---
# pull-deps Tool

`pull-deps` is a tool that can pull down dependencies to the local repository and lay dependencies out into the extension directory as needed.

`pull-deps` has several command line options, they are as follows:

`-c` or `--coordinate` (Can be specified multiply times)

    Extension coordinate to pull down, followed by a maven coordinate, e.g. io.druid.extensions:mysql-metadata-storage

`-h` or `--hadoop-coordinate` (Can be specified multiply times)

    Hadoop dependency to pull down, followed by a maven coordinate, e.g. org.apache.hadoop:hadoop-client:2.4.0

`--no-default-hadoop`

    Don't pull down the default hadoop coordinate, i.e., org.apache.hadoop:hadoop-client:2.3.0. If `-h` option is supplied, then default hadoop coordinate will not be downloaded.

`--clean`
    
    Remove exisiting extension and hadoop dependencies directories before pulling down dependencies.

`-l` or `--localRepository`

    A local repostiry that Maven will use to put downloaded files. Then pull-deps will lay these files out into the extensions directory as needed.

`-r` or `--remoteRepository`

    Add a remote repository to the default remote repository list, which includes https://repo1.maven.org/maven2/ and https://metamx.artifactoryonline.com/metamx/pub-libs-releases-local

`-d` or `--defaultVersion`

    Version to use for extension coordinate that doesn't have a version information. For example, if extension coordinate is `io.druid.extensions:mysql-metadata-storage`, and default version is `0.8.0`, then this coordinate will be treated as `io.druid.extensions:mysql-metadata-storage:0.8.0`

To run `pull-deps`, you should

1) Specify `druid.extensions.directory` and `druid.extensions.hadoopDependenciesDir`, these two properties tell `pull-deps` where to put extensions. If you don't specify them,  default values will be used, see [Configuration](../configuration/index.html).

2) Tell `pull-deps` what to download using `-c` or `-h` option, which are followed by a maven coordinate.

Example:

Suppose you want to download ```druid-examples```, ```mysql-metadata-storage``` and ```hadoop-client```(both 2.3.0 and 2.4.0) with a specific version, you can run `pull-deps` command with `-c io.druid.extensions:druid-examples:0.8.0`, `-c io.druid.extensions:mysql-metadata-storage:0.8.0`, `-h org.apache.hadoop:hadoop-client:2.3.0` and `-h org.apache.hadoop:hadoop-client:2.4.0`, an example command would be:

```java -classpath "/my/druid/library/*" io.druid.cli.Main tools pull-deps --clean -c io.druid.extensions:mysql-metadata-storage:0.8.0 -c io.druid.extensions:druid-examples:0.8.0 -h org.apache.hadoop:hadoop-client:2.3.0 -h org.apache.hadoop:hadoop-client:2.4.0```

Because `--clean` is supplied, this command will first remove the directories specified at `druid.extensions.directory` and `druid.extensions.hadoopDependenciesDir`, then recreate them and start downloading the extensions there. After finishing downloading, if you go to the extension directories you specified, you will see

```
tree extensions
extensions
├── druid-examples
│   ├── commons-beanutils-1.8.3.jar
│   ├── commons-digester-1.8.jar
│   ├── commons-logging-1.1.1.jar
│   ├── commons-validator-1.4.0.jar
│   ├── druid-examples-0.8.0.jar
│   ├── twitter4j-async-3.0.3.jar
│   ├── twitter4j-core-3.0.3.jar
│   └── twitter4j-stream-3.0.3.jar
└── mysql-metadata-storage
    ├── jdbi-2.32.jar
    ├── mysql-connector-java-5.1.34.jar
    └── mysql-metadata-storage-0.8.0.jar
```

```
tree hadoop-dependencies
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

Note that if you specify `--defaultVersion`, you don't have to put version information in the coordinate. For example, if you want both `druid-examples` and `mysql-metadata-storage` to use version `0.8.0`,  you can change the command above to

```java -classpath "/my/druid/library/*" io.druid.cli.Main tools pull-deps --defaultVersion 0.8.0 --clean -c io.druid.extensions:mysql-metadata-storage -c io.druid.extensions:druid-examples -h org.apache.hadoop:hadoop-client:2.3.0 -h org.apache.hadoop:hadoop-client:2.4.0```
