---
id: pull-deps
title: "pull-deps tool"
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


`pull-deps` is an Apache Druid (incubating) tool that can pull down dependencies to the local repository and lay dependencies out into the extension directory as needed.

`pull-deps` has several command line options, they are as follows:

`-c` or `--coordinate` (Can be specified multiply times)

Extension coordinate to pull down, followed by a maven coordinate, e.g. org.apache.druid.extensions:mysql-metadata-storage

`-h` or `--hadoop-coordinate` (Can be specified multiply times)

Apache Hadoop dependency to pull down, followed by a maven coordinate, e.g. org.apache.hadoop:hadoop-client:2.4.0

`--no-default-hadoop`

Don't pull down the default hadoop coordinate, i.e., org.apache.hadoop:hadoop-client:2.3.0. If `-h` option is supplied, then default hadoop coordinate will not be downloaded.

`--clean`

Remove existing extension and hadoop dependencies directories before pulling down dependencies.

`-l` or `--localRepository`

A local repository that Maven will use to put downloaded files. Then pull-deps will lay these files out into the extensions directory as needed.

`-r` or `--remoteRepository`

Add a remote repository. Unless --no-default-remote-repositories is provided, these will be used after https://repo1.maven.org/maven2/ and https://metamx.artifactoryonline.com/metamx/pub-libs-releases-local

`--no-default-remote-repositories`

Don't use the default remote repositories, only use the repositories provided directly via --remoteRepository.

`-d` or `--defaultVersion`

Version to use for extension coordinate that doesn't have a version information. For example, if extension coordinate is `org.apache.druid.extensions:mysql-metadata-storage`, and default version is `{{DRUIDVERSION}}`, then this coordinate will be treated as `org.apache.druid.extensions:mysql-metadata-storage:{{DRUIDVERSION}}`

`--use-proxy`

Use http/https proxy to send request to the remote repository servers. `--proxy-host` and `--proxy-port` must be set explicitly if this option is enabled.

`--proxy-type`

Set the proxy type, Should be either *http* or *https*, default value is *https*.

`--proxy-host`

Set the proxy host. e.g. proxy.com.

`--proxy-port`

Set the proxy port number. e.g. 8080.

`--proxy-username`

Set a username to connect to the proxy, this option is only required if the proxy server uses authentication.

`--proxy-password`

Set a password to connect to the proxy, this option is only required if the proxy server uses authentication.

To run `pull-deps`, you should

1) Specify `druid.extensions.directory` and `druid.extensions.hadoopDependenciesDir`, these two properties tell `pull-deps` where to put extensions. If you don't specify them,  default values will be used, see [Configuration](../configuration/index.md).

2) Tell `pull-deps` what to download using `-c` or `-h` option, which are followed by a maven coordinate.

Example:

Suppose you want to download ```mysql-metadata-storage``` and ```hadoop-client```(both 2.3.0 and 2.4.0) with a specific version, you can run `pull-deps` command with `-c org.apache.druid.extensions:mysql-metadata-storage:{{DRUIDVERSION}}`, `-h org.apache.hadoop:hadoop-client:2.3.0` and `-h org.apache.hadoop:hadoop-client:2.4.0`, an example command would be:

```
java -classpath "/my/druid/lib/*" org.apache.druid.cli.Main tools pull-deps --clean -c org.apache.druid.extensions:mysql-metadata-storage:{{DRUIDVERSION}} -h org.apache.hadoop:hadoop-client:2.3.0 -h org.apache.hadoop:hadoop-client:2.4.0
```

Because `--clean` is supplied, this command will first remove the directories specified at `druid.extensions.directory` and `druid.extensions.hadoopDependenciesDir`, then recreate them and start downloading the extensions there. After finishing downloading, if you go to the extension directories you specified, you will see

```
tree extensions
extensions
└── mysql-metadata-storage
    └── mysql-metadata-storage-{{DRUIDVERSION}}.jar
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

Note that if you specify `--defaultVersion`, you don't have to put version information in the coordinate. For example, if you want `mysql-metadata-storage` to use version `{{DRUIDVERSION}}`,  you can change the command above to

```
java -classpath "/my/druid/lib/*" org.apache.druid.cli.Main tools pull-deps --defaultVersion {{DRUIDVERSION}} --clean -c org.apache.druid.extensions:mysql-metadata-storage -h org.apache.hadoop:hadoop-client:2.3.0 -h org.apache.hadoop:hadoop-client:2.4.0
```

> Please note to use the pull-deps tool you must know the Maven groupId, artifactId, and version of your extension.
>
> For Druid community extensions listed [here](../development/extensions.md), the groupId is "org.apache.druid.extensions.contrib" and the artifactId is the name of the extension.
