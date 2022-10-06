---
id: docker
title: "Tutorial: Run with Docker"
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

This quickstart guides you through the steps to download the Apache Druid image from [Docker Hub](https://hub.docker.com/r/apache/druid) and deploy it on a single machine using [Docker](https://www.docker.com/get-started) and [Docker Compose](https://docs.docker.com/compose/).
After you finish the initial setup, the cluster will be ready to load data.

Before beginning the quickstart, it is helpful to read the [general Druid overview](../design/index.md) and the [ingestion overview](../ingestion/index.md), because the tutorials refer to concepts discussed on those pages. It also helps to be familiar with [Docker](https://www.docker.com/get-started).

This tutorial assumes you will download the required files from GitHub. The files are also available in a Druid installation and in the Druid sources.

## Prerequisites

* [Docker](https://www.docker.com/get-started)

### Docker memory requirements

The default `docker-compose.yml` launches eight containers: Zookeeper, PostgreSQL, and six Druid containers based upon the [micro quickstart configuration](../operations/single-server.html#micro-quickstart-4-cpu-16gib-ram).
Each Druid service is configured to use up to 7 GiB of memory (6 GiB direct memory and 1 GiB heap). However, the quickstart will not use all the available memory.

For this setup, Docker needs at least 6 GiB of memory available for the Druid cluster. For Docker Desktop on Mac OS, adjust the memory settings in the [Docker Desktop preferences](https://docs.docker.com/desktop/mac/). If you experience a crash with a 137 error code you likely don't have enough memory allocated to Docker.

You can modify the value of `DRUID_SINGLE_NODE_CONF` in the Docker [`environment`](#environment-file) to use different single-server mode. For example to use the nano quickstart: `DRUID_SINGLE_NODE_CONF=nano-quickstart`.


## Getting started

Create a directory to hold the Druid Docker files.

The Druid source code contains [an example `docker-compose.yml`](https://github.com/apache/druid/blob/{{DRUIDVERSION}}/distribution/docker/docker-compose.yml) which pulls an image from Docker Hub and is suited to be used as an example environment and to experiment with Docker based Druid configuration and deployments. [Download](https://raw.githubusercontent.com/apache/druid/{{DRUIDVERSION}}/distribution/docker/docker-compose.yml) this file to the directory created above.

### Compose file

The example `docker-compose.yml` will create a container for each Druid service, as well as ZooKeeper and a PostgreSQL container as the metadata store.

It will also create a named volume `druid_shared` as deep storage to keep and share segments and task logs among Druid services. The volume is mounted as `opt/shared` in the container.

### Environment file

The Druid `docker-compose.yml` example uses an [environment file](https://docs.docker.com/compose/environment-variables/#the-env_file-configuration-option) to specify the complete Druid configuration, including the environment variables described in [Configuration](#configuration). This file is named `environment` by default, and must be in the same directory as the `docker-compose.yml` file. [Download](https://raw.githubusercontent.com/apache/druid/{{DRUIDVERSION}}/distribution/docker/environment) the example `environment` file to the directory created above. The options in this file work well for trying Druid and for using the tutorial.

The single-file approach is inadequate for a production system. Instead we suggest using either `DRUID_COMMON_CONFIG` and `DRUID_CONFIG_${service}` or specially tailored, service-specific environment files.

### Configuration

Configuration of the Druid Docker container is done via environment variables set within the container. Docker Compose passes the values from the `environment file` into the container. The variables may additionally specify paths to [the standard Druid configuration files](../configuration/index.md) which must be available within the container.

The default values are fine for the Quickstart. Production systems will want to modify the defaults.

Basic configuration:

* `DRUID_MAXDIRECTMEMORYSIZE` -- set Java max direct memory size. Default is 6 GiB.
* `DRUID_XMX` -- set Java `Xmx`, the maximum heap size. Default is 1 GB.

Production configuration:

* `DRUID_CONFIG_COMMON` -- full path to a file for Druid common properties
* `DRUID_CONFIG_${service}` -- full path to a file for Druid service properties
* `JAVA_OPTS` -- set Java options

Logging configuration:

* `DRUID_LOG4J` -- set the entire [`log4j.xml` configuration file](https://logging.apache.org/log4j/2.x/manual/configuration.html#XML)  verbatim. ([Example](https://github.com/apache/druid/blob/{{DRUIDVERSION}}/distribution/docker/environment#L52))
* `DRUID_LOG_LEVEL` -- override the default [Log4j log level](https://en.wikipedia.org/wiki/Log4j#Log4j_log_levels)
* `DRUID_SERVICE_LOG4J` -- set the entire [`log4j.xml` configuration file](https://logging.apache.org/log4j/2.x/manual/configuration.html#XML)  verbatim specific to a service.
* `DRUID_SERVICE_LOG_LEVEL` -- override the default [Log4j log level](https://en.wikipedia.org/wiki/Log4j#Log4j_log_levels) in the service specific log4j.

Advanced memory configuration:

* `DRUID_XMS` -- set Java [`Xms`](https://docs.oracle.com/cd/E19900-01/819-4742/abeik/index.html), the initial heap size. Default is 1 GB.
* `DRUID_MAXNEWSIZE` -- set [Java max new size](https://docs.oracle.com/cd/E19900-01/819-4742/abeik/index.html)
* `DRUID_NEWSIZE` -- set [Java new size](https://docs.oracle.com/cd/E19900-01/819-4742/abeik/index.html)

In addition to the special environment variables, the script which launches Druid in the container will use any environment variable starting with the `druid_` prefix as command-line configuration. For example, an environment variable

`druid_metadata_storage_type=postgresql`

is translated into the following option in the Java launch command for the Druid process in the container:

`-Ddruid.metadata.storage.type=postgresql`

Note that Druid uses port 8888 for the console. This port is also used by Jupyter and other tools. To avoid conflicts, you can change the port in the [`ports`](https://github.com/apache/druid/blob/0.21.1/distribution/docker/docker-compose.yml#L125) section of the `docker-compose.yml` file. For example, to expose the console on port 9999 of the host:

```yaml
    container_name: router
    ...
    ports:
      - "9999:8888"
```

## Launching the cluster

`cd` into the directory that contains the configuration files. This is the directory you created above, or the `distribution/docker/` in your Druid installation directory if you installed Druid locally.

Run `docker-compose up` to launch the cluster with a shell attached, or `docker-compose up -d` to run the cluster in the background.

Once the cluster has started, you can navigate to the [web console](../operations/web-console.md) at [http://localhost:8888](http://localhost:8888). The [Druid router process](../design/router.md) serves the UI.

![web console](../assets/tutorial-quickstart-01.png "web console")

It takes a few seconds for all the Druid processes to fully start up. If you open the console immediately after starting the services, you may see some errors that you can safely ignore.

## Using the cluster

From here you can follow along with the [Quickstart](./index.md#load-data). For production use, refine your `docker-compose.yml` file to add any additional external service dependencies as necessary.

You can explore the Druid containers using Docker to start a shell:

```sh
docker exec -ti <id> sh
```

Where `<id>` is the container id found with `docker ps`. Druid is installed in `/opt/druid`. The [script](https://github.com/apache/druid/blob/{{DRUIDVERSION}}/distribution/docker/druid.sh) which consumes the environment variables mentioned above, and which launches Druid, is located at `/druid.sh`.

Run `docker-compose down` to shut down the cluster. Your data is persisted as a set of [Docker volumes](https://docs.docker.com/storage/volumes/) and will be available when you restart your Druid cluster.

