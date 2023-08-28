---
id: tutorial-jupyter-docker
title: "Docker for Jupyter Notebook tutorials"
sidebar_label: "Docker for tutorials"
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


Apache Druid provides a custom Jupyter container that contains the prerequisites
for all [Jupyter-based Druid tutorials](tutorial-jupyter-index.md), as well as all of the tutorials themselves.
You can run the Jupyter container, as well as containers for Druid and Apache Kafka,
using the Docker Compose file provided in the Druid GitHub repository.

You can run the following combination of applications:
* [Jupyter only](#start-only-the-jupyter-container)
* [Jupyter and Druid](#start-jupyter-and-druid)
* [Jupyter, Druid, and Kafka](#start-jupyter-druid-and-kafka)
* [Kafka and Jupyter](#start-kafka-and-jupyter)

## Prerequisites

Jupyter in Docker requires that you have **Docker** and **Docker Compose**.
We recommend installing these through [Docker Desktop](https://docs.docker.com/desktop/).

For ARM-based devices, see [Tutorial setup for ARM-based devices](#tutorial-setup-for-arm-based-devices).

## Launch the Docker containers

You run Docker Compose to launch Jupyter and optionally Druid or Kafka.
Docker Compose references the configuration in `docker-compose.yaml`.
Running Druid in Docker also requires the `environment` file, which
sets the configuration properties for the Druid services.
To get started, download both `docker-compose.yaml` and `environment` from
[`tutorial-jupyter-docker.zip`](https://github.com/apache/druid/blob/master/examples/quickstart/jupyter-notebooks/docker-jupyter/tutorial-jupyter-docker.zip).

Alternatively, you can clone the [Apache Druid repo](https://github.com/apache/druid) and
access the files in `druid/examples/quickstart/jupyter-notebooks/docker-jupyter`.

### Start only the Jupyter container

If you already have Druid running locally or on another machine, you can run the Docker containers for Jupyter only.
In the same directory as `docker-compose.yaml`, start the application:

```bash
docker compose --profile jupyter up -d
```

The Docker Compose file assigns `8889` for the Jupyter port.
You can override the port number by setting the `JUPYTER_PORT` environment variable before starting the Docker application.

If Druid is running local to the same machine as Jupyter, open the tutorial and set the `host` variable to `host.docker.internal` before starting. For example:
```python
host = "host.docker.internal"
```

### Start Jupyter and Druid

Running Druid in Docker requires the `environment` file as well as an environment variable named `DRUID_VERSION`,
which determines the version of Druid to use. The Druid version references the Docker tag to pull from the
[Apache Druid Docker Hub](https://hub.docker.com/r/apache/druid/tags).

In the same directory as `docker-compose.yaml` and `environment`, start the application:

```bash
DRUID_VERSION={{DRUIDVERSION}} docker compose --profile druid-jupyter up -d
```

### Start Jupyter, Druid, and Kafka

Running Druid in Docker requires the `environment` file as well as the `DRUID_VERSION` environment variable.

In the same directory as `docker-compose.yaml` and `environment`, start the application:

```bash
DRUID_VERSION={{DRUIDVERSION}} docker compose --profile all-services up -d
```

### Start Kafka and Jupyter

If you already have Druid running externally, such as an existing cluster or a dedicated infrastructure for Druid, you can run the Docker containers for Kafka and Jupyter only.

In the same directory as `docker-compose.yaml` and `environment`, start the application:

```bash
DRUID_VERSION={{DRUIDVERSION}} docker compose --profile kafka-jupyter up -d
```

If you have an external Druid instance running on a different machine than the one hosting the Docker Compose environment, change the `host` variable in the notebook tutorial to the hostname or address of the machine where Druid is running.

If Druid is running local to the same machine as Jupyter, open the tutorial and set the `host` variable to `host.docker.internal` before starting. For example:

```python
host = "host.docker.internal"
```

To enable Druid to ingest data from Kafka within the Docker Compose environment, update the `bootstrap.servers` property in the Kafka ingestion spec to `localhost:9094` before ingesting. For reference, see [Consumer properties](../development/extensions-core/kafka-supervisor-reference.md#consumer-properties).

### Update image from Docker Hub

If you already have a local cache of the Jupyter image, you can update the image before running the application using the following command:

```bash
docker compose pull jupyter
```

### Use locally built image

The default Docker Compose file pulls the custom Jupyter Notebook image from a third party Docker Hub.
If you prefer to build the image locally from the official source, do the following:
1. Clone the Apache Druid repository.
2. Navigate to `examples/quickstart/jupyter-notebooks/docker-jupyter`.
3. Start the services using `-f docker-compose-local.yaml` in the `docker compose` command. For example:

```bash
DRUID_VERSION={{DRUIDVERSION}} docker compose --profile all-services -f docker-compose-local.yaml up -d
```

## Access Jupyter-based tutorials

The following steps show you how to access the Jupyter notebook tutorials from the Docker container.
At startup, Docker creates and mounts a volume to persist data from the container to your local machine.
This way you can save your work completed within the Docker container.

1. Navigate to the notebooks at http://localhost:8889.
:::info
 If you set `JUPYTER_PORT` to another port number, replace `8889` with the value of the Jupyter port.
:::

2. Select a tutorial. If you don't plan to save your changes, you can use the notebook directly as is. Otherwise, continue to the next step.

3. Optional: To save a local copy of your tutorial work,
select **File > Save as...** from the navigation menu. Then enter `work/<notebook name>.ipynb`.
If the notebook still displays as read only, you may need to refresh the page in your browser.
Access the saved files in the `notebooks` folder in your local working directory.

## View the Druid web console

To access the Druid web console in Docker, go to http://localhost:8888/unified-console.html.
Use the web console to view datasources and ingestion tasks that you create in the tutorials.

## Stop Docker containers

Shut down the Docker application using the following command:

```bash
docker compose down -v
```

## Tutorial setup without using Docker

To use the Jupyter Notebook-based tutorials without using Docker, do the following:

1. Clone the Apache Druid repo, or download the [tutorials](tutorial-jupyter-index.md#tutorials)
as well as the [Python client for Druid](tutorial-jupyter-index.md#python-api-for-druid).

2. Install the prerequisite Python packages with the following commands:

   ```bash
   # Install requests
   pip install requests
   ```

   ```bash
   # Install JupyterLab
   pip install jupyterlab
   
   # Install Jupyter Notebook
   pip install notebook
   ```

   Individual notebooks may list additional packages you need to install to complete the tutorial.

3. In your Druid source repo, install `druidapi` with the following commands:

   ```bash
   cd examples/quickstart/jupyter-notebooks/druidapi
   pip install .
   ```

4. Start Jupyter, in the same directory as the tutorials, using either JupyterLab or Jupyter Notebook:
   ```bash
   # Start JupyterLab on port 3001
   jupyter lab --port 3001

   # Start Jupyter Notebook on port 3001
   jupyter notebook --port 3001
   ```

5. Start Druid. You can use the [Quickstart (local)](./index.md) instance. The tutorials
   assume that you are using the quickstart, so no authentication or authorization
   is expected unless explicitly mentioned.

   If you contribute to Druid, and work with Druid integration tests, you can use a test cluster.
   Assume you have an environment variable, `DRUID_DEV`, which identifies your Druid source repo.
 
   ```bash
   cd $DRUID_DEV
   ./it.sh build
   ./it.sh image
   ./it.sh up <category>
   ```
 
   Replace `<category>` with one of the available integration test categories. See the integration
   test `README.md` for details.

You should now be able to access and complete the tutorials.

## Tutorial setup for ARM-based devices

For ARM-based devices, follow this setup to start Druid externally, while keeping Kafka and Jupyter within the Docker Compose environment:

1. Start Druid using the `start-druid` script. You can follow [Quickstart (local)](./index.md) instructions. The tutorials
   assume that you are using the quickstart, so no authentication or authorization is expected unless explicitly mentioned.
2. Start either Jupyter only or Jupyter and Kafka using the following commands in the same directory as `docker-compose.yaml` and `environment`:
   
   ```bash
   # Start only Jupyter
   docker compose --profile jupyter up -d
   
   # Start Kafka and Jupyter
   DRUID_VERSION={{DRUIDVERSION}} docker compose --profile kafka-jupyter up -d
   ```

3. If Druid is running local to the same machine as Jupyter, open the tutorial and set the `host` variable to `host.docker.internal` before starting. For example:
   ```python
   host = "host.docker.internal"
   ```
4. If using Kafka to handle the data stream that will be ingested into Druid and Druid is running local to the same machine, update the consumer property `bootstrap.servers` to `localhost:9094`.

## Learn more

See the following topics for more information:
* [Jupyter Notebook tutorials](tutorial-jupyter-index.md) for the available Jupyter Notebook-based tutorials for Druid
* [Tutorial: Run with Docker](docker.md) for running Druid from a Docker container