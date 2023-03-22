---
id: tutorial-jupyter-index
title: "Jupyter Notebook tutorials"
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

<!-- tutorial-jupyter-index.md and examples/quickstart/juptyer-notebooks/README.md
    share a lot of the same content. If you make a change in one place, update the other
    too. -->

You can try out the Druid APIs using the Jupyter Notebook-based tutorials. These
tutorials provide snippets of Python code that you can use to run calls against
the Druid API to complete the tutorial.

## Prerequisites

The Jupyter-based tutorials require the following:

- An available Druid instance.
- Python 3.7 or later
- The `requests` package for Python
- JupyterLab (recommended) or Jupyter Notebook running on a non-default port.
By default, Druid and Jupyter both try to use port `8888`, so start Jupyter on a different port.

Individual tutorials may require additional Python packages, such as for visualization or streaming ingestion.

### Run Jupyter and Druid in Docker

> Jupyter in Docker requires that you have **Docker** and **Docker Compose**.
We recommend installing [Docker Desktop](https://docs.docker.com/desktop/),
which includes Docker Compose along with Docker Engine and Docker CLI.

Apache Druid provides a custom Jupyter container that contains the prerequisites
for all Jupyter-based Druid tutorials, as well as all of the tutorials themselves.
You can run the Jupyter container, as well as containers for Druid and Apache Kafka,
using the Docker Compose file provided in the Druid GitHub repository.

You can run the following combination of applications:
* [Jupyter only](#start-only-the-jupyter-container)
* [Jupyter and Druid](#start-jupyter-and-druid)
* [Jupyter, Druid, and Kafka](#start-jupyter-druid-and-kafka)

Clone the [apache/druid repo](https://github.com/apache/druid) and
navigate to `druid/examples/quickstart/jupyter-notebooks/docker-jupyter`.
Alternatively, you can download a local copy of
[`docker-compose.yaml`](https://github.com/apache/druid/blob/master/examples/quickstart/jupyter-notebooks/docker-jupyter/docker-compose.yaml) and [`environment`](https://github.com/apache/druid/blob/master/examples/quickstart/jupyter-notebooks/docker-jupyter/environment).

#### Start only the Jupyter container

If you already have Druid running locally, you can run only the Jupyter container to complete the tutorials.
In the same directory as `docker-compose.yaml`, start the application:

```bash
docker-compose --profile jupyter up -d
```

The port assigned to Jupyter is `8889` by default.
You can override the port number by setting the `JUPYTER_PORT` environment variable before starting the Docker application.

#### Start Jupyter and Druid

Running Druid in Docker requires the `environment` file as well as the `DRUID_VERSION` environment variable.

In the same directory as `docker-compose.yaml` and `environment`, start the application:

```bash
DRUID_VERSION=25.0.0 docker-compose --profile druid-jupyter up -d
```

#### Start Jupyter, Druid, and Kafka

Running Druid in Docker requires the `environment` file as well as the `DRUID_VERSION` environment variable.

In the same directory as `docker-compose.yaml` and `environment`, start the application:

```bash
DRUID_VERSION=25.0.0 docker-compose --profile all-services up -d
```

#### Access Jupyter-based tutorials

At startup, Docker creates and mounts a volume to persist data from the container to your local machine.
Access the files created in the Docker container in the `notebooks` folder in your local working directory.

1. Navigate to available notebooks at http://localhost:8889.
   > If you set `JUPYTER_PORT` to another port number, replace `8889` with the value of the Jupyter port.

2. Select a tutorial.

3. From the navigation menu, select **File > Save as...**.
In the **Save As** dialog, enter `work/<notebook name>.ipynb`. This step allows you to retain a local copy of your work in the notebook. If the notebook still displays as read only, you may need to refresh the page in your browser.

#### View the Druid web console

To access the Druid web console in Docker, go to http://localhost:8888/unified-console.html.
Use the web console to view datasources and ingestion tasks that you create in the tutorials.

#### Stop Docker containers

Shut down the Docker application using the following command:

```bash
docker-compose down -v
```

### Run Jupyter and Druid as standalone applications

1. Install the prerequisite Python packages with the following commands:

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

2. Start Jupyter using either JupyterLab or Jupyter Notebook:
   ```bash
   # Start JupyterLab on port 3001
   jupyter lab --port 3001

   # Start Jupyter Notebook on port 3001
   jupyter notebook --port 3001
   ```

3. Start Druid. You can use the [Quickstart (local)](./index.md) instance. The tutorials
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

## Python API for Druid

One of the notebooks shows how to use the Druid REST API. The others focus on other
topics and use a simple set of Python wrappers around the underlying REST API. The
wrappers reside in the `druidapi` package within the notebooks directory. While the package
can be used in any Python program, the key purpose, at present, is to support these
notebooks. See the
[Introduction to the Druid Python API](https://github.com/apache/druid/tree/master/examples/quickstart/jupyter-notebooks/python-api-tutorial.ipynb)
for an overview of the Python API.

## Tutorials

The notebooks are located in the [apache/druid repo](https://github.com/apache/druid/tree/master/examples/quickstart/jupyter-notebooks/). You can either clone the repo or download the notebooks you want individually.

The links that follow are the raw GitHub URLs, so you can use them to download the notebook directly, such as with `wget`, or manually through your web browser. Note that if you save the file from your web browser, make sure to remove the `.txt` extension.

- [Introduction to the Druid REST API](
  https://raw.githubusercontent.com/apache/druid/master/examples/quickstart/jupyter-notebooks/api-tutorial.ipynb)
  walks you through some of the basics related to the Druid REST API and several endpoints.
- [Introduction to the Druid Python API](
  https://raw.githubusercontent.com/apache/druid/master/examples/quickstart/jupyter-notebooks/Python_API_Tutorial.ipynb)
  walks you through some of the basics related to the Druid API using the Python wrapper API.
- [Introduction to Druid SQL](https://raw.githubusercontent.com/apache/druid/master/examples/quickstart/jupyter-notebooks/sql-tutorial.ipynb) covers the basics of Druid SQL.
