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

# Jupyter in Docker

TODO description

## Prerequisites

Jupyter in Docker requires that you have **Docker** and **Docker Compose**.
We recommend installing Docker Desktop, which includes Docker Compose along with Docker Engine and Docker CLI.

Clone the [apache/druid repo](https://github.com/apache/druid) and
navigate to `druid/examples/quickstart/jupyter-notebooks/docker-jupyter`.
Alternatively, you can download a local copy of
[`docker-compose.yaml`](https://github.com/apache/druid/blob/master/examples/quickstart/jupyter-notebooks/docker-jupyter/docker-compose.yaml) and [`environment`](https://github.com/apache/druid/blob/master/examples/quickstart/jupyter-notebooks/docker-jupyter/environment).

## Run Jupyter notebook on Docker

The `docker-compose.yaml` file can launch containers for Jupyter, Apache Druid, and Apache Kafka.
The Jupyter container includes prerequisite Python packages and the [Jupyter notebook tutorials for Druid](https://druid.apache.org/docs/latest/tutorials/tutorial-jupyter-index.html).

You can use the Docker Compose file to run the following combination of applications:
* Jupyter as a standalone application
* Jupyter and Druid
* Jupyter, Druid, and Kafka

### Start only the Jupyter container

In the same directory as `docker-compose.yaml`, start the application:

```
docker-compose --profile jupyter up -d
```

The port assigned to Jupyter is `8889` by default.
You can override the port number by setting the `JUPYTER_PORT` environment variable before starting the Docker application.

### Start Jupyter and Druid

Running Druid in Docker requires the `environment` file as well as the `DRUID_VERSION` environment variable.

In the same directory as `docker-compose.yaml` and `environment`, start the application:

```
DRUID_VERSION=25.0.0 docker-compose --profile druid-jupyter up -d
```

### Start Jupyter, Druid, and Apache Kafka

Running Druid in Docker requires the `environment` file as well as the `DRUID_VERSION` environment variable.

In the same directory as `docker-compose.yaml` and `environment`, start the application:

```
DRUID_VERSION=25.0.0 docker-compose --profile all-services up -d
```

## Access Jupyter-based tutorials

At startup, Docker creates and mounts a volume to persist data from the container to your local machine.
Access the files created in the Docker container in the `notebooks` folder in your local working directory.

1. Navigate to available notebooks at http://localhost:8889.
> If you set `JUPYTER_PORT` to another port number, replace `8889` with the value of the Jupyter port.

2. Select a tutorial.

3. From the navigation menu, select **File > Save as...**.
In the **Save As** dialog, enter `work/<notebook name>.ipynb`. This step allows you to retain a local copy of your work in the notebook. If the notebook still displays as read only, you may need to refresh the page in your browser.

## Stop Docker containers

Shut down the Docker application using the following command:

```
docker-compose down -v
```

## View the Druid web console

To access the Druid web console in Docker, go to http://localhost:8888/unified-console.html.
Use the web console to view datasources and ingestion tasks that you create in the tutorials.

## Contributing

You may want to update the Jupyter image to add prerequisite Python packages,
new tutorial notebooks, or configuration files.

To build the custom Jupyter image locally:

1. Clone the Druid repo if you haven't already.
2. Navigate to `examples/quickstart/jupyter-notebooks` in your Druid source repo.
3. Run the following command:

   ```
   docker build -t apache/druid:jupyter-notebook .
   ```

TODO how to update image on DockerHub
