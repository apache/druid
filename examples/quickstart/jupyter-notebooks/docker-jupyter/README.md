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

For details on getting started with Jupyter in Docker,
see [Docker for Jupyter Notebook tutorials](../../../../docs/tutorials/tutorial-jupyter-docker.md).

## Contributing

### Rebuild Jupyter image

You may want to update the Jupyter image to access new or updated tutorial notebooks,
include new Python packages, or update configuration files.

To build the custom Jupyter image locally:

1. Clone the Druid repo if you haven't already.
2. Navigate to `examples/quickstart/jupyter-notebooks` in your Druid source repo.
3. Edit the image definition in `Dockerfile`.
4. Navigate to the `docker-jupyter` directory.
5. Generate the new build using the following command:

   ```shell
   DRUID_VERSION=25.0.0 docker compose --profile all-services -f docker-compose-local.yaml up -d --build
   ```

   You can change the value of `DRUID_VERSION` or the profile used from the Docker Compose file.

### Update Docker Compose

The Docker Compose file defines a multi-container application that allows you to run
the custom Jupyter Notebook container, Apache Druid, and Apache Kafka.

Any changes to `docker-compose.yaml` should also be made to `docker-compose-local.yaml`
and vice versa. These files should be identical except that `docker-compose.yaml`
contains an `image` attribute while `docker-compose-local.yaml` contains a `build` subsection.

If you update `docker-compose.yaml`, recreate the ZIP file using the following command:

```bash
zip tutorial-jupyter-docker.zip docker-compose.yaml environment
```

