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
see [Jupyter Notebook tutorials](../../../../docs/tutorials/tutorial-jupyter-index.md).

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
