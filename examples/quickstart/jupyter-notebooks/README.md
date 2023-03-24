# Jupyter Notebook tutorials for Druid

If you are reading this in Jupyter, switch over to the [0-START-HERE](0-START-HERE.ipynb)
notebook instead.

<!-- This README, the "0-START-HERE" notebook, and the tutorial-jupyter-index.md file in
docs/tutorials share a lot of the same content. If you make a change in one place, update
the other too. -->

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

You can try out the Druid APIs using the Jupyter Notebook-based tutorials. These
tutorials provide snippets of Python code that you can use to run calls against
the Druid API to complete the tutorial.

## Prerequisites

Make sure you meet the following requirements before starting the Jupyter-based tutorials:

- Python 3

- The `requests` package for Python. For example, you can install it with the following command:

  ```bash
  pip install requests
  ```

- JupyterLab (recommended) or Jupyter Notebook running on a non-default port. By default, Druid
  and Jupyter both try to use port `8888`, so start Jupyter on a different port.

  - Install JupyterLab or Notebook:

    ```bash
    # Install JupyterLab
    pip install jupyterlab
    # Install Jupyter Notebook
    pip install notebook
    ```
  - Start Jupyter using either JupyterLab
    ```bash
    # Start JupyterLab on port 3001
    jupyter lab --port 3001
    ```

    Or using Jupyter Notebook
    ```bash
    # Start Jupyter Notebook on port 3001
    jupyter notebook --port 3001
    ```

- The Python API client for Druid. Clone the Druid repo if you haven't already.
Go to your Druid source repo and install `druidapi` with the following commands:

  ```bash
  cd examples/quickstart/jupyter-notebooks/druidapi
  pip install .
  ```

- An available Druid instance. You can use the [quickstart deployment](https://druid.apache.org/docs/latest/tutorials/index.html).
  The tutorials assume that you are using the quickstart, so no authentication or authorization
  is expected unless explicitly mentioned.

  If you contribute to Druid, and work with Druid integration tests, can use a test cluster.
  Assume you have an environment variable, `DRUID_DEV`, which identifies your Druid source repo.

  ```bash
  cd $DRUID_DEV
  ./it.sh build
  ./it.sh image
  ./it.sh up <category>
  ```

  Replace `<catagory>` with one of the available integration test categories. See the integration 
  test `README.md` for details.

## Continue in Jupyter

Start Jupyter (see above) and navigate to the "0-START-HERE" notebook for more information.
