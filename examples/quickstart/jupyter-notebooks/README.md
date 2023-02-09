# Jupyter Notebook tutorials for Druid

<!-- This README and the tutorial-jupyter-index.md file in docs/tutorials share a lot of the same content. If you make a change in one place, update the other too. -->

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

You can try out the Druid APIs using the Jupyter Notebook-based tutorials. These tutorials provide snippets of Python code that you can use to run calls against the Druid API to complete the tutorial.

## Prerequisites

Make sure you meet the following requirements before starting the Jupyter-based tutorials:

- Python 3 

- The `requests` package for Python. For example, you can install it with the following command: 
   
   ```bash
   pip3 install requests
   ````

- JupyterLab (recommended) or Jupyter Notebook running on a non-default port. By default, Druid and Jupyter both try to use port `8888,` so start Jupyter on a different port.

  - Install JupyterLab or Notebook:
  
     ```bash
    # Install JupyterLab
    pip3 install jupyterlab  
    # Install Jupyter Notebook
    pip3 install notebook
     ```
     
  -  Start Jupyter:
      -  JupyterLab 
         ```bash
         # Start JupyterLab on port 3001
         jupyter lab --port 3001
         ```
      - Jupyter Notebook
        ```bash
        # Start Jupyter Notebook on port 3001
        jupyter notebook --port 3001
        ```

- An available Druid instance. You can use the `micro-quickstart` configuration described in [Quickstart (local)](../../../docs/tutorials/index.md). The tutorials assume that you are using the quickstart, so no authentication or authorization is expected unless explicitly mentioned.

## Tutorials

The notebooks are located in the [apache/druid repo](https://github.com/apache/druid/tree/master/examples/quickstart/jupyter-notebooks/). You can either clone the repo or download the notebooks you want individually. 

The links that follow are the raw GitHub URLs, so you can use them to download the notebook directly, such as with `wget`, or manually through your web browser. Note that if you save the file from your web browser, make sure to remove the `.txt` extension.

- [Introduction to the Druid API](https://raw.githubusercontent.com/apache/druid/master/examples/quickstart/jupyter-notebooks/api-tutorial.ipynb) walks you through some of the basics related to the Druid API and several endpoints.
- [Introduction to Druid SQL](https://raw.githubusercontent.com/apache/druid/master/examples/quickstart/jupyter-notebooks/sql-tutorial.ipynb) covers the basics of Druid SQL.

## Contributing

If you build a Jupyter tutorial, you need to do a few things to add it to the docs in addition to saving the notebook in this directory. The process requires two PRs to the repo.

For the first PR, do the following:

1. Clear the outputs from your notebook before you make the PR. You can use the following command: 

   ```bash
   jupyter nbconvert --ClearOutputPreprocessor.enabled=True --inplace ./path/to/notebook/notebookName.ipynb
   ```

2. Create the PR as you normally would. Make sure to note that this PR is the one that contains only the Jupyter notebook and that there will be a subsequent PR that updates related pages.

3. After this first PR is merged, grab the "raw" URL for the file from GitHub. For example, navigate to the file in the GitHub web UI and select **Raw**. Use the URL for this in the second PR as the download link.

For the second PR, do the following:

1. Update the list of [Tutorials](#tutorials) on this page and in the [ Jupyter tutorial index page](../../../docs/tutorials/tutorial-jupyter-index.md#tutorials) in the `docs/tutorials` directory. 
2. Update `tutorial-jupyter-index.md` and provide the URL to the raw version of the file that becomes available after the first PR is merged.