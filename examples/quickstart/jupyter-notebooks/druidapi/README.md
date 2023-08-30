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
  
# Python API for Druid


`druidapi` is a Python library to interact with all aspects of your
[Apache Druid](https://druid.apache.org/) cluster. 
`druidapi` picks up where the venerable [pydruid](https://github.com/druid-io/pydruid) library 
left off to include full SQL support and support for many of of Druid APIs. `druidapi` is usable 
in any Python environment, but is optimized for use in Jupyter, providing a complete interactive
environment which complements the UI-based Druid console. The primary use of `druidapi` at present
is to support the set of tutorial notebooks provided in the parent directory.

`druidapi` works against any version of Druid. Operations that make use of newer features obviously work
only against versions of Druid that support those features.

## Install

At present, the best way to use `druidapi` is to clone the Druid repo itself:

```bash
git clone git@github.com:apache/druid.git
```

`druidapi` is located in `examples/quickstart/jupyter-notebooks/druidapi/`.
From this directory, install the package and its dependencies with pip using the following command:

```
pip install .
```

Note that there is a second level `druidapi` directory that contains the modules. Do not run
the install command in the subdirectory.

Verify your installation by checking that the following command runs in Python:

```python
import druidapi
```

The import statement should not return anything if it runs successfully.

## Getting started

To use `druidapi`, first import the library, then connect to your cluster by providing the URL to your Router instance. The way that is done differs a bit between consumers.

### From a tutorial Jupyter notebook

The tutorial Jupyter notebooks in `examples/quickstart/jupyter-notebooks` reside in the same directory tree
as this library. We start the library using the Jupyter-oriented API which is able to render tables in
HTML. First, identify your Router endpoint. Use the following for a local installation:

```python
router_endpoint = 'http://localhost:8888'
```

Then, import the library, declare the `druidapi` CSS styles, and create a client to your cluster:

```python
import druidapi
druid = druidapi.jupyter_client(router_endpoint)
```

The `jupyter_client` call defines a number of CSS styles to aid in displaying tabular results. It also
provides a "display" client that renders information as HTML tables.

### From a Python script

`druidapi` works in any Python script. When run outside of a Jupyter notebook, the various "display"
commands revert to displaying a text (not HTML) format. The steps are similar to those above:

```python
import druidapi
druid = druidapi.client(router_endpoint)
```

## Library organization

`druidapi` organizes Druid REST operations into various "clients," each of which provides operations
for one of Druid's functional areas. Obtain a client from the `druid` client created above. For
status operations:

```python
status_client = druid.status
```

The set of clients is still under construction. The set at present includes the following. The
set of operations within each client is also partial, and includes only those operations used
within one of the tutorial notebooks. Contributions are welcome to expand the scope. Clients are
available as properties on the `druid` object created above.

* `status` - Status operations such as service health, property values, and so on. This client
  is special: it works only with the Router. The Router does not proxy these calls to other nodes.
  Use the `status_for()` method to get status for other nodes.
* `datasources` - Operations on datasources such as dropping a datasource.
* `tasks` - Work with Overlord tasks: status, reports, and more.
* `sql` - SQL query operations for both the interactive query engine and MSQ.
* `display` - A set of convenience operations to display results as lightly formatted tables
  in either HTML (for Jupyter notebooks) or text (for other Python scripts).

## Assumed cluster architecture

`druidapi` assumes that you run a standard Druid cluster with a Router in front of the other nodes.
This design works well for most Druid clusters:

* Run locally, such as the various quickstart clusters.
* Remote cluster on the same network.
* Druid cluster running under Docker Compose such as that explained in the Druid documentation.
* Druid integration test clusters launched via the Druid development `it.sh` command.
* Druid clusters running under Kubernetes

In all the Docker, Docker Compose and Kubernetes scenaris, the Router's port (typically 8888) must be visible
to the machine running `druidapi`, perhaps via port mapping or a proxy.

The Router is then responsible for routing Druid REST requests to the various other Druid nodes,
including those not visible outside of a private Docker or Kubernetes network.

The one exception to this rule is if you want to perform a health check (i.e. the `/status` endpoint)
on a service other than the Router. These checks are _not_ proxied by the Router: you must connect to
the target service directly.

## Status operations

When working with tutorials, a local Druid cluster, or a Druid integration test cluster, it is common
to start your cluster then immediately start performing `druidapi` operations. However, because Druid
is a distributed system, it can take some time for all the services to become ready. This seems to be
particularly true when starting a cluster with Docker Compose or Kubernetes on the local system.

Therefore, the first operation is to wait for the cluster to become ready:

```python
status_client = druid.status
status_client.wait_until_ready()
```

Without this step, your operations may mysteriously fail, and you'll wonder if you did something wrong.
Some clients retry operations multiple times in case a service is not yet ready. For typical scripts
against a stable cluster, the above line should be sufficient instead. This step is built into the
`jupyter_client()` method to ensure notebooks provide a good exerience.

If your notebook or script uses newer features, you should start by ensuring that the target Druid cluster
is of the correct version:

```python
status_client.version
```

This check will prevent frustration if the notebook is used against previous releases.

Similarly, if the notebook or script uses features defined in an extension, check that the required
extension is loaded:

```python
status_client.properties['druid.extensions.loadList']
```

## Display client

When run in a Jupyter notebook, it is often handy to format results for display. A special display
client performs operations _and_ formats them for display as HTML tables within the notebook.

```python
display = druid.display
```

The most common methods are:

* `sql(sql)` - Run a query and display the results as an HTML table.
* `schemas()` - Display the schemas defined in Druid.
* `tables(schema)` - Display the tables (datasources) in the given schema, `druid` by default.
* `table(name)` - Display the schema (list of columns) for the the given table. The name can
  be one part (`foo`) or two parts (`INFORMATION_SCHEMA.TABLES`).
* `function(name)` - Display the arguments for a table function defined in the catalog.

The display client also has other methods to format data as a table, to display various kinds
of messages and so on.

## Interactive queries

The original [`pydruid`](https://pythonhosted.org/pydruid/) library revolves around Druid 
"native" queries. Most new applications now use SQL. `druidapi` provides two ways to run
queries, depending on whether you want to display the results (typical in a notebook), or
use the results in Python code. You can run SQL queries using the SQL client:

```python
sql_client = druid.sql
```

To obtain the results of a SQL query against the example Wikipedia table (datasource) in a "raw" form:


```python
sql = '''
SELECT
  channel,
  COUNT(*) AS "count"
FROM wikipedia
GROUP BY channel
ORDER BY COUNT(*) DESC
LIMIT 5
'''
client.sql(sql)
```

Gives:

```text
[{'channel': '#en.wikipedia', 'count': 6650},
 {'channel': '#sh.wikipedia', 'count': 3969},
 {'channel': '#sv.wikipedia', 'count': 1867},
 {'channel': '#ceb.wikipedia', 'count': 1808},
 {'channel': '#de.wikipedia', 'count': 1357}]
```

The raw results are handy when Python code consumes the results, or for a quick check. The raw results
can also be forward to advanced visualization tools such a Pandas.

For simple visualization in notebooks (or as text in Python scripts), you can use the "display" client:

```python
display = druid.display
display.sql(sql)
```

When run without HTML visualization, the above gives:

```text
channel        count
#en.wikipedia   6650
#sh.wikipedia   3969
#sv.wikipedia   1867
#ceb.wikipedia  1808
#de.wikipedia   1357
```

Within Jupyter, the results are formatted as an HTML table.

### Advanced queries

In addition to the SQL text, Druid also lets you specify:

* A query context
* Query parameters
* Result format options

The Druid `SqlQuery` object specifies these options. You can build up a Python equivalent:

```python
sql = '''
SELECT *
FROM INFORMATION_SCHEMA.SCHEMATA
WHERE SCHEMA_NAME = ?
'''

sql_query = {
    'query': sql,
    'parameters': [
        {'type': consts.SQL_VARCHAR_TYPE, 'value': 'druid'}
    ],
    'resultFormat': consts.SQL_OBJECT
}
```

However, the easier approach is to let `druidapi` handle the work for you using a SQL request:

```python
req = self.client.sql_request(sql)
req.add_parameter('druid')
```

Either way, when you submit the query in this form, you get a SQL response back:

```python
resp = sql_client.sql_query(req)
```

The SQL response wraps the REST response. First, we ensure that the request worked:

```python
resp.ok
```

If the request failed, we can obtain the error message:

```python
resp.error_message
```

If the request succeeded, we can obtain the results in a variety of ways. The easiest is to obtain
the data as a list of Java objects. This is the form shown in the "raw" example above. This works
only if you use the default ('objects') result format.

```python
resp.rows
```

You can also obtain the schema of the result:

```python
resp.schema
```

The result is a list of `ColumnSchema` objects. Get column information from the `name`, `sql_type`
and `druid_type` fields in each object. 

For other formats, you can obtain the REST payload directly:

```python
resp.results
```

Use the `results()` method if you requested other formats, such as CSV. The `rows()` and `schema()` methods
are not available for these other result formats.

The result can also format the results as a text or HTML table, depending on how you created the client:

```python
resp.show()
```

In fact, the display client `sql()` method uses the `resp.show()` method internally, which in turn uses the
`rows` and `schema` properties.

### Run a query and return results

The above forms are handy for interactive use in a notebook. If you just need to run a query to use the results
in code, just do the following:

```python
rows = sql_client.sql(sql)
```

This form takes a set of arguments so that you can use Python to parameterize the query:

```python
sql = 'SELECT * FROM {}'
rows = sql_client.sql(sql, ['myTable'])
```

## MSQ queries

The SQL client can also run an MSQ query. See the `sql-tutorial.ipynb` notebook for examples. First define the
query:

```python
sql = '''
INSERT INTO myTable ...
'''
```

Then launch an ingestion task:

```python
task = sql_client.task(sql)
```

To learn the Overlord task ID:

```python
task.id
```

You can use the tasks client to track the status, or let the task object do it for you:

```python
task.wait_until_done()
```

You can combine the run-and-wait operations into a single call:

```python
task = sql_client.run_task(sql)
```

A quirk of Druid is that MSQ reports task completion as soon as ingestion is done. However, it takes a 
while for Druid to load the resulting segments, so you must wait for the table to become queryable:

```python
sql_client.wait_until_ready('myTable')
```

## Datasource operations

To get information about a datasource, prefer to query the `INFORMATION_SCHEMA` tables, or use the methods
in the display client. Use the datasource client for other operations.

```python
datasources = druid.datasources
```

To delete a datasource:

```python
datasources.drop('myWiki', True)
```

The True argument asks for "if exists" semantics so you don't get an error if the datasource does not exist.

## REST client

The `druidapi` is based on a simple REST client which is itself based on the Requests library. If you
need to use Druid REST APIs not yet wrapped by this library, you can use the REST client directly.
(If you find such APIs, we encourage you to add methods to the library and contribute them to Druid.)

The REST client implements the common patterns seen in the Druid REST API. You can create a client directly:

```python
from druidapi.rest import DruidRestClient
rest_client = DruidRestClient("http://localhost:8888")
```

Or, if you have already created the Druid client, you can reuse the existing REST client. This is how 
the various other clients work internally.

```python
rest_client = druid.rest
```

The REST API maintains the Druid host: you just provide the specifc URL tail. There are methods to get or 
post JSON results. For example, to get status information:

```python
rest_client.get_json('/status')
```

A quick comparison of the three approaches (Requests, REST client, Python client):

Status:

* Requests: `session.get(druid_host + '/status').json()`
* REST client: `rest_client.get_json('/status')`
* Status client: `status_client.status()`

Health:

* Requests: `session.get(druid_host + '/status/health').json()`
* REST client: `rest_client.get_json('/status/health')`
* Status client: `status_client.is_healthy()`

Ingest data:

* Requests: See the REST tutorial.
* REST client: as the REST tutorial, but use `rest_client.post_json('/druid/v2/sql/task', sql_request)` and
  `rest_client.get_json(f"/druid/indexer/v1/task/{ingestion_taskId}/status")`
* SQL client: `sql_client.run_task(sql)`, also a form for a full SQL request.

List datasources:

* Requests: `session.get(druid_host + '/druid/coordinator/v1/datasources').json()`
* REST client: `rest_client.get_json('/druid/coordinator/v1/datasources')`
* Datasources client: `ds_client.names()`

Query data, where `sql_request` is a properly-formatted `SqlRequest` dictionary:

* Requests: `session.post(druid_host + '/druid/v2/sql', json=sql_request).json()`
* REST client: `rest_client.post_json('/druid/v2/sql', sql_request)`
* SQL Client: `sql_client.show(sql)`, where `sql` is the query text

In general, you have to provide the all the details for the Requests library. The REST client handles the low-level repetitious bits. The Python clients provide methods that encapsulate the specifics of the URLs and return formats.

## Constants

Druid has a large number of special constants: type names, options, etc. The consts module provides definitions for many of these:

```python
from druidapi import consts
help(consts)
```

## Contributing

We encourage you to contribute to the `druidapi` package.
Set up an editable installation for development by running the following command
in a local clone of your `apache/druid` repo in
`examples/quickstart/jupyter-notebooks/druidapi/`:

```
pip install -e .
```

An editable installation allows you to implement and test changes iteratively
without having to reinstall the package with every change.

When you update the package, also increment the version field in `setup.py` following the
[PEP 440 semantic versioning scheme](https://peps.python.org/pep-0440/#semantic-versioning).

Use the following guidelines for incrementing the version number:
* Increment the third position for a patch or bug fix.
* Increment the second position for new features, such as adding new method wrappers.
* Increment the first position for major changes and changes that are not backwards compatible.

Submit your contribution by opening a pull request to the `apache/druid` GitHub repository.

