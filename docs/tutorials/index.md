---
id: index
title: "Quickstart"
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


This quickstart gets you started with Apache Druid and introduces you to some of its basic features. 
Following these steps, you will install Druid and load sample 
data using its native batch ingestion feature. 

Before starting, you may want to read the [general Druid overview](../design/index.md) and
[ingestion overview](../ingestion/index.md), as the tutorials refer to concepts discussed on those pages.

## Requirements

You can follow these steps on a relatively small machine, such as a laptop with around 4 CPU and 16 GB of RAM. 

Druid comes with several startup configuration profiles for a range of machine sizes. 
The `micro-quickstart`configuration profile shown here is suitable for early evaluation scenarios. To explore 
Druid's performance or scaling capabilities, you'll need a larger machine.

The configuration profiles included with Druid range from the even smaller _Nano-Quickstart_ configuration (1 CPU, 4GB RAM) 
to the _X-large_ configuration (64 CPU, 512GB RAM). For more information, see 
[Single server deployment](operations/single-server). Alternatively, see [Clustered deployment](tutorials/cluster) for 
information on deploying Druid services across clustered machines. 

The software requirements for the installation machine are:

* Linux, Mac OS X, or other Unix-like OS (Windows is not supported)
* Java 8, Update 92 or later (8u92+)

> Druid officially supports Java 8 only. Support for later major versions of Java is currently in experimental status.
>
> You can set the location of the Java installation Druid uses with the environment variables `DRUID_JAVA_HOME` or `JAVA_HOME`. For more 
information, run the verify-java script.


## Step 1. Getting started

After confirming the [requirements](#requirements), follow these steps: 

1. [Download](https://www.apache.org/dyn/closer.cgi?path=/druid/{{DRUIDVERSION}}/apache-druid-{{DRUIDVERSION}}-bin.tar.gz)
the {{DRUIDVERSION}} release.
2. Extract Druid by running the following commands in your terminal:

   ```bash
   tar -xzf apache-druid-{{DRUIDVERSION}}-bin.tar.gz
   cd apache-druid-{{DRUIDVERSION}}
   ```
In the package, you'll find `LICENSE` and `NOTICE` files and directories for executable files, configuration files, sample data and more.

## Step 2: Start up Druid services

The following commands start up Druid services using the `micro-quickstart` single-machine configuration. 

From the apache-druid-{{DRUIDVERSION}} package root, run the following command:

```bash
./bin/start-micro-quickstart
```

This brings up instances of ZooKeeper and the Druid services, all running on the local machine, as indicated in the command response:

```bash
$ ./bin/start-micro-quickstart
[Fri May  3 11:40:50 2019] Running command[zk], logging to[/apache-druid-{{DRUIDVERSION}}/var/sv/zk.log]: bin/run-zk conf
[Fri May  3 11:40:50 2019] Running command[coordinator-overlord], logging to[/apache-druid-{{DRUIDVERSION}}/var/sv/coordinator-overlord.log]: bin/run-druid coordinator-overlord conf/druid/single-server/micro-quickstart
[Fri May  3 11:40:50 2019] Running command[broker], logging to[/apache-druid-{{DRUIDVERSION}}/var/sv/broker.log]: bin/run-druid broker conf/druid/single-server/micro-quickstart
[Fri May  3 11:40:50 2019] Running command[router], logging to[/apache-druid-{{DRUIDVERSION}}/var/sv/router.log]: bin/run-druid router conf/druid/single-server/micro-quickstart
[Fri May  3 11:40:50 2019] Running command[historical], logging to[/apache-druid-{{DRUIDVERSION}}/var/sv/historical.log]: bin/run-druid historical conf/druid/single-server/micro-quickstart
[Fri May  3 11:40:50 2019] Running command[middleManager], logging to[/apache-druid-{{DRUIDVERSION}}/var/sv/middleManager.log]: bin/run-druid middleManager conf/druid/single-server/micro-quickstart
```

All persistent state, such as the cluster metadata store and segments for the services, are kept in the `var` directory under 
the Druid root directory, apache-druid-{{DRUIDVERSION}}. A log file for each service is written to `var/sv`, as noted in the command output.

At any time, you can revert Druid to its original, post-installation state by deleting the `var` directory. You may
wish to do this, for example, between Druid tutorials. 


To stop Druid, type CTRL-C in the terminal. This exits the `bin/start-micro-quickstart` script and 
terminates all Druid processes. 



## Step 3. Open the Druid console 

After Druid finishes starting up, open the [Druid console](../operations/druid-console.md) at [http://localhost:8888](http://localhost:8888). 

![Druid console](../assets/tutorial-quickstart-01.png "Druid console")

It may take a few seconds for all Druid services to start up, including the [Druid router](../design/router.md), which serves the console. If you attempt to open the Druid console before startup is complete, you may see errors in the browser. In this case, wait a few moments and try again. 



## Step 4: Loading data


Ingestion specs define the schema of the data Druid reads and stores. You can write ingestion specs by hand or using the _data loader_, 
as we will here. 

For this tutorial, we'll load sample data bundled with Druid that represents Wikipedia page edits on a given day. 

1. Click **Load data** from the Druid console header (![Load data](../assets/load-data-button.png)).

2. Select the **Local disk** tile and then click **Connect data**.

   ![Data loader init](../assets/tutorial-batch-data-loader-01.png "Data loader init")

3. Enter the following values: 

   - **Base directory**: `quickstart/tutorial/`

   - **File filter**: `wikiticker-2015-09-12-sampled.json.gz` 

   Entering the base directory and [wildcard file filter](https://commons.apache.org/proper/commons-io/apidocs/org/apache/commons/io/filefilter/WildcardFileFilter.html) separately, as we've done here, allows for having data ingested from multiple files at once.

   ![Data location](../assets/tutorial-batch-data-loader-015.png "Data location")

4. Click **Apply**. 

   The raw data appears in the data loader, allowing you to verify that you entered a valid path and the data appears as expected. 

   ![Data loader sample](../assets/tutorial-batch-data-loader-02.png "Data loader sample")

   > Notice that where you are in the data loader configuration steps appears at the top of the console, **Connect** in the 
   screenshot above. You can go 
   forward or backward at any time by clicking the step you want to go to. 

5. Click **Next: Parse data**. 

   The data loader tries to determine the correct parser for the data automatically. In this case, 
   it identifies the data format as `json`, as shown in the **Input format** field towards the bottom right of the page.

   ![Data loader parse data](../assets/tutorial-batch-data-loader-03.png "Data loader parse data")

   Feel free to select other parser options to view their configuration options and get a sense of 
   how Druid parses other types of data.

6. With the JSON parser selected, click **Next: Parse time**. Here you can view and adjust the 
   primary timestamp column for the data.

   ![Data loader parse time](../assets/tutorial-batch-data-loader-04.png "Data loader parse time")

   Druid requires data to have a primary timestamp column (internally stored in a column called `__time`).
   If you do not have a timestamp in your data, select `Constant value`. In our example, the data loader 
   determines that the `time` column is the only candidate that can be used as the primary time column.

7. Click **Next: Transform**, **Next: Filter**, and then **Next: Configure schema**, skipping a few steps.

   You do not need to adjust transformation or filtering settings, as applying ingestion time transforms and 
   filters are out of scope for this tutorial.

8. The Configure schema settings are where you configure what [dimensions](../ingestion/index.md#dimensions) 
   and [metrics](../ingestion/index.md#metrics) are ingested. The outcome of this configuration represents exactly how the 
   data will appear in Druid after ingestion. 

   Since our dataset is very small, you can turn off [rollup](../ingestion/index.md#rollup) 
   by unsetting the **Rollup** switch and confirming the change when prompted.

   ![Data loader schema](../assets/tutorial-batch-data-loader-05.png "Data loader schema")


10. Click **Next: Partition** to configure how the data will be split into segments. In this case, choose `DAY` as 
    the **Segment Granularity**. 

    ![Data loader partition](../assets/tutorial-batch-data-loader-06.png "Data loader partition")

    Since this is a small dataset, we can have a 
   single segment, which `DAY` gives us. 

11. Click **Next: Tune** and **Next: Publish**

12. The Publish settings are where you can specify the datasource name in Druid. Let's change the default from `wikiticker-2015-09-12-sampled` 
to `wikipedia`. 

    ![Data loader publish](../assets/tutorial-batch-data-loader-07.png "Data loader publish")


13. Click **Next: Edit spec** to review the ingestion spec we've constructed with the data loader. 

    ![Data loader spec](../assets/tutorial-batch-data-loader-08.png "Data loader spec")

    Feel free to go back and make changes in previous steps to see how changes will update the spec.
    Similarly, you can edit the spec directly and see it reflected in the previous steps. 

    > For other ways to load ingestion specs in Druid, see the [file loading tutorial](tutorial-batch). 

14. Once you are satisfied with the spec, click **Submit**.

    The new task for our wikipedia datasource now appears in the Ingestion view. 

    ![Tasks view](../assets/tutorial-batch-data-loader-09.png "Tasks view")

    The task may take a minute or two to complete. When done, the task status should appear as SUCCESS, with
    an indication of the duration of the task. (Note that the view is set to automatically 
    refresh, so you do not need to refresh the browser to see the status change.)

    When a tasks succeeds it means that it built one or more segments that will now be picked up by the data servers.


## Step 5. Query the data 

You can now see the data as a datasource in the console and try out a query. 

1. Click **Datasources** from the console header. 
  
   If the wikipedia datasource doesn't appear, wait a few moments for the segment to finish loading. A datasource is 
   queryable once its Availability is shown to be "Fully available". 

2. When the datasource is available, open the Actions menu (![Actions](../assets/datasources-action-button.png)) and choose  **Query with SQL**.

   ![Datasource view](../assets/tutorial-batch-data-loader-10.png "Datasource view")

   > Notice that the action menu is where you can edit retention rules for your data, configure compaction, and perform
   other administrative tasks for the data. 

3. Run the prepopulated query, `SELECT * FROM "wikipedia"` to see the results.

   ![Query view](../assets/tutorial-batch-data-loader-11.png "Query view")

Congratulations! You've gotten from no Druid at all to loading and querying data on Druid, in just one quickstart. 


## Next steps

After completing this quickstart, here's what you might want to do next. 

### Reset Druid to its original state

After stopping Druid services, you can perform a clean start by deleting the `var` directory from the Druid root directory and 
running the `bin/start-micro-quickstart` script again. You will likely want to do this before taking other data ingestion tutorials, 
since in them you will create the same wikipedia datasource. 


### Take another tutorial 

Check out the [query tutorial](../tutorials/tutorial-query.md) to further explore the Query features in the Druid console. 

Alternatively, learn about other ways to ingest data in one of these tutorials: 

- [Loading stream data from Apache Kafka](./tutorial-kafka.md) – How to load streaming data from a Kafka topic.
- [Loading a file using Apache Hadoop](./tutorial-batch-hadoop.md) – How to perform a batch file load, using a remote Hadoop cluster.
- [Writing your own ingestion spec](./tutorial-ingestion-spec.md) – How to write a new ingestion spec and use it to load data.



