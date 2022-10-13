---
id: tutorial-batch-native
title: "Load data with native batch ingestion"
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


This topic shows you how to load and query data files in Apache Druid using its native batch ingestion feature. 

## Prerequisites

Install Druid, start up Druid services, and open the web console as described in the [Druid quickstart](index.md).

## Load data

Ingestion specs define the schema of the data Druid reads and stores. You can write ingestion specs by hand or using the _data loader_, 
as we'll do here to perform batch file loading with Druid's native batch ingestion.

The Druid distribution bundles sample data we can use. The sample data located in `quickstart/tutorial/wikiticker-2015-09-12-sampled.json.gz` 
in the Druid root directory represents Wikipedia page edits for a given day. 

1. Click **Load data** from the web console header (![Load data](../assets/tutorial-batch-data-loader-00.png)).

2. Select the **Local disk** tile and then click **Connect data**.

   ![Data loader init](../assets/tutorial-batch-data-loader-01.png "Data loader init")

3. Enter the following values: 

   - **Base directory**: `quickstart/tutorial/`

   - **File filter**: `wikiticker-2015-09-12-sampled.json.gz` 

   ![Data location](../assets/tutorial-batch-data-loader-015.png "Data location")

   Entering the base directory and [wildcard file filter](https://commons.apache.org/proper/commons-io/apidocs/org/apache/commons/io/filefilter/WildcardFileFilter.html) separately, as afforded by the UI, allows you to specify multiple files for ingestion at once.

4. Click **Apply**. 

   The data loader displays the raw data, giving you a chance to verify that the data 
   appears as expected. 

   ![Data loader sample](../assets/tutorial-batch-data-loader-02.png "Data loader sample")

   Notice that your position in the sequence of steps to load data, **Connect** in our case, appears at the top of the console, as shown below. 
   You can click other steps to move forward or backward in the sequence at any time.

   ![Load data](../assets/tutorial-batch-data-loader-12.png)  


5. Click **Next: Parse data**. 

   The data loader tries to determine the parser appropriate for the data format automatically. In this case 
   it identifies the data format as `json`, as shown in the **Input format** field at the bottom right.

   ![Data loader parse data](../assets/tutorial-batch-data-loader-03.png "Data loader parse data")

   Feel free to select other **Input format** options to get a sense of their configuration settings 
   and how Druid parses other types of data.  

6. With the JSON parser selected, click **Next: Parse time**. The **Parse time** settings are where you view and adjust the 
   primary timestamp column for the data.

   ![Data loader parse time](../assets/tutorial-batch-data-loader-04.png "Data loader parse time")

   Druid requires data to have a primary timestamp column (internally stored in a column called `__time`).
   If you do not have a timestamp in your data, select `Constant value`. In our example, the data loader 
   determines that the `time` column is the only candidate that can be used as the primary time column.

7. Click **Next: Transform**, **Next: Filter**, and then **Next: Configure schema**, skipping a few steps.

   You do not need to adjust transformation or filtering settings, as applying ingestion time transforms and 
   filters are out of scope for this tutorial.

8. The Configure schema settings are where you configure what [dimensions](../ingestion/data-model.md#dimensions) 
   and [metrics](../ingestion/data-model.md#metrics) are ingested. The outcome of this configuration represents exactly how the 
   data will appear in Druid after ingestion. 

   Since our dataset is very small, you can turn off [rollup](../ingestion/rollup.md) 
   by unsetting the **Rollup** switch and confirming the change when prompted.

   ![Data loader schema](../assets/tutorial-batch-data-loader-05.png "Data loader schema")


9. Click **Next: Partition** to configure how the data will be split into segments. In this case, choose `DAY` as the **Segment granularity**. 

    ![Data loader partition](../assets/tutorial-batch-data-loader-06.png "Data loader partition")

    Since this is a small dataset, we can have just a single segment, which is what selecting `DAY` as the 
    segment granularity gives us. 

10. Click **Next: Tune** and **Next: Publish**.

11. The Publish settings are where you specify the datasource name in Druid. Let's change the default name from  `wikiticker-2015-09-12-sampled` to `wikipedia`. 

    ![Data loader publish](../assets/tutorial-batch-data-loader-07.png "Data loader publish")

12. Click **Next: Edit spec** to review the ingestion spec we've constructed with the data loader. 

    ![Data loader spec](../assets/tutorial-batch-data-loader-08.png "Data loader spec")

    Feel free to go back and change settings from previous steps to see how doing so updates the spec.
    Similarly, you can edit the spec directly and see it reflected in the previous steps. 

    For other ways to load ingestion specs in Druid, see [Tutorial: Loading a file](./tutorial-batch.md). 
13. Once you are satisfied with the spec, click **Submit**.


    The new task for our wikipedia datasource now appears in the Ingestion view. 

    ![Tasks view](../assets/tutorial-batch-data-loader-09.png "Tasks view")

    The task may take a minute or two to complete. When done, the task status should be "SUCCESS", with
    the duration of the task indicated. Note that the view is set to automatically 
    refresh, so you do not need to refresh the browser to see the status change.

    A successful task means that one or more segments have been built and are now picked up by our data servers.


## Query the data 

You can now see the data as a datasource in the console and try out a query, as follows: 

1. Click **Datasources** from the console header. 

   If the wikipedia datasource doesn't appear, wait a few moments for the segment to finish loading. A datasource is 
   queryable once it is shown to be "Fully available" in the **Availability** column. 

2. When the datasource is available, open the Actions menu (![Actions](../assets/datasources-action-button.png)) for that 
   datasource and choose **Query with SQL**.

   ![Datasource view](../assets/tutorial-batch-data-loader-10.png "Datasource view")

   > Notice the other actions you can perform for a datasource, including configuring retention rules, compaction, and more. 
3. Run the prepopulated query, `SELECT * FROM "wikipedia"` to see the results.

   ![Query view](../assets/tutorial-batch-data-loader-11.png "Query view")
