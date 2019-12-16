---
layout: doc_page
title: "Druid HBase2 Indexing"
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

# Druid HBase2 Indexing

To use this Apache Druid (incubating) extension, make sure to [include](../../operations/including-extensions.html) 
`druid-hbase2-indexing` extension.

## Introduction

This extension is a plug-in that indexes data in HBase.  
Based on ParallelIndexTask and HadoopIndexTask, only Batch Indexing is supported due to the nature of HBase.  
It supports indexing of data in tables as well as snapshots.
Since the HBase2 Indexer is based on ParallelIndexTask, it can be executed in the form of Peon in the middle manager node
 or in the M/R form because it is based on HadoopIndexTask.  
You can also adjust the parallelism to suit your preferences. However, in case of MapReduce, it depends on 
the property of HBase's TableInputFormat.

**The Druid HBase2 Indexer has been tested with HBase 2.2.2.**

## Configuration
HBase2 uses the same version of the protobuf library as Druid, 
so no druid.extensions.useExtensionClassloaderFirst setting is required.
But if you want to use it with HBase1, you need to configure it for HBase1.

## Specification

HBase2 Indexer needs to be configured differently depending on execution type such as Peon and MapReduce and 
cluster type such as secure cluster and non-secure cluster. And the setting must be different depending on 
whether the target data is a table or a snapshot.  

All settings for indexing HBase data must be defined in the spec file.

### Common part 
Since the data in HBase is not a Json string, it is necessary to map the row key and column values 
to Dimension and Metric and define what the data type is. These definitions must be defined in the specification file 
identically, whether based on Peon or MapReduce.  
This definition is called the `hbaseRowSpec` item and must be placed within a parser specification whose type is `hbase2`.

```json
"dataSchema": {
  "dataSource" : "wikipedia-hbase-table",
  "parser" : {
    "type" : "hbase2",
    "parseSpec" : {
      "format" : "hbase2",
      "dimensionsSpec" : {
        "dimensions" : [
          "channel",
          "cityName",
          ...
        ]
      },
      "timestampSpec" : {
        "format" : "auto",
        "column" : "time"
      },
      "hbaseRowSpec": {
        "rowKeySpec": {
          "format": "delimiter",
          "delimiter": "|",
          "columns": [
            {
              "type": "string",
              "name": "salt"
            },
            {
              "type": "string",
              "name": "time"
            }
          ]
        },
        "columnSpec": [
          {
            "type": "string",
            "name": "A:channel",
            "mappingName": "channel"
          },
          {
            "type": "string",
            "name": "A:cityName",
            "mappingName": "cityName"
          },
          ...
          {
            "type": "int",
            "name": "A:added",
            "mappingName": "added"
          },
          {
            "type": "int",
            "name": "A:deleted",
            "mappingName": "deleted"
          },
          ...
        ]
      }
    }
  },
"metricsSpec" : [
  {
    "type": "count",
    "name": "count"
    },
  {
    "type": "longSum",
    "name": "sum_added",
    "fieldName": "added",
    "expression": null
  },
  ...
],
...
}
```

The `hbaseRowSpec` consists of a `rowKeySpec` and a `columnSpec`, where the `rowKeySpec` describes the structure of 
the HBase RowKey, and the `columnSpec` describes the data to be indexed on HBase.   
Both must define a `type` item that describes their type. 
Currently supported types are `string, int, long, float, double, boolean`.

* rowKeySpec  
The `rowKeySpec`'s format field indicates whether to parse the HBase's RowKey with a `delimiter` or fixed length(`fixedLength`). 
When defining the delimiter, the delimiter item must be defined together, and in the case of fixed length, 
the length must be defined in the `length` item for each column. 
Each column name can be referenced when defining a dimension or metric specification.
* columnSpec  
The `name` in `columnSpec` represents the column name of HBase and should be defined 
in the form of `[Column Family]:[Column Name]`. 
The dimension and metric specifications refer to the names defined in mappingName.

### Peon
Since Peon type execution is based on ParallelIndexTask, the spec file is defined as `index_parallel` type.
The difference is the `firehose` entry in the `ioConfig` specification. Since we need to read data from HBase, 
we define a `firebase` of type `hbase2`.

```json
"ioConfig": {
  "type": "index_parallel",
  "firehose" : {
    "type": "hbase2",
    "connectionConfig": {
      "zookeeperQuorum": "<zookeeper>",
      "kerberosConfig": {
        "principal": null,
        "keytab": null
      }
    },
    "scanInfo": {
      "type": "table",
      "name": "default:wikipedia",
      "startKey": null,
      "endKey": null
    },
    "splitByRegion": true,
    "hbaseClientConfig": {
      "hbase.client.scanner.timeout.period": 60000,
      "hbase.client.scanner.caching": 100,
      "hbase.client.scanner.max.result.size": 2097152
    }
  }
}
```

* connectionConfig  
Describes the zookeeper information for the target cluster. 
In case of security cluster, `principal` and `keytab` file information should be provided in `kerberosConfig`. 
For insecure clusters, you can omit the `kerberosConfig` entry.
* scanInfo  
Describe the information about the data to be read.
`type` determines whether to read from a `table` or a `snapshot`. 
`name` is the name of the table or snapshot. For snapshots, a `restoreDir` item can be added, 
whose value must be on **the same filesystem** as HBase. 
The range of data to be read with `startKey` and `endKey` can be limited. 
If it is null, full scan is performed and when defining, it should be defined as string type. 
In the case of a binary, it is defined in the form of a binary string such as 
`org.apache.hadoop.hbase.util.Bytes#toStringBinary`.
* splitByRegion  
Determine the number of tasks for parallel processing. 
If `true`, one task will be executed for each region, and if you want to run fewer or more tasks, define `taskCount`.
* hbaseClientConfig
Defines properties to apply to HBase client.

### MapReduce
If you are using HDFS as deep storage, using MapReduce to index the data on HBase is a good choice. 
Like the Peon method, both Secure/Nonsecure clusters can be accessed on MapReduce.
MapReduce is based on HadoopIndexTask. Unlike Peon, you have to define how to read data in inputSpec item 
that is not firehose. For this reason, 'index_hadoop' Spec should not be used as it is, 
but `index_hadoop_hbase2` Spec should be used.

```json
{
  "type": "index_hadoop_hbase2",
  "spec" : {
    ...
    "ioConfig" : {
      "type" : "hadoop",
      "inputSpec" : {
        "type" : "hbase2",
        "connectionConfig": {
          "zookeeperQuorum": "<zookeeper>",
          "kerberosConfig": {
            "principal": "",
            "keytab": ""
          }
        },
        "scanInfo": {
          "type": "snapshot",
          "name": "wikipedia-snapshot",
          "startKey": null,
          "endKey": null
        },
        "hbaseClientConfig": {
        }
      }
    },
    "tuningConfig": {
      "type": "hadoop",
      ...
      "jobProperties": {
        "mapreduce.job.classloader" : true,
        "mapreduce.job.classloader.system.classes": "-org.apache.hadoop.hbase.,-org.apache.hadoop.metrics2.MetricHistogram,-org.apache.hadoop.metrics2.MetricsExecutor,-org.apache.hadoop.metrics2.lib.,org.apache.hadoop.",
      }
    }
  },
  "hadoopDependencyCoordinates": ["org.apache.hadoop:hadoop-client:2.8.5"]
}
```

One thing to note here is that you must add these values 
to the **mapreduce.job.classloader.system.classes** property when **reading the snapshot**.
`org.apache.hadoop.` already in the default value of the `mapreduce.job.classloader.system.classes` property. 
This will cause class loading problems with the classes in the _org.apache.hadoop_ package 
in the _hbase-hadoop-compat_ and _hbase-hadoop2-compat_ libraries.

If you need to access a cluster other than the one used as Deep Storage, 
add the relevant property value to `jobProperties` item of `tuningConfig` specification.

One thing to note is that if the HDFS used for deep storage is a secure cluster, 
the `ipc.client.fallback-to-simple-auth-allowed` setting must be set in `core-site.xml` 
in `$DRUID_HOME/conf/druid/~/_common` when reading snapshots of nonsecure clusters.
In the case of the peon method, it can be set in the specification file.

If there is a conflict due to a library version problem, place the `druid-hbase2-indexing.properties` file 
on the classpath or create it in `$DRUID_HOME/extensions/druid-hbase2-indexing` directory.
Then write the library you want to remove in the `hadoop.remove.lib.in.classpath` property.

```druid-hbase-indexing.properties
# Remove a library that has a conflict in MapReduce.
hadoop.remove.lib.in.classpath=hbase-1.,some-lib-1.0.0.jar,...
```

The property value must include the library name and version information, 
and all libraries that start with that value are removed from the Hadoop classpath.