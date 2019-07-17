---
layout: toc
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

## Getting Started
  * [Design](/docs/VERSION/design/index.html)
    * [What is Druid?](/docs/VERSION/design/index.html#what-is-druid)
    * [When should I use Druid?](/docs/VERSION/design/index.html#when-to-use-druid)
    * [Architecture](/docs/VERSION/design/index.html#architecture)
    * [Datasources & Segments](/docs/VERSION/design/index.html#datasources-and-segments)
    * [Query processing](/docs/VERSION/design/index.html#query-processing)
    * [External dependencies](/docs/VERSION/design/index.html#external-dependencies)
    * [Ingestion overview](/docs/VERSION/ingestion/index.html)
  * [Getting Started](/docs/VERSION/operations/getting-started.html)
    * [Single-server Quickstart](/docs/VERSION/tutorials/index.html)
      * [Tutorial: Loading a file from local disk](/docs/VERSION/tutorials/tutorial-batch.html)
      * [Tutorial: Loading stream data from Apache Kafka](/docs/VERSION/tutorials/tutorial-kafka.html)
      * [Tutorial: Loading a file using Apache Hadoop](/docs/VERSION/tutorials/tutorial-batch-hadoop.html)
      * [Tutorial: Loading stream data using HTTP push](/docs/VERSION/tutorials/tutorial-tranquility.html)
      * [Tutorial: Querying data](/docs/VERSION/tutorials/tutorial-query.html)
      * Further tutorials
        * [Tutorial: Rollup](/docs/VERSION/tutorials/tutorial-rollup.html)
        * [Tutorial: Configuring retention](/docs/VERSION/tutorials/tutorial-retention.html)
        * [Tutorial: Updating existing data](/docs/VERSION/tutorials/tutorial-update-data.html)
        * [Tutorial: Compacting segments](/docs/VERSION/tutorials/tutorial-compaction.html)
        * [Tutorial: Deleting data](/docs/VERSION/tutorials/tutorial-delete-data.html)
        * [Tutorial: Writing your own ingestion specs](/docs/VERSION/tutorials/tutorial-ingestion-spec.html)
        * [Tutorial: Transforming input data](/docs/VERSION/tutorials/tutorial-transform-spec.html)    
    * [Clustering](/docs/VERSION/tutorials/cluster.html)
    * Further examples
      * [Single-server deployment](/docs/VERSION/operations/single-server.html)
      * [Clustered deployment](/docs/VERSION/tutorials/cluster.html#fresh-deployment)

## Data Ingestion
  * [Ingestion overview](/docs/VERSION/ingestion/index.html)
  * [Schema Design](/docs/VERSION/ingestion/schema-design.html)
  * [Data Formats](/docs/VERSION/ingestion/data-formats.html)
  * [Tasks Overview](/docs/VERSION/ingestion/tasks.html)
  * [Ingestion Spec](/docs/VERSION/ingestion/ingestion-spec.html)
    * [Transform Specs](/docs/VERSION/ingestion/transform-spec.html)
    * [Firehoses](/docs/VERSION/ingestion/firehose.html)
  * [Schema Changes](/docs/VERSION/ingestion/schema-changes.html)
  * [Batch File Ingestion](/docs/VERSION/ingestion/batch-ingestion.html)
    * [Native Batch Ingestion](/docs/VERSION/ingestion/native_tasks.html)
    * [Hadoop Batch Ingestion](/docs/VERSION/ingestion/hadoop.html)
  * [Stream Ingestion](/docs/VERSION/ingestion/stream-ingestion.html)
    * [Kafka Indexing Service (Stream Pull)](/docs/VERSION/development/extensions-core/kafka-ingestion.html)
    * [Stream Push](/docs/VERSION/ingestion/stream-push.html)
  * [Compaction](/docs/VERSION/ingestion/compaction.html)
  * [Updating Existing Data](/docs/VERSION/ingestion/update-existing-data.html)
  * [Deleting Data](/docs/VERSION/ingestion/delete-data.html)
  * [Task Locking & Priority](/docs/VERSION/ingestion/locking-and-priority.html)
  * [Task Reports](/docs/VERSION/ingestion/reports.html)
  * [FAQ](/docs/VERSION/ingestion/faq.html)
  * [Misc. Tasks](/docs/VERSION/ingestion/misc-tasks.html)

## Querying
  * [Druid SQL](/docs/VERSION/querying/sql.html)
  * [Native queries](/docs/VERSION/querying/querying.html)
    * [Timeseries](/docs/VERSION/querying/timeseriesquery.html)
    * [TopN](/docs/VERSION/querying/topnquery.html)
    * [GroupBy](/docs/VERSION/querying/groupbyquery.html)
    * [Time Boundary](/docs/VERSION/querying/timeboundaryquery.html)
    * [Segment Metadata](/docs/VERSION/querying/segmentmetadataquery.html)
    * [DataSource Metadata](/docs/VERSION/querying/datasourcemetadataquery.html)
    * [Search](/docs/VERSION/querying/searchquery.html)
    * [Scan](/docs/VERSION/querying/scan-query.html)
    * [Select](/docs/VERSION/querying/select-query.html)
    * Components
      * [Datasources](/docs/VERSION/querying/datasource.html)
      * [Filters](/docs/VERSION/querying/filters.html)
      * [Aggregations](/docs/VERSION/querying/aggregations.html)
      * [Post Aggregations](/docs/VERSION/querying/post-aggregations.html)
      * [Granularities](/docs/VERSION/querying/granularities.html)
      * [DimensionSpecs](/docs/VERSION/querying/dimensionspecs.html)
      * [Sorting Orders](/docs/VERSION/querying/sorting-orders.html)
      * [Virtual Columns](/docs/VERSION/querying/virtual-columns.html)
      * [Context](/docs/VERSION/querying/query-context.html)
  * Concepts
    * [Multi-value dimensions](/docs/VERSION/querying/multi-value-dimensions.html)
    * [Lookups](/docs/VERSION/querying/lookups.html)
    * [Joins](/docs/VERSION/querying/joins.html)
    * [Multitenancy](/docs/VERSION/querying/multitenancy.html)
    * [Caching](/docs/VERSION/querying/caching.html)
    * [Geographic Queries](/docs/VERSION/development/geo.html) (experimental)

## Design
  * [Overview](/docs/VERSION/design/index.html)
  * Storage
    * [Segments](/docs/VERSION/design/segments.html)
  * [Servers and Processes](/docs/VERSION/design/processes.html)
    * Master server
      * [Coordinator](/docs/VERSION/design/coordinator.html)
      * [Overlord](/docs/VERSION/design/overlord.html)
    * Query server
      * [Broker](/docs/VERSION/design/broker.html)
      * [Router](/docs/VERSION/development/router.html) (optional; experimental)
    * Data server
      * [Historical](/docs/VERSION/design/historical.html)
      * [MiddleManager](/docs/VERSION/design/middlemanager.html)
        * [Peons](/docs/VERSION/design/peons.html)    
  * Dependencies
    * [Deep Storage](/docs/VERSION/dependencies/deep-storage.html)
    * [Metadata Storage](/docs/VERSION/dependencies/metadata-storage.html)
    * [ZooKeeper](/docs/VERSION/dependencies/zookeeper.html)

## Operations
  * [Management UIs](/docs/VERSION/operations/management-uis.html)    
  * [Including Extensions](/docs/VERSION/operations/including-extensions.html)
  * [Data Retention](/docs/VERSION/operations/rule-configuration.html)
  * [High Availability](/docs/VERSION/operations/high-availability.html)
  * [Updating the Cluster](/docs/VERSION/operations/rolling-updates.html)
  * [Metrics and Monitoring](/docs/VERSION/operations/metrics.html)
  * [Alerts](/docs/VERSION/operations/alerts.html)
  * [Different Hadoop Versions](/docs/VERSION/operations/other-hadoop.html)
  * [HTTP Compression](/docs/VERSION/operations/http-compression.html)  
  * [API Reference](/docs/VERSION/operations/api-reference.html)
      * [Coordinator](/docs/VERSION/operations/api-reference.html#coordinator)
      * [Overlord](/docs/VERSION/operations/api-reference.html#overlord)
      * [MiddleManager](/docs/VERSION/operations/api-reference.html#middlemanager)
      * [Peon](/docs/VERSION/operations/api-reference.html#peon)
      * [Broker](/docs/VERSION/operations/api-reference.html#broker)
      * [Historical](/docs/VERSION/operations/api-reference.html#historical)
  * Tuning and Recommendations
    * [Basic Cluster Tuning](/docs/VERSION/operations/basic-cluster-tuning.html)  
    * [General Recommendations](/docs/VERSION/operations/recommendations.html)
    * [JVM Best Practices](/docs/VERSION/configuration/index.html#jvm-configuration-best-practices)        
  * Tools
    * [Dump Segment Tool](/docs/VERSION/operations/dump-segment.html)
    * [Insert Segment Tool](/docs/VERSION/operations/insert-segment-to-db.html)
    * [Pull Dependencies Tool](/docs/VERSION/operations/pull-deps.html)  
  * Security
    * [TLS Support](/docs/VERSION/operations/tls-support.html)
    * [Password Provider](/docs/VERSION/operations/password-provider.html)  

## Configuration
  * [Configuration Reference](/docs/VERSION/configuration/index.html)
  * [Recommended Configuration File Organization](/docs/VERSION/configuration/index.html#recommended-configuration-file-organization)  
  * [Common Configuration](/docs/VERSION/configuration/index.html#common-configurations)
  * Processes
    * [Coordinator](/docs/VERSION/configuration/index.html#coordinator)
    * [Overlord](/docs/VERSION/configuration/index.html#overlord)
    * [MiddleManager & Peons](/docs/VERSION/configuration/index.html#middle-manager-and-peons)    
    * [Historical](/docs/VERSION/configuration/index.html#historical)
    * [Broker](/docs/VERSION/configuration/index.html#broker)
  * [Caching](/docs/VERSION/configuration/index.html#cache-configuration)
  * [General Query Configuration](/docs/VERSION/configuration/index.html#general-query-configuration)
  * [Configuring Logging](/docs/VERSION/configuration/logging.html)

## Development
  * [Overview](/docs/VERSION/development/overview.html)
  * [Libraries](/libraries.html)
  * [Extensions](/docs/VERSION/development/extensions.html)
  * [JavaScript](/docs/VERSION/development/javascript.html)
  * [Build From Source](/docs/VERSION/development/build.html)
  * [Versioning](/docs/VERSION/development/versioning.html)
  * [Integration](/docs/VERSION/development/integrating-druid-with-other-technologies.html)
  * [Experimental Features](/docs/VERSION/development/experimental.html)

## Misc
  * [Druid Expressions Language](/docs/VERSION/misc/math-expr.html)
  * [Papers & Talks](/docs/VERSION/misc/papers-and-talks.html)
  * [Thanks](/thanks.html)
