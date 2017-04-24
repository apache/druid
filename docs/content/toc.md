---
layout: toc
---

## Getting Started
  * [Introduction](/docs/VERSION/design/)
  * [Quickstart](/docs/VERSION/tutorials/quickstart.html)
  * [Loading Data](/docs/VERSION/tutorials/ingestion.html)
    * [Loading from Files](/docs/VERSION/tutorials/tutorial-batch.html)
    * [Loading from Streams](/docs/VERSION/tutorials/tutorial-streams.html)
    * [Loading from Kafka](/docs/VERSION/tutorials/tutorial-kafka.html)
  * [Clustering](/docs/VERSION/tutorials/cluster.html)

## Data Ingestion
  * [Data Formats](/docs/VERSION/ingestion/data-formats.html)  
  * [Defining a Schema](/docs/VERSION/ingestion/index.html)
  * [Designing Schemas](/docs/VERSION/ingestion/schema-design.html)
  * [Changing Schemas](/docs/VERSION/ingestion/schema-changes.html)  
  * [Batch File Ingestion](/docs/VERSION/ingestion/batch-ingestion.html)
  * [Stream Ingestion](/docs/VERSION/ingestion/stream-ingestion.html)
    * [Stream Push](/docs/VERSION/ingestion/stream-push.html)
    * [Stream Pull](/docs/VERSION/ingestion/stream-pull.html)
  * [Updating Data](/docs/VERSION/ingestion/update-existing-data.html)
  * [Ingestion Tasks](/docs/VERSION/ingestion/tasks.html)

## Querying
  * [Overview](/docs/VERSION/querying/querying.html)
  * [JSON over HTTP](/docs/VERSION/querying/json-over-http.html)
  * [SQL](/docs/VERSION/querying/sql.html)
  * [Caching](/docs/VERSION/querying/caching.html)      

## Concepts
  * [Design](/docs/VERSION/design/design.html)  
  * Storage
    * [Rollup](/docs/VERSION/design/rollup.html)
    * [Partitioning](/docs/VERSION/design/partitioning.html)
    * [Segments](/docs/VERSION/design/segments.html)
    * [Columns](/docs/VERSION/design/columns.html)
  * Querying
    * [Timeline](/docs/VERSION/design/timeline.html)
  * Druid Processes
    * [Historical](/docs/VERSION/design/historical.html)
    * [Broker](/docs/VERSION/design/broker.html)
    * [Coordinator](/docs/VERSION/design/coordinator.html)
    * [Indexer](/docs/VERSION/design/indexing-service.html)    
  * Dependencies
    * [Deep Storage](/docs/VERSION/dependencies/deep-storage.html)
    * [Metadata Storage](/docs/VERSION/dependencies/metadata-storage.html)
    * [ZooKeeper](/docs/VERSION/dependencies/zookeeper.html)

## Operations
  * [Best Practices](/docs/VERSION/operations/recommendations.html)
  * Ingestion
    * [Tuning](/docs/VERSION/ingestion/faq.html)
    * [Multitenancy](/docs/VERSION/ingestion/multitenancy.html)    
  * Querying    
    * [Tuning](/docs/VERSION/querying/tuning.html)
    * [Multitenancy](/docs/VERSION/querying/multitenancy.html)
    * [Multi-value dimensions](/docs/VERSION/querying/multi-value-dimensions.html)  
  * [Data Retention](/docs/VERSION/operations/rule-configuration.html)
  * [Metrics and Monitoring](/docs/VERSION/operations/metrics.html)
  * [Alerts](/docs/VERSION/operations/alerts.html)
  * [Updating the Cluster](/docs/VERSION/operations/rolling-updates.html)  
  * [Performance FAQ](/docs/VERSION/operations/performance-faq.html)
  * Tools
    * [Introspect Segment](/docs/VERSION/operations/dump-segment.html)
    * [Insert Segment](/docs/VERSION/operations/insert-segment-to-db.html)
    * [Pull Dependencies](/docs/VERSION/operations/pull-deps.html)      

## Configuration
  * [Common Configuration](/docs/VERSION/configuration/index.html)
  * [Indexing Process](/docs/VERSION/configuration/indexing-service.html)
  * [Coordinator Process](/docs/VERSION/configuration/coordinator.html)
  * [Historical Process](/docs/VERSION/configuration/historical.html)
  * [Broker Process](/docs/VERSION/configuration/broker.html)  
  * [Logging](/docs/VERSION/configuration/logging.html)
  
## Integration   
   * [Extensions](/docs/VERSION/operations/including-extensions.html)
   * [Hadoop](/docs/VERSION/operations/other-hadoop.html)
   * [Kafka](/docs/VERSION/operations/other-hadoop.html) 
   * [Spark](/docs/VERSION/operations/other-hadoop.html)
   * [Stream Processors](/docs/VERSION/development/integrating-druid-with-other-technologies.html)
  
## Development
  * [Overview](/docs/VERSION/development/overview.html)
  * [Creating Extensions](/docs/VERSION/development/extensions.html)
  * [External Libraries](/docs/VERSION/development/libraries.html)  
  * [JavaScript](/docs/VERSION/development/javascript.html)
  * [Build From Source](/docs/VERSION/development/build.html)
  * [Versioning](/docs/VERSION/development/versioning.html)
  * [APIs](/docs/VERSION/development/apis.html)  
  * Experimental Features
    * [Overview](/docs/VERSION/development/experimental.html)
    * [Approximate Histograms and Quantiles](/docs/VERSION/development/extensions-core/approximate-histograms.html)
    * [Datasketches](/docs/VERSION/development/extensions-core/datasketches-aggregators.html)
    * [Geographic Queries](/docs/VERSION/development/geo.html)
    * [Router](/docs/VERSION/development/router.html)
    * [Kafka Indexing Service](/docs/VERSION/development/extensions-core/kafka-ingestion.html)

## Misc
  * [Papers & Talks](/docs/VERSION/misc/papers-and-talks.html)
  * [Thanks](/thanks.html)
