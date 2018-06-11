---
layout: toc
---

## Getting Started
  * [Concepts](/docs/VERSION/design/)
  * [Quickstart](/docs/VERSION/tutorials/quickstart.html)
  * [Loading Data](/docs/VERSION/tutorials/ingestion.html)
    * [Loading from Files](/docs/VERSION/tutorials/tutorial-batch.html)
    * [Loading from Streams](/docs/VERSION/tutorials/tutorial-streams.html)
    * [Loading from Kafka](/docs/VERSION/tutorials/tutorial-kafka.html)
  * [Clustering](/docs/VERSION/tutorials/cluster.html)

## Data Ingestion
  * [Data Formats](/docs/VERSION/ingestion/data-formats.html)
  * [Ingestion Spec](/docs/VERSION/ingestion/index.html)
  * [Schema Design](/docs/VERSION/ingestion/schema-design.html)
  * [Schema Changes](/docs/VERSION/ingestion/schema-changes.html)
  * [Batch File Ingestion](/docs/VERSION/ingestion/batch-ingestion.html)
  * [Stream Ingestion](/docs/VERSION/ingestion/stream-ingestion.html)
    * [Stream Push](/docs/VERSION/ingestion/stream-push.html)
    * [Stream Pull](/docs/VERSION/ingestion/stream-pull.html)
  * [Updating Existing Data](/docs/VERSION/ingestion/update-existing-data.html)
  * [Ingestion Tasks](/docs/VERSION/ingestion/tasks.html)
  * [FAQ](/docs/VERSION/ingestion/faq.html)

## Querying
  * [Overview](/docs/VERSION/querying/querying.html)
  * [Timeseries](/docs/VERSION/querying/timeseriesquery.html)
  * [TopN](/docs/VERSION/querying/topnquery.html)
  * [GroupBy](/docs/VERSION/querying/groupbyquery.html)
  * [Time Boundary](/docs/VERSION/querying/timeboundaryquery.html)
  * [Segment Metadata](/docs/VERSION/querying/segmentmetadataquery.html)
  * [DataSource Metadata](/docs/VERSION/querying/datasourcemetadataquery.html)
  * [Search](/docs/VERSION/querying/searchquery.html)
  * [Select](/docs/VERSION/querying/select-query.html)
  * [Scan](/docs/VERSION/querying/scan-query.html)
  * Components
    * [Datasources](/docs/VERSION/querying/datasource.html)
    * [Filters](/docs/VERSION/querying/filters.html)
    * [Aggregations](/docs/VERSION/querying/aggregations.html)
    * [Post Aggregations](/docs/VERSION/querying/post-aggregations.html)
    * [Granularities](/docs/VERSION/querying/granularities.html)
    * [DimensionSpecs](/docs/VERSION/querying/dimensionspecs.html)
    * [Context](/docs/VERSION/querying/query-context.html)
  * [Multi-value dimensions](/docs/VERSION/querying/multi-value-dimensions.html)
  * [SQL](/docs/VERSION/querying/sql.html)
  * [Lookups](/docs/VERSION/querying/lookups.html)
  * [Joins](/docs/VERSION/querying/joins.html)
  * [Multitenancy](/docs/VERSION/querying/multitenancy.html)
  * [Caching](/docs/VERSION/querying/caching.html)
  * [Sorting Orders](/docs/VERSION/querying/sorting-orders.html)

## Design
  * [Overview](/docs/VERSION/design/design.html)
  * Storage
    * [Segments](/docs/VERSION/design/segments.html)
  * Node Types
    * [Historical](/docs/VERSION/design/historical.html)
    * [Broker](/docs/VERSION/design/broker.html)
    * [Coordinator](/docs/VERSION/design/coordinator.html)
    * [Indexing Service](/docs/VERSION/design/indexing-service.html)
    * [Realtime](/docs/VERSION/design/realtime.html)
  * Dependencies
    * [Deep Storage](/docs/VERSION/dependencies/deep-storage.html)
    * [Metadata Storage](/docs/VERSION/dependencies/metadata-storage.html)
    * [ZooKeeper](/docs/VERSION/dependencies/zookeeper.html)

## Operations
  * [Good Practices](/docs/VERSION/operations/recommendations.html)
  * [Including Extensions](/docs/VERSION/operations/including-extensions.html)
  * [Data Retention](/docs/VERSION/operations/rule-configuration.html)
  * [Metrics and Monitoring](/docs/VERSION/operations/metrics.html)
  * [Alerts](/docs/VERSION/operations/alerts.html)
  * [Updating the Cluster](/docs/VERSION/operations/rolling-updates.html)
  * [Different Hadoop Versions](/docs/VERSION/operations/other-hadoop.html)
  * [Performance FAQ](/docs/VERSION/operations/performance-faq.html)
  * [Dump Segment Tool](/docs/VERSION/operations/dump-segment.html)
  * [Insert Segment Tool](/docs/VERSION/operations/insert-segment-to-db.html)
  * [Pull Dependencies Tool](/docs/VERSION/operations/pull-deps.html)
  * [Recommendations](/docs/VERSION/operations/recommendations.html)
  * [TLS Support](/docs/VERSION/operations/tls-support.html)
  * [Password Provider](/docs/VERSION/operations/password-provider.html)

## Configuration
  * [Common Configuration](/docs/VERSION/configuration/index.html)
  * [Indexing Service](/docs/VERSION/configuration/indexing-service.html)
  * [Coordinator](/docs/VERSION/configuration/coordinator.html)
  * [Historical](/docs/VERSION/configuration/historical.html)
  * [Broker](/docs/VERSION/configuration/broker.html)
  * [Realtime](/docs/VERSION/configuration/realtime.html)
  * [Configuring Logging](/docs/VERSION/configuration/logging.html)
  * [Configuring Authentication and Authorization](/docs/VERSION/configuration/auth.html)
  
## Development
  * [Overview](/docs/VERSION/development/overview.html)
  * [Libraries](/docs/VERSION/development/libraries.html)
  * [Extensions](/docs/VERSION/development/extensions.html)
  * [JavaScript](/docs/VERSION/development/javascript.html)
  * [Build From Source](/docs/VERSION/development/build.html)
  * [Versioning](/docs/VERSION/development/versioning.html)
  * [Integration](/docs/VERSION/development/integrating-druid-with-other-technologies.html)
  * Experimental Features
    * [Overview](/docs/VERSION/development/experimental.html)
    * [Approximate Histograms and Quantiles](/docs/VERSION/development/extensions-core/approximate-histograms.html)
    * [Datasketches](/docs/VERSION/development/extensions-core/datasketches-extension.html)
    * [Geographic Queries](/docs/VERSION/development/geo.html)
    * [Router](/docs/VERSION/development/router.html)
    * [Kafka Indexing Service](/docs/VERSION/development/extensions-core/kafka-ingestion.html)


## Misc
  * [Papers & Talks](/docs/VERSION/misc/papers-and-talks.html)
  * [Thanks](/thanks.html)
