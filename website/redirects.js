/*
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
  */



const Redirects=[
  {
    "from": [
      "/docs/latest/configuration/auth.html",
      "/docs/latest/design/auth.html"
    ],
    "to": "/docs/latest/operations/auth"
  },
  {
    "from": [
      "/docs/latest/configuration/broker.html",
      "/docs/latest/configuration/caching.html",
      "/docs/latest/configuration/coordinator.html",
      "/docs/latest/configuration/historical.html",
      "/docs/latest/configuration/indexing-service.html",
      "/docs/latest/development/extensions-core/caffeine-cache.html"
    ],
    "to": "/docs/latest/configuration/"
  },
  {
    "from": "/docs/latest/configuration/hadoop.html",
    "to": "/docs/latest/ingestion/hadoop"
  },
  {
    "from": [
      "/docs/latest/configuration/production-cluster.html",
      "/docs/latest/configuration/simple-cluster.html",
      "/docs/latest/misc/cluster-setup.html",
      "/docs/latest/misc/evaluate.html"
    ],
    "to": "/docs/latest/tutorials/cluster"
  },
  {
    "from": [
      "/docs/latest/configuration/realtime.html",
      "/docs/latest/design/plumber.html",
      "/docs/latest/design/realtime.html",
      "/docs/latest/development/extensions-contrib/kafka-simple.html",
      "/docs/latest/development/extensions-contrib/rabbitmq.html",
      "/docs/latest/development/extensions-contrib/rocketmq.html",
      "/docs/latest/development/extensions-core/kafka-eight-firehose.html",
      "/docs/latest/ingestion/stream-pull.html"
    ],
    "to": "/docs/latest/ingestion/standalone-realtime"
  },
  {
    "from": [
      "/docs/latest/configuration/zookeeper.html",
      "/docs/latest/dependencies/zookeeper.html"
    ],
    "to": "/docs/latest/design/zookeeper"
  },
  {
    "from": "/docs/latest/dependencies/cassandra-deep-storage.html",
    "to": "/docs/latest/development/extensions-contrib/cassandra"
  },
  {
    "from": [
      "/docs/latest/design/concepts-and-terminology.html",
      "/docs/latest/design/design.html",
      "/docs/latest/ingestion/batch-ingestion.html",
      "/docs/latest/ingestion/hadoop-vs-native-batch.html",
      "/docs/latest/ingestion/ingestion.html",
      "/docs/latest/ingestion/overview.html",
      "/docs/latest/ingestion/realtime-ingestion.html",
      "/docs/latest/ingestion/stream-ingestion.html",
      "/docs/latest/tutorials/examples.html",
      "/docs/latest/tutorials/ingestion-streams.html",
      "/docs/latest/tutorials/ingestion.html",
      "/docs/latest/tutorials/quickstart.html",
      "/docs/latest/tutorials/tutorial-a-first-look-at-druid.html",
      "/docs/latest/tutorials/tutorial-all-about-queries.html"
    ],
    "to": "/docs/latest/design/"
  },
  {
    "from": "/docs/latest/development/approximate-histograms.html",
    "to": "/docs/latest/development/extensions-core/approximate-histograms"
  },
  {
    "from": "/docs/latest/development/community-extensions/azure.html",
    "to": "/docs/latest/development/extensions-core/azure"
  },
  {
    "from": "/docs/latest/development/community-extensions/cassandra.html",
    "to": "/docs/latest/development/extensions-contrib/cassandra"
  },
  {
    "from": "/docs/latest/development/community-extensions/cloudfiles.html",
    "to": "/docs/latest/development/extensions-contrib/cloudfiles"
  },
  {
    "from": "/docs/latest/development/community-extensions/graphite.html",
    "to": "/docs/latest/development/extensions-contrib/graphite"
  },
  {
    "from": [
      "/docs/latest/development/community-extensions/kafka-simple.html",
      "/docs/latest/development/community-extensions/rabbitmq.html",
      "/docs/latest/development/kafka-simple-consumer-firehose.html",
      "/docs/latest/development/extensions-core/kafka-supervisor-operations.html",
      "/docs/latest/development/extensions-core/kafka-supervisor-reference.html",
      "/docs/latest/development/extensions-core/kafka-ingestion.html",
      "/docs/latest/development/extensions-core/kafka-supervisor-operations",
      "/docs/latest/development/extensions-core/kafka-supervisor-reference",
      "/docs/latest/development/extensions-core/kafka-ingestion"
    ],
    "to": "/docs/latest/ingestion/kafka-ingestion"
  },
  {
    "from": [
      "/docs/latest/development/extensions-core/kinesis-ingestion.html",
      "/docs/latest/development/extensions-core/kinesis-ingestion"
    ],
    "to": "/docs/latest/ingestion/kinesis-ingestion"
  },
  {
    "from": "/docs/latest/development/extensions-contrib/orc.html",
    "to": "/docs/latest/development/extensions-core/orc"
  },
  {
    "from": "/docs/latest/development/extensions-contrib/parquet.html",
    "to": "/docs/latest/development/extensions-core/parquet"
  },
  {
    "from": "/docs/latest/development/extensions-contrib/scan-query.html",
    "to": "/docs/latest/querying/scan-query"
  },
  {
    "from": "/docs/latest/development/extensions-core/namespaced-lookup.html",
    "to": "/docs/latest/development/extensions-core/lookups-cached-global"
  },
  {
    "from": "/docs/latest/development/indexer.html",
    "to": "/docs/latest/design/indexer"
  },
  {
    "from": "/docs/latest/development/router.html",
    "to": "/docs/latest/design/router"
  },
  {
    "from": "/docs/latest/development/select-query.html",
    "to": "/docs/latest/querying/select-query"
  },
  {
    "from": [
      "/docs/latest/",
    ],
    "to": "/docs/latest/design/"
  },
  {
    "from": "/docs/latest/ingestion/automatic-compaction.html",
    "to": "/docs/latest/data-management/automatic-compaction"
  },
  {
    "from": "/docs/latest/ingestion/command-line-hadoop-indexer.html",
    "to": "/docs/latest/ingestion/hadoop"
  },
  {
    "from": "/docs/latest/ingestion/compaction.html",
    "to": "/docs/latest/data-management/compaction"
  },
  {
    "from": "/docs/latest/ingestion/data-management.html",
    "to": "/docs/latest/data-management/"
  },
  {
    "from": "/docs/latest/ingestion/delete-data.html",
    "to": "/docs/latest/data-management/delete"
  },
  {
    "from": [
      "/docs/latest/ingestion/firehose.html",
      "/docs/latest/ingestion/migrate-from-firehose"
  ],
    "to": "/docs/latest/operations/migrate-from-firehose" 
  },
  {
    "from": [
      "/docs/latest/ingestion/flatten-json.html",
      "/docs/latest/ingestion/transform-spec.html"
    ],
    "to": "/docs/latest/ingestion/ingestion-spec"
  },
  {
    "from": [
      "/docs/latest/ingestion/locking-and-priority.html",
      "/docs/latest/ingestion/misc-tasks.html",
      "/docs/latest/ingestion/reports.html"
    ],
    "to": "/docs/latest/ingestion/tasks"
  },
  {
    "from": [
      "/docs/latest/ingestion/native_tasks.html",
      "/docs/latest/ingestion/native-batch-input-sources.html"
  ],
    "to": "/docs/latest/ingestion/input-sources"
  },

  {
    "from": "/docs/latest/ingestion/schema-changes.html",
    "to": "/docs/latest/design/segments"
  },
  {
    "from": "/docs/latest/ingestion/stream-push.html",
    "to": "/docs/latest/ingestion/tranquility"
  },
  {
    "from": "/docs/latest/ingestion/update-existing-data.html",
    "to": "/docs/latest/data-management/update"
  },
  {
    "from": "/docs/latest/misc/tasks.html",
    "to": "/docs/latest/ingestion/tasks"
  },

{
    "from": [
      "/docs/latest/operations/including-extensions.html",
      "/docs/latest/development/extensions"
    ],
    "to": "/docs/latest/configuration/extensions"
  },
  {
    "from": "/docs/latest/operations/multitenancy.html",
    "to": "/docs/latest/querying/multitenancy"
  },
  {
    "from": "/docs/latest/operations/performance-faq.html",
    "to": "/docs/latest/operations/basic-cluster-tuning"
  },
  {
    "from": "/docs/latest/querying/optimizations.html",
    "to": "/docs/latest/querying/multi-value-dimensions"
  },
  {
    "from": "/docs/latest/querying/searchqueryspec.html",
    "to": "/docs/latest/querying/searchquery"
  },
  {
    "from": [
      "/docs/latest/tutorials/booting-a-production-cluster.html",
      "/docs/latest/tutorials/firewall.html",
      "/docs/latest/tutorials/tutorial-the-druid-cluster.html"
    ],
    "to": "/docs/latest/tutorials/cluster"
  },
  {
    "from": "/docs/latest/tutorials/tutorial-loading-batch-data.html",
    "to": "/docs/latest/tutorials/tutorial-batch"
  },
  {
    "from": "/docs/latest/tutorials/tutorial-loading-streaming-data.html",
    "to": "/docs/latest/tutorials/tutorial-kafka"
  },
  {
    "from": "/docs/latest/tutorials/tutorial-tranquility.html",
    "to": "/docs/latest/ingestion/tranquility"
  },
  {
    "from": "/docs/latest/development/extensions-contrib/google.html",
    "to": "/docs/latest/development/extensions-core/google"
  },
  {
    "from": "/docs/latest/development/integrating-druid-with-other-technologies.html",
    "to": "/docs/latest/ingestion/"
  },
  {
    "from": [
      "/docs/latest/operations/druid-console.html",
      "/docs/latest/operations/management-uis.html"
    ],
    "to": "/docs/latest/operations/web-console"
  },
  {
    "from": "/docs/latest/operations/recommendations.html",
    "to": "/docs/latest/operations/basic-cluster-tuning"
  },
  {
    "from":  "/docs/latest/misc/math-expr.html",
    "to": "/docs/latest/querying/math-expr"

  },
  {
    "from":  "/docs/latest/operations/getting-started",
    "to": "/docs/latest/design/"
  },
  {
    "from":  "/docs/latest/querying/sql-api.html",
    "to": "/docs/latest/api-reference/sql-api"
  },
  {
    "from":  "/docs/latest/querying/sql-jdbc.html",
    "to": "/docs/latest/api-reference/sql-jdbc"
  },
  {
    "from":  "/docs/latest/multi-stage-query/api.html",
    "to": "/docs/latest/api-reference/sql-ingestion-api"
  },
  {
    "from":  "/docs/latest/operations/api-reference.html",
    "to": "/docs/latest/api-reference/"
  },
  {
    "from": "/docs/latest/design/processes.html",
    "to": "/docs/latest/design/architecture"
  },
  {
    "from":  "/docs/latest/operations/api-reference/",
    "to": "/docs/latest/api-reference/"
  }
]


module.exports.Redirects = Redirects;