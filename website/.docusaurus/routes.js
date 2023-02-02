import React from 'react';
import ComponentCreator from '@docusaurus/ComponentCreator';

export default [
  {
    path: '/__docusaurus/debug',
    component: ComponentCreator('/__docusaurus/debug', '25f'),
    exact: true
  },
  {
    path: '/__docusaurus/debug/config',
    component: ComponentCreator('/__docusaurus/debug/config', '340'),
    exact: true
  },
  {
    path: '/__docusaurus/debug/content',
    component: ComponentCreator('/__docusaurus/debug/content', 'f62'),
    exact: true
  },
  {
    path: '/__docusaurus/debug/globalData',
    component: ComponentCreator('/__docusaurus/debug/globalData', '2b7'),
    exact: true
  },
  {
    path: '/__docusaurus/debug/metadata',
    component: ComponentCreator('/__docusaurus/debug/metadata', '62c'),
    exact: true
  },
  {
    path: '/__docusaurus/debug/registry',
    component: ComponentCreator('/__docusaurus/debug/registry', '193'),
    exact: true
  },
  {
    path: '/__docusaurus/debug/routes',
    component: ComponentCreator('/__docusaurus/debug/routes', '169'),
    exact: true
  },
  {
    path: '/search',
    component: ComponentCreator('/search', 'f40'),
    exact: true
  },
  {
    path: '/docs',
    component: ComponentCreator('/docs', '544'),
    routes: [
      {
        path: '/docs/comparisons/druid-vs-elasticsearch',
        component: ComponentCreator('/docs/comparisons/druid-vs-elasticsearch', 'cb9'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/comparisons/druid-vs-key-value',
        component: ComponentCreator('/docs/comparisons/druid-vs-key-value', '40c'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/comparisons/druid-vs-kudu',
        component: ComponentCreator('/docs/comparisons/druid-vs-kudu', '4bf'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/comparisons/druid-vs-redshift',
        component: ComponentCreator('/docs/comparisons/druid-vs-redshift', '6a9'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/comparisons/druid-vs-spark',
        component: ComponentCreator('/docs/comparisons/druid-vs-spark', '762'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/comparisons/druid-vs-sql-on-hadoop',
        component: ComponentCreator('/docs/comparisons/druid-vs-sql-on-hadoop', 'd5b'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/configuration/',
        component: ComponentCreator('/docs/configuration/', '10b'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/configuration/human-readable-byte',
        component: ComponentCreator('/docs/configuration/human-readable-byte', 'ecf'),
        exact: true
      },
      {
        path: '/docs/configuration/logging',
        component: ComponentCreator('/docs/configuration/logging', '3c7'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/data-management/',
        component: ComponentCreator('/docs/data-management/', '886'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/data-management/automatic-compaction',
        component: ComponentCreator('/docs/data-management/automatic-compaction', '574'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/data-management/compaction',
        component: ComponentCreator('/docs/data-management/compaction', 'aa4'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/data-management/delete',
        component: ComponentCreator('/docs/data-management/delete', 'af7'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/data-management/schema-changes',
        component: ComponentCreator('/docs/data-management/schema-changes', '2a2'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/data-management/update',
        component: ComponentCreator('/docs/data-management/update', '339'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/dependencies/deep-storage',
        component: ComponentCreator('/docs/dependencies/deep-storage', 'e74'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/dependencies/metadata-storage',
        component: ComponentCreator('/docs/dependencies/metadata-storage', '07d'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/dependencies/zookeeper',
        component: ComponentCreator('/docs/dependencies/zookeeper', '49c'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/design/',
        component: ComponentCreator('/docs/design/', '9d4'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/design/architecture',
        component: ComponentCreator('/docs/design/architecture', '3e1'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/design/auth',
        component: ComponentCreator('/docs/design/auth', '9f0'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/design/broker',
        component: ComponentCreator('/docs/design/broker', '566'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/design/coordinator',
        component: ComponentCreator('/docs/design/coordinator', '282'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/design/extensions-contrib/dropwizard',
        component: ComponentCreator('/docs/design/extensions-contrib/dropwizard', 'deb'),
        exact: true
      },
      {
        path: '/docs/design/historical',
        component: ComponentCreator('/docs/design/historical', '2ab'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/design/indexer',
        component: ComponentCreator('/docs/design/indexer', '3db'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/design/indexing-service',
        component: ComponentCreator('/docs/design/indexing-service', '8f9'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/design/middlemanager',
        component: ComponentCreator('/docs/design/middlemanager', '056'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/design/overlord',
        component: ComponentCreator('/docs/design/overlord', '489'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/design/peons',
        component: ComponentCreator('/docs/design/peons', '867'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/design/processes',
        component: ComponentCreator('/docs/design/processes', '016'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/design/router',
        component: ComponentCreator('/docs/design/router', '63e'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/design/segments',
        component: ComponentCreator('/docs/design/segments', '610'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/build',
        component: ComponentCreator('/docs/development/build', '0e6'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/experimental',
        component: ComponentCreator('/docs/development/experimental', 'f49'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/experimental-features',
        component: ComponentCreator('/docs/development/experimental-features', 'dc1'),
        exact: true
      },
      {
        path: '/docs/development/extensions',
        component: ComponentCreator('/docs/development/extensions', '7e9'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-contrib/aliyun-oss',
        component: ComponentCreator('/docs/development/extensions-contrib/aliyun-oss', '806'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-contrib/ambari-metrics-emitter',
        component: ComponentCreator('/docs/development/extensions-contrib/ambari-metrics-emitter', 'a33'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-contrib/cassandra',
        component: ComponentCreator('/docs/development/extensions-contrib/cassandra', '07b'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-contrib/cloudfiles',
        component: ComponentCreator('/docs/development/extensions-contrib/cloudfiles', 'ec5'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-contrib/compressed-big-decimal',
        component: ComponentCreator('/docs/development/extensions-contrib/compressed-big-decimal', '724'),
        exact: true
      },
      {
        path: '/docs/development/extensions-contrib/distinctcount',
        component: ComponentCreator('/docs/development/extensions-contrib/distinctcount', 'f77'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-contrib/gce-extensions',
        component: ComponentCreator('/docs/development/extensions-contrib/gce-extensions', '9c9'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-contrib/graphite',
        component: ComponentCreator('/docs/development/extensions-contrib/graphite', '784'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-contrib/influx',
        component: ComponentCreator('/docs/development/extensions-contrib/influx', '977'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-contrib/influxdb-emitter',
        component: ComponentCreator('/docs/development/extensions-contrib/influxdb-emitter', 'cff'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-contrib/k8s-jobs',
        component: ComponentCreator('/docs/development/extensions-contrib/k8s-jobs', '3d2'),
        exact: true
      },
      {
        path: '/docs/development/extensions-contrib/kafka-emitter',
        component: ComponentCreator('/docs/development/extensions-contrib/kafka-emitter', '1f0'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-contrib/materialized-view',
        component: ComponentCreator('/docs/development/extensions-contrib/materialized-view', 'a6d'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-contrib/momentsketch-quantiles',
        component: ComponentCreator('/docs/development/extensions-contrib/momentsketch-quantiles', 'ea7'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-contrib/moving-average-query',
        component: ComponentCreator('/docs/development/extensions-contrib/moving-average-query', 'a97'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-contrib/opentsdb-emitter',
        component: ComponentCreator('/docs/development/extensions-contrib/opentsdb-emitter', '422'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-contrib/prometheus',
        component: ComponentCreator('/docs/development/extensions-contrib/prometheus', '46a'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-contrib/redis-cache',
        component: ComponentCreator('/docs/development/extensions-contrib/redis-cache', 'b31'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-contrib/sqlserver',
        component: ComponentCreator('/docs/development/extensions-contrib/sqlserver', 'c58'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-contrib/statsd',
        component: ComponentCreator('/docs/development/extensions-contrib/statsd', 'f38'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-contrib/tdigestsketch-quantiles',
        component: ComponentCreator('/docs/development/extensions-contrib/tdigestsketch-quantiles', '115'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-contrib/thrift',
        component: ComponentCreator('/docs/development/extensions-contrib/thrift', '407'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-contrib/time-min-max',
        component: ComponentCreator('/docs/development/extensions-contrib/time-min-max', '89d'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-core/approximate-histograms',
        component: ComponentCreator('/docs/development/extensions-core/approximate-histograms', '3b4'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-core/avro',
        component: ComponentCreator('/docs/development/extensions-core/avro', '559'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-core/azure',
        component: ComponentCreator('/docs/development/extensions-core/azure', '8a8'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-core/bloom-filter',
        component: ComponentCreator('/docs/development/extensions-core/bloom-filter', '6c4'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-core/datasketches-extension',
        component: ComponentCreator('/docs/development/extensions-core/datasketches-extension', '321'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-core/datasketches-hll',
        component: ComponentCreator('/docs/development/extensions-core/datasketches-hll', '62d'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-core/datasketches-kll',
        component: ComponentCreator('/docs/development/extensions-core/datasketches-kll', 'a52'),
        exact: true
      },
      {
        path: '/docs/development/extensions-core/datasketches-quantiles',
        component: ComponentCreator('/docs/development/extensions-core/datasketches-quantiles', 'ed0'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-core/datasketches-theta',
        component: ComponentCreator('/docs/development/extensions-core/datasketches-theta', 'dfc'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-core/datasketches-tuple',
        component: ComponentCreator('/docs/development/extensions-core/datasketches-tuple', '7ac'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-core/druid-aws-rds',
        component: ComponentCreator('/docs/development/extensions-core/druid-aws-rds', '382'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-core/druid-basic-security',
        component: ComponentCreator('/docs/development/extensions-core/druid-basic-security', 'b5d'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-core/druid-kerberos',
        component: ComponentCreator('/docs/development/extensions-core/druid-kerberos', '824'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-core/druid-lookups',
        component: ComponentCreator('/docs/development/extensions-core/druid-lookups', 'f57'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-core/druid-pac4j',
        component: ComponentCreator('/docs/development/extensions-core/druid-pac4j', 'e5f'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-core/druid-ranger-security',
        component: ComponentCreator('/docs/development/extensions-core/druid-ranger-security', '57f'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-core/examples',
        component: ComponentCreator('/docs/development/extensions-core/examples', '377'),
        exact: true
      },
      {
        path: '/docs/development/extensions-core/google',
        component: ComponentCreator('/docs/development/extensions-core/google', '3c4'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-core/hdfs',
        component: ComponentCreator('/docs/development/extensions-core/hdfs', '832'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-core/kafka-extraction-namespace',
        component: ComponentCreator('/docs/development/extensions-core/kafka-extraction-namespace', '6b7'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-core/kafka-ingestion',
        component: ComponentCreator('/docs/development/extensions-core/kafka-ingestion', 'aae'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-core/kafka-supervisor-operations',
        component: ComponentCreator('/docs/development/extensions-core/kafka-supervisor-operations', 'bb1'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-core/kafka-supervisor-reference',
        component: ComponentCreator('/docs/development/extensions-core/kafka-supervisor-reference', '535'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-core/kinesis-ingestion',
        component: ComponentCreator('/docs/development/extensions-core/kinesis-ingestion', 'e90'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-core/kubernetes',
        component: ComponentCreator('/docs/development/extensions-core/kubernetes', '1f7'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-core/lookups-cached-global',
        component: ComponentCreator('/docs/development/extensions-core/lookups-cached-global', 'f01'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-core/mysql',
        component: ComponentCreator('/docs/development/extensions-core/mysql', '304'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-core/orc',
        component: ComponentCreator('/docs/development/extensions-core/orc', '7d4'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-core/parquet',
        component: ComponentCreator('/docs/development/extensions-core/parquet', '6b1'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-core/postgresql',
        component: ComponentCreator('/docs/development/extensions-core/postgresql', 'bca'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-core/protobuf',
        component: ComponentCreator('/docs/development/extensions-core/protobuf', '721'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-core/s3',
        component: ComponentCreator('/docs/development/extensions-core/s3', 'a44'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-core/simple-client-sslcontext',
        component: ComponentCreator('/docs/development/extensions-core/simple-client-sslcontext', '4b4'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-core/stats',
        component: ComponentCreator('/docs/development/extensions-core/stats', '094'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/extensions-core/test-stats',
        component: ComponentCreator('/docs/development/extensions-core/test-stats', '773'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/geo',
        component: ComponentCreator('/docs/development/geo', '328'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/javascript',
        component: ComponentCreator('/docs/development/javascript', 'dcb'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/modules',
        component: ComponentCreator('/docs/development/modules', '221'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/overview',
        component: ComponentCreator('/docs/development/overview', '523'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/development/versioning',
        component: ComponentCreator('/docs/development/versioning', 'ed0'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/ingestion/',
        component: ComponentCreator('/docs/ingestion/', 'd72'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/ingestion/data-formats',
        component: ComponentCreator('/docs/ingestion/data-formats', 'e6d'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/ingestion/data-model',
        component: ComponentCreator('/docs/ingestion/data-model', '570'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/ingestion/faq',
        component: ComponentCreator('/docs/ingestion/faq', '535'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/ingestion/hadoop',
        component: ComponentCreator('/docs/ingestion/hadoop', '1d4'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/ingestion/ingestion-spec',
        component: ComponentCreator('/docs/ingestion/ingestion-spec', '1b9'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/ingestion/migrate-from-firehose',
        component: ComponentCreator('/docs/ingestion/migrate-from-firehose', '18f'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/ingestion/native-batch',
        component: ComponentCreator('/docs/ingestion/native-batch', 'd5f'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/ingestion/native-batch-firehose',
        component: ComponentCreator('/docs/ingestion/native-batch-firehose', 'f88'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/ingestion/native-batch-input-sources',
        component: ComponentCreator('/docs/ingestion/native-batch-input-sources', '8ec'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/ingestion/native-batch-simple-task',
        component: ComponentCreator('/docs/ingestion/native-batch-simple-task', '12b'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/ingestion/partitioning',
        component: ComponentCreator('/docs/ingestion/partitioning', '8d2'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/ingestion/rollup',
        component: ComponentCreator('/docs/ingestion/rollup', '54f'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/ingestion/schema-design',
        component: ComponentCreator('/docs/ingestion/schema-design', 'fdc'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/ingestion/standalone-realtime',
        component: ComponentCreator('/docs/ingestion/standalone-realtime', '621'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/ingestion/tasks',
        component: ComponentCreator('/docs/ingestion/tasks', '888'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/ingestion/tranquility',
        component: ComponentCreator('/docs/ingestion/tranquility', '6c3'),
        exact: true
      },
      {
        path: '/docs/misc/math-expr',
        component: ComponentCreator('/docs/misc/math-expr', 'fb2'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/misc/papers-and-talks',
        component: ComponentCreator('/docs/misc/papers-and-talks', 'd6b'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/multi-stage-query/',
        component: ComponentCreator('/docs/multi-stage-query/', 'dce'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/multi-stage-query/api',
        component: ComponentCreator('/docs/multi-stage-query/api', '00f'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/multi-stage-query/concepts',
        component: ComponentCreator('/docs/multi-stage-query/concepts', '128'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/multi-stage-query/examples',
        component: ComponentCreator('/docs/multi-stage-query/examples', '326'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/multi-stage-query/known-issues',
        component: ComponentCreator('/docs/multi-stage-query/known-issues', '587'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/multi-stage-query/reference',
        component: ComponentCreator('/docs/multi-stage-query/reference', 'bf5'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/multi-stage-query/security',
        component: ComponentCreator('/docs/multi-stage-query/security', '5d2'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/operations/alerts',
        component: ComponentCreator('/docs/operations/alerts', 'e56'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/operations/api-reference',
        component: ComponentCreator('/docs/operations/api-reference', 'cd7'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/operations/auth-ldap',
        component: ComponentCreator('/docs/operations/auth-ldap', 'fa2'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/operations/basic-cluster-tuning',
        component: ComponentCreator('/docs/operations/basic-cluster-tuning', '2a2'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/operations/clean-metadata-store',
        component: ComponentCreator('/docs/operations/clean-metadata-store', 'ba4'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/operations/deep-storage-migration',
        component: ComponentCreator('/docs/operations/deep-storage-migration', '3ca'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/operations/dump-segment',
        component: ComponentCreator('/docs/operations/dump-segment', '79a'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/operations/dynamic-config-provider',
        component: ComponentCreator('/docs/operations/dynamic-config-provider', 'f48'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/operations/export-metadata',
        component: ComponentCreator('/docs/operations/export-metadata', '914'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/operations/getting-started',
        component: ComponentCreator('/docs/operations/getting-started', '7e3'),
        exact: true
      },
      {
        path: '/docs/operations/high-availability',
        component: ComponentCreator('/docs/operations/high-availability', 'f4d'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/operations/http-compression',
        component: ComponentCreator('/docs/operations/http-compression', '2b1'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/operations/insert-segment-to-db',
        component: ComponentCreator('/docs/operations/insert-segment-to-db', '2df'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/operations/java',
        component: ComponentCreator('/docs/operations/java', 'fc1'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/operations/kubernetes',
        component: ComponentCreator('/docs/operations/kubernetes', '751'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/operations/metadata-migration',
        component: ComponentCreator('/docs/operations/metadata-migration', '8b1'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/operations/metrics',
        component: ComponentCreator('/docs/operations/metrics', '802'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/operations/mixed-workloads',
        component: ComponentCreator('/docs/operations/mixed-workloads', '4fc'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/operations/other-hadoop',
        component: ComponentCreator('/docs/operations/other-hadoop', '981'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/operations/password-provider',
        component: ComponentCreator('/docs/operations/password-provider', 'b3c'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/operations/pull-deps',
        component: ComponentCreator('/docs/operations/pull-deps', 'fef'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/operations/python',
        component: ComponentCreator('/docs/operations/python', 'd78'),
        exact: true
      },
      {
        path: '/docs/operations/request-logging',
        component: ComponentCreator('/docs/operations/request-logging', '707'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/operations/reset-cluster',
        component: ComponentCreator('/docs/operations/reset-cluster', 'cc4'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/operations/rolling-updates',
        component: ComponentCreator('/docs/operations/rolling-updates', 'e57'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/operations/rule-configuration',
        component: ComponentCreator('/docs/operations/rule-configuration', '0ec'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/operations/security-overview',
        component: ComponentCreator('/docs/operations/security-overview', '30e'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/operations/security-user-auth',
        component: ComponentCreator('/docs/operations/security-user-auth', 'fa6'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/operations/segment-optimization',
        component: ComponentCreator('/docs/operations/segment-optimization', '74f'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/operations/single-server',
        component: ComponentCreator('/docs/operations/single-server', '025'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/operations/tls-support',
        component: ComponentCreator('/docs/operations/tls-support', '53c'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/operations/use_sbt_to_build_fat_jar',
        component: ComponentCreator('/docs/operations/use_sbt_to_build_fat_jar', '633'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/operations/web-console',
        component: ComponentCreator('/docs/operations/web-console', '412'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/querying/',
        component: ComponentCreator('/docs/querying/', 'ef8'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/querying/aggregations',
        component: ComponentCreator('/docs/querying/aggregations', '085'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/querying/caching',
        component: ComponentCreator('/docs/querying/caching', '536'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/querying/datasource',
        component: ComponentCreator('/docs/querying/datasource', 'f52'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/querying/datasourcemetadataquery',
        component: ComponentCreator('/docs/querying/datasourcemetadataquery', '192'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/querying/dimensionspecs',
        component: ComponentCreator('/docs/querying/dimensionspecs', 'd68'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/querying/filters',
        component: ComponentCreator('/docs/querying/filters', '336'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/querying/granularities',
        component: ComponentCreator('/docs/querying/granularities', '8d3'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/querying/groupbyquery',
        component: ComponentCreator('/docs/querying/groupbyquery', '5fb'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/querying/having',
        component: ComponentCreator('/docs/querying/having', '929'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/querying/hll-old',
        component: ComponentCreator('/docs/querying/hll-old', '1ea'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/querying/joins',
        component: ComponentCreator('/docs/querying/joins', '14b'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/querying/limitspec',
        component: ComponentCreator('/docs/querying/limitspec', '327'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/querying/lookups',
        component: ComponentCreator('/docs/querying/lookups', 'a0d'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/querying/multi-value-dimensions',
        component: ComponentCreator('/docs/querying/multi-value-dimensions', '4bc'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/querying/multitenancy',
        component: ComponentCreator('/docs/querying/multitenancy', '60d'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/querying/nested-columns',
        component: ComponentCreator('/docs/querying/nested-columns', 'de9'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/querying/post-aggregations',
        component: ComponentCreator('/docs/querying/post-aggregations', '4e7'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/querying/query-context',
        component: ComponentCreator('/docs/querying/query-context', '6aa'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/querying/query-execution',
        component: ComponentCreator('/docs/querying/query-execution', 'f2d'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/querying/scan-query',
        component: ComponentCreator('/docs/querying/scan-query', '7dc'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/querying/searchquery',
        component: ComponentCreator('/docs/querying/searchquery', 'b06'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/querying/segmentmetadataquery',
        component: ComponentCreator('/docs/querying/segmentmetadataquery', 'e60'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/querying/select-query',
        component: ComponentCreator('/docs/querying/select-query', '739'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/querying/sorting-orders',
        component: ComponentCreator('/docs/querying/sorting-orders', 'b3c'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/querying/sql',
        component: ComponentCreator('/docs/querying/sql', '0be'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/querying/sql-aggregations',
        component: ComponentCreator('/docs/querying/sql-aggregations', '31b'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/querying/sql-api',
        component: ComponentCreator('/docs/querying/sql-api', '192'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/querying/sql-data-types',
        component: ComponentCreator('/docs/querying/sql-data-types', '972'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/querying/sql-functions',
        component: ComponentCreator('/docs/querying/sql-functions', '4dd'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/querying/sql-jdbc',
        component: ComponentCreator('/docs/querying/sql-jdbc', 'dd9'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/querying/sql-json-functions',
        component: ComponentCreator('/docs/querying/sql-json-functions', '30a'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/querying/sql-metadata-tables',
        component: ComponentCreator('/docs/querying/sql-metadata-tables', '3da'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/querying/sql-multivalue-string-functions',
        component: ComponentCreator('/docs/querying/sql-multivalue-string-functions', 'f6d'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/querying/sql-operators',
        component: ComponentCreator('/docs/querying/sql-operators', '5a4'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/querying/sql-query-context',
        component: ComponentCreator('/docs/querying/sql-query-context', 'a34'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/querying/sql-scalar',
        component: ComponentCreator('/docs/querying/sql-scalar', '436'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/querying/sql-translation',
        component: ComponentCreator('/docs/querying/sql-translation', '14a'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/querying/timeboundaryquery',
        component: ComponentCreator('/docs/querying/timeboundaryquery', '338'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/querying/timeseriesquery',
        component: ComponentCreator('/docs/querying/timeseriesquery', '3e8'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/querying/topnmetricspec',
        component: ComponentCreator('/docs/querying/topnmetricspec', '5d3'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/querying/topnquery',
        component: ComponentCreator('/docs/querying/topnquery', '91b'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/querying/troubleshooting',
        component: ComponentCreator('/docs/querying/troubleshooting', '730'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/querying/using-caching',
        component: ComponentCreator('/docs/querying/using-caching', 'f33'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/querying/virtual-columns',
        component: ComponentCreator('/docs/querying/virtual-columns', '7fe'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/tutorials/',
        component: ComponentCreator('/docs/tutorials/', '0a4'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/tutorials/cluster',
        component: ComponentCreator('/docs/tutorials/cluster', '589'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/tutorials/docker',
        component: ComponentCreator('/docs/tutorials/docker', '76c'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/tutorials/tutorial-batch',
        component: ComponentCreator('/docs/tutorials/tutorial-batch', 'db4'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/tutorials/tutorial-batch-hadoop',
        component: ComponentCreator('/docs/tutorials/tutorial-batch-hadoop', 'c54'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/tutorials/tutorial-batch-native',
        component: ComponentCreator('/docs/tutorials/tutorial-batch-native', 'cd5'),
        exact: true
      },
      {
        path: '/docs/tutorials/tutorial-compaction',
        component: ComponentCreator('/docs/tutorials/tutorial-compaction', '0c9'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/tutorials/tutorial-delete-data',
        component: ComponentCreator('/docs/tutorials/tutorial-delete-data', '484'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/tutorials/tutorial-ingestion-spec',
        component: ComponentCreator('/docs/tutorials/tutorial-ingestion-spec', '185'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/tutorials/tutorial-jdbc',
        component: ComponentCreator('/docs/tutorials/tutorial-jdbc', 'f8c'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/tutorials/tutorial-jupyter-index',
        component: ComponentCreator('/docs/tutorials/tutorial-jupyter-index', 'f1d'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/tutorials/tutorial-kafka',
        component: ComponentCreator('/docs/tutorials/tutorial-kafka', 'f0b'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/tutorials/tutorial-kerberos-hadoop',
        component: ComponentCreator('/docs/tutorials/tutorial-kerberos-hadoop', '249'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/tutorials/tutorial-msq-convert-spec',
        component: ComponentCreator('/docs/tutorials/tutorial-msq-convert-spec', 'c22'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/tutorials/tutorial-msq-extern',
        component: ComponentCreator('/docs/tutorials/tutorial-msq-extern', 'dfb'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/tutorials/tutorial-query',
        component: ComponentCreator('/docs/tutorials/tutorial-query', '14a'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/tutorials/tutorial-retention',
        component: ComponentCreator('/docs/tutorials/tutorial-retention', 'def'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/tutorials/tutorial-rollup',
        component: ComponentCreator('/docs/tutorials/tutorial-rollup', '757'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/tutorials/tutorial-sketches-theta',
        component: ComponentCreator('/docs/tutorials/tutorial-sketches-theta', '0ff'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/tutorials/tutorial-sql-query-view',
        component: ComponentCreator('/docs/tutorials/tutorial-sql-query-view', 'fc1'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/tutorials/tutorial-transform-spec',
        component: ComponentCreator('/docs/tutorials/tutorial-transform-spec', '9e3'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/tutorials/tutorial-unnest-datasource',
        component: ComponentCreator('/docs/tutorials/tutorial-unnest-datasource', '57a'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/tutorials/tutorial-update-data',
        component: ComponentCreator('/docs/tutorials/tutorial-update-data', 'c2b'),
        exact: true,
        sidebar: "docs"
      }
    ]
  },
  {
    path: '/',
    component: ComponentCreator('/', '538'),
    exact: true
  },
  {
    path: '*',
    component: ComponentCreator('*'),
  },
];
