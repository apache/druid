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
    path: '/community/',
    component: ComponentCreator('/community/', '4e0'),
    exact: true
  },
  {
    path: '/community/cla',
    component: ComponentCreator('/community/cla', '935'),
    exact: true
  },
  {
    path: '/community/join-slack',
    component: ComponentCreator('/community/join-slack', '9f4'),
    exact: true
  },
  {
    path: '/downloads',
    component: ComponentCreator('/downloads', 'ad7'),
    exact: true
  },
  {
    path: '/druid-powered',
    component: ComponentCreator('/druid-powered', '064'),
    exact: true
  },
  {
    path: '/faq',
    component: ComponentCreator('/faq', '40e'),
    exact: true
  },
  {
    path: '/libraries',
    component: ComponentCreator('/libraries', '9b8'),
    exact: true
  },
  {
    path: '/licensing',
    component: ComponentCreator('/licensing', '929'),
    exact: true
  },
  {
    path: '/technology',
    component: ComponentCreator('/technology', '2f2'),
    exact: true
  },
  {
    path: '/use-cases',
    component: ComponentCreator('/use-cases', '138'),
    exact: true
  },
  {
    path: '/docs/latest',
    component: ComponentCreator('/docs/latest', 'bd0'),
    routes: [
      {
        path: '/docs/latest/comparisons/druid-vs-elasticsearch',
        component: ComponentCreator('/docs/latest/comparisons/druid-vs-elasticsearch', '07d'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/comparisons/druid-vs-key-value',
        component: ComponentCreator('/docs/latest/comparisons/druid-vs-key-value', '00c'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/comparisons/druid-vs-kudu',
        component: ComponentCreator('/docs/latest/comparisons/druid-vs-kudu', '7eb'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/comparisons/druid-vs-redshift',
        component: ComponentCreator('/docs/latest/comparisons/druid-vs-redshift', 'f55'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/comparisons/druid-vs-spark',
        component: ComponentCreator('/docs/latest/comparisons/druid-vs-spark', '072'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/comparisons/druid-vs-sql-on-hadoop',
        component: ComponentCreator('/docs/latest/comparisons/druid-vs-sql-on-hadoop', '39a'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/configuration/',
        component: ComponentCreator('/docs/latest/configuration/', '9ea'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/configuration/human-readable-byte',
        component: ComponentCreator('/docs/latest/configuration/human-readable-byte', '12a'),
        exact: true
      },
      {
        path: '/docs/latest/configuration/logging',
        component: ComponentCreator('/docs/latest/configuration/logging', 'd62'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/data-management/',
        component: ComponentCreator('/docs/latest/data-management/', '65e'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/data-management/automatic-compaction',
        component: ComponentCreator('/docs/latest/data-management/automatic-compaction', '1c9'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/data-management/compaction',
        component: ComponentCreator('/docs/latest/data-management/compaction', 'e91'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/data-management/delete',
        component: ComponentCreator('/docs/latest/data-management/delete', 'a93'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/data-management/schema-changes',
        component: ComponentCreator('/docs/latest/data-management/schema-changes', 'ca4'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/data-management/update',
        component: ComponentCreator('/docs/latest/data-management/update', 'f1b'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/dependencies/deep-storage',
        component: ComponentCreator('/docs/latest/dependencies/deep-storage', '8c5'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/dependencies/metadata-storage',
        component: ComponentCreator('/docs/latest/dependencies/metadata-storage', 'ae8'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/dependencies/zookeeper',
        component: ComponentCreator('/docs/latest/dependencies/zookeeper', 'f07'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/design/',
        component: ComponentCreator('/docs/latest/design/', '7a6'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/design/architecture',
        component: ComponentCreator('/docs/latest/design/architecture', '3c1'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/design/auth',
        component: ComponentCreator('/docs/latest/design/auth', '5ee'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/design/broker',
        component: ComponentCreator('/docs/latest/design/broker', '742'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/design/coordinator',
        component: ComponentCreator('/docs/latest/design/coordinator', '904'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/design/extensions-contrib/dropwizard',
        component: ComponentCreator('/docs/latest/design/extensions-contrib/dropwizard', 'cff'),
        exact: true
      },
      {
        path: '/docs/latest/design/historical',
        component: ComponentCreator('/docs/latest/design/historical', 'dd8'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/design/indexer',
        component: ComponentCreator('/docs/latest/design/indexer', 'a80'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/design/indexing-service',
        component: ComponentCreator('/docs/latest/design/indexing-service', 'ef1'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/design/middlemanager',
        component: ComponentCreator('/docs/latest/design/middlemanager', '96f'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/design/overlord',
        component: ComponentCreator('/docs/latest/design/overlord', '918'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/design/peons',
        component: ComponentCreator('/docs/latest/design/peons', '2d8'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/design/processes',
        component: ComponentCreator('/docs/latest/design/processes', '26f'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/design/router',
        component: ComponentCreator('/docs/latest/design/router', 'f08'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/design/segments',
        component: ComponentCreator('/docs/latest/design/segments', 'd16'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/build',
        component: ComponentCreator('/docs/latest/development/build', '220'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/experimental',
        component: ComponentCreator('/docs/latest/development/experimental', 'aee'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/experimental-features',
        component: ComponentCreator('/docs/latest/development/experimental-features', '1ec'),
        exact: true
      },
      {
        path: '/docs/latest/development/extensions',
        component: ComponentCreator('/docs/latest/development/extensions', '230'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-contrib/aliyun-oss',
        component: ComponentCreator('/docs/latest/development/extensions-contrib/aliyun-oss', '856'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-contrib/ambari-metrics-emitter',
        component: ComponentCreator('/docs/latest/development/extensions-contrib/ambari-metrics-emitter', 'd3b'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-contrib/cassandra',
        component: ComponentCreator('/docs/latest/development/extensions-contrib/cassandra', 'c57'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-contrib/cloudfiles',
        component: ComponentCreator('/docs/latest/development/extensions-contrib/cloudfiles', 'ec5'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-contrib/compressed-big-decimal',
        component: ComponentCreator('/docs/latest/development/extensions-contrib/compressed-big-decimal', 'c01'),
        exact: true
      },
      {
        path: '/docs/latest/development/extensions-contrib/distinctcount',
        component: ComponentCreator('/docs/latest/development/extensions-contrib/distinctcount', '26a'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-contrib/gce-extensions',
        component: ComponentCreator('/docs/latest/development/extensions-contrib/gce-extensions', '951'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-contrib/graphite',
        component: ComponentCreator('/docs/latest/development/extensions-contrib/graphite', 'cbd'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-contrib/influx',
        component: ComponentCreator('/docs/latest/development/extensions-contrib/influx', 'adf'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-contrib/influxdb-emitter',
        component: ComponentCreator('/docs/latest/development/extensions-contrib/influxdb-emitter', 'ff0'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-contrib/k8s-jobs',
        component: ComponentCreator('/docs/latest/development/extensions-contrib/k8s-jobs', '560'),
        exact: true
      },
      {
        path: '/docs/latest/development/extensions-contrib/kafka-emitter',
        component: ComponentCreator('/docs/latest/development/extensions-contrib/kafka-emitter', '15c'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-contrib/materialized-view',
        component: ComponentCreator('/docs/latest/development/extensions-contrib/materialized-view', '160'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-contrib/momentsketch-quantiles',
        component: ComponentCreator('/docs/latest/development/extensions-contrib/momentsketch-quantiles', 'c67'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-contrib/moving-average-query',
        component: ComponentCreator('/docs/latest/development/extensions-contrib/moving-average-query', 'd10'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-contrib/opentsdb-emitter',
        component: ComponentCreator('/docs/latest/development/extensions-contrib/opentsdb-emitter', '8ee'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-contrib/prometheus',
        component: ComponentCreator('/docs/latest/development/extensions-contrib/prometheus', '5c3'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-contrib/redis-cache',
        component: ComponentCreator('/docs/latest/development/extensions-contrib/redis-cache', '964'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-contrib/sqlserver',
        component: ComponentCreator('/docs/latest/development/extensions-contrib/sqlserver', '70a'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-contrib/statsd',
        component: ComponentCreator('/docs/latest/development/extensions-contrib/statsd', '9fc'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-contrib/tdigestsketch-quantiles',
        component: ComponentCreator('/docs/latest/development/extensions-contrib/tdigestsketch-quantiles', '231'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-contrib/thrift',
        component: ComponentCreator('/docs/latest/development/extensions-contrib/thrift', 'c12'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-contrib/time-min-max',
        component: ComponentCreator('/docs/latest/development/extensions-contrib/time-min-max', '463'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-core/approximate-histograms',
        component: ComponentCreator('/docs/latest/development/extensions-core/approximate-histograms', '1aa'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-core/avro',
        component: ComponentCreator('/docs/latest/development/extensions-core/avro', '2b5'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-core/azure',
        component: ComponentCreator('/docs/latest/development/extensions-core/azure', '89b'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-core/bloom-filter',
        component: ComponentCreator('/docs/latest/development/extensions-core/bloom-filter', 'b97'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-core/datasketches-extension',
        component: ComponentCreator('/docs/latest/development/extensions-core/datasketches-extension', 'f1b'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-core/datasketches-hll',
        component: ComponentCreator('/docs/latest/development/extensions-core/datasketches-hll', '1e3'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-core/datasketches-kll',
        component: ComponentCreator('/docs/latest/development/extensions-core/datasketches-kll', 'dd0'),
        exact: true
      },
      {
        path: '/docs/latest/development/extensions-core/datasketches-quantiles',
        component: ComponentCreator('/docs/latest/development/extensions-core/datasketches-quantiles', '825'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-core/datasketches-theta',
        component: ComponentCreator('/docs/latest/development/extensions-core/datasketches-theta', '168'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-core/datasketches-tuple',
        component: ComponentCreator('/docs/latest/development/extensions-core/datasketches-tuple', 'ba1'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-core/druid-aws-rds',
        component: ComponentCreator('/docs/latest/development/extensions-core/druid-aws-rds', 'c78'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-core/druid-basic-security',
        component: ComponentCreator('/docs/latest/development/extensions-core/druid-basic-security', '6ed'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-core/druid-kerberos',
        component: ComponentCreator('/docs/latest/development/extensions-core/druid-kerberos', '9ba'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-core/druid-lookups',
        component: ComponentCreator('/docs/latest/development/extensions-core/druid-lookups', '1c8'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-core/druid-pac4j',
        component: ComponentCreator('/docs/latest/development/extensions-core/druid-pac4j', '8a8'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-core/druid-ranger-security',
        component: ComponentCreator('/docs/latest/development/extensions-core/druid-ranger-security', '1a5'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-core/examples',
        component: ComponentCreator('/docs/latest/development/extensions-core/examples', '976'),
        exact: true
      },
      {
        path: '/docs/latest/development/extensions-core/google',
        component: ComponentCreator('/docs/latest/development/extensions-core/google', 'ac0'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-core/hdfs',
        component: ComponentCreator('/docs/latest/development/extensions-core/hdfs', '56c'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-core/kafka-extraction-namespace',
        component: ComponentCreator('/docs/latest/development/extensions-core/kafka-extraction-namespace', 'b53'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-core/kafka-ingestion',
        component: ComponentCreator('/docs/latest/development/extensions-core/kafka-ingestion', '36f'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-core/kafka-supervisor-operations',
        component: ComponentCreator('/docs/latest/development/extensions-core/kafka-supervisor-operations', 'dec'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-core/kafka-supervisor-reference',
        component: ComponentCreator('/docs/latest/development/extensions-core/kafka-supervisor-reference', '5fb'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-core/kinesis-ingestion',
        component: ComponentCreator('/docs/latest/development/extensions-core/kinesis-ingestion', 'b09'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-core/kubernetes',
        component: ComponentCreator('/docs/latest/development/extensions-core/kubernetes', 'c15'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-core/lookups-cached-global',
        component: ComponentCreator('/docs/latest/development/extensions-core/lookups-cached-global', '221'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-core/mysql',
        component: ComponentCreator('/docs/latest/development/extensions-core/mysql', '4a1'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-core/orc',
        component: ComponentCreator('/docs/latest/development/extensions-core/orc', '335'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-core/parquet',
        component: ComponentCreator('/docs/latest/development/extensions-core/parquet', 'eeb'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-core/postgresql',
        component: ComponentCreator('/docs/latest/development/extensions-core/postgresql', 'bec'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-core/protobuf',
        component: ComponentCreator('/docs/latest/development/extensions-core/protobuf', '49a'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-core/s3',
        component: ComponentCreator('/docs/latest/development/extensions-core/s3', 'bed'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-core/simple-client-sslcontext',
        component: ComponentCreator('/docs/latest/development/extensions-core/simple-client-sslcontext', 'eba'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-core/stats',
        component: ComponentCreator('/docs/latest/development/extensions-core/stats', 'b65'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/extensions-core/test-stats',
        component: ComponentCreator('/docs/latest/development/extensions-core/test-stats', 'ec9'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/geo',
        component: ComponentCreator('/docs/latest/development/geo', '021'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/javascript',
        component: ComponentCreator('/docs/latest/development/javascript', 'a26'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/modules',
        component: ComponentCreator('/docs/latest/development/modules', 'f74'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/overview',
        component: ComponentCreator('/docs/latest/development/overview', 'b0f'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/development/versioning',
        component: ComponentCreator('/docs/latest/development/versioning', '91d'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/ingestion/',
        component: ComponentCreator('/docs/latest/ingestion/', '584'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/ingestion/data-formats',
        component: ComponentCreator('/docs/latest/ingestion/data-formats', 'f46'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/ingestion/data-model',
        component: ComponentCreator('/docs/latest/ingestion/data-model', '697'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/ingestion/faq',
        component: ComponentCreator('/docs/latest/ingestion/faq', 'cb8'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/ingestion/hadoop',
        component: ComponentCreator('/docs/latest/ingestion/hadoop', '1da'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/ingestion/ingestion-spec',
        component: ComponentCreator('/docs/latest/ingestion/ingestion-spec', 'ffd'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/ingestion/migrate-from-firehose',
        component: ComponentCreator('/docs/latest/ingestion/migrate-from-firehose', '4f2'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/ingestion/native-batch',
        component: ComponentCreator('/docs/latest/ingestion/native-batch', '8d4'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/ingestion/native-batch-firehose',
        component: ComponentCreator('/docs/latest/ingestion/native-batch-firehose', 'f3e'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/ingestion/native-batch-input-sources',
        component: ComponentCreator('/docs/latest/ingestion/native-batch-input-sources', 'c39'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/ingestion/native-batch-simple-task',
        component: ComponentCreator('/docs/latest/ingestion/native-batch-simple-task', '398'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/ingestion/partitioning',
        component: ComponentCreator('/docs/latest/ingestion/partitioning', 'd81'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/ingestion/rollup',
        component: ComponentCreator('/docs/latest/ingestion/rollup', '090'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/ingestion/schema-design',
        component: ComponentCreator('/docs/latest/ingestion/schema-design', '42d'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/ingestion/standalone-realtime',
        component: ComponentCreator('/docs/latest/ingestion/standalone-realtime', '7c3'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/ingestion/tasks',
        component: ComponentCreator('/docs/latest/ingestion/tasks', 'f8f'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/ingestion/tranquility',
        component: ComponentCreator('/docs/latest/ingestion/tranquility', '77c'),
        exact: true
      },
      {
        path: '/docs/latest/misc/math-expr',
        component: ComponentCreator('/docs/latest/misc/math-expr', 'c94'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/misc/papers-and-talks',
        component: ComponentCreator('/docs/latest/misc/papers-and-talks', '904'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/multi-stage-query/',
        component: ComponentCreator('/docs/latest/multi-stage-query/', '9f4'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/multi-stage-query/api',
        component: ComponentCreator('/docs/latest/multi-stage-query/api', '3b6'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/multi-stage-query/concepts',
        component: ComponentCreator('/docs/latest/multi-stage-query/concepts', '5b4'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/multi-stage-query/examples',
        component: ComponentCreator('/docs/latest/multi-stage-query/examples', 'd62'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/multi-stage-query/known-issues',
        component: ComponentCreator('/docs/latest/multi-stage-query/known-issues', 'ca4'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/multi-stage-query/reference',
        component: ComponentCreator('/docs/latest/multi-stage-query/reference', '8da'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/multi-stage-query/security',
        component: ComponentCreator('/docs/latest/multi-stage-query/security', 'b4f'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/operations/alerts',
        component: ComponentCreator('/docs/latest/operations/alerts', '44b'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/operations/api-reference',
        component: ComponentCreator('/docs/latest/operations/api-reference', 'c76'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/operations/auth-ldap',
        component: ComponentCreator('/docs/latest/operations/auth-ldap', '7e1'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/operations/basic-cluster-tuning',
        component: ComponentCreator('/docs/latest/operations/basic-cluster-tuning', 'e6d'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/operations/clean-metadata-store',
        component: ComponentCreator('/docs/latest/operations/clean-metadata-store', 'b89'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/operations/deep-storage-migration',
        component: ComponentCreator('/docs/latest/operations/deep-storage-migration', '609'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/operations/dump-segment',
        component: ComponentCreator('/docs/latest/operations/dump-segment', '583'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/operations/dynamic-config-provider',
        component: ComponentCreator('/docs/latest/operations/dynamic-config-provider', '856'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/operations/export-metadata',
        component: ComponentCreator('/docs/latest/operations/export-metadata', '094'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/operations/getting-started',
        component: ComponentCreator('/docs/latest/operations/getting-started', '945'),
        exact: true
      },
      {
        path: '/docs/latest/operations/high-availability',
        component: ComponentCreator('/docs/latest/operations/high-availability', 'a3b'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/operations/http-compression',
        component: ComponentCreator('/docs/latest/operations/http-compression', 'b97'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/operations/insert-segment-to-db',
        component: ComponentCreator('/docs/latest/operations/insert-segment-to-db', 'b9c'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/operations/java',
        component: ComponentCreator('/docs/latest/operations/java', '83d'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/operations/kubernetes',
        component: ComponentCreator('/docs/latest/operations/kubernetes', 'f18'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/operations/metadata-migration',
        component: ComponentCreator('/docs/latest/operations/metadata-migration', '0a7'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/operations/metrics',
        component: ComponentCreator('/docs/latest/operations/metrics', '2fc'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/operations/mixed-workloads',
        component: ComponentCreator('/docs/latest/operations/mixed-workloads', 'fb3'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/operations/other-hadoop',
        component: ComponentCreator('/docs/latest/operations/other-hadoop', 'a7d'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/operations/password-provider',
        component: ComponentCreator('/docs/latest/operations/password-provider', 'f45'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/operations/pull-deps',
        component: ComponentCreator('/docs/latest/operations/pull-deps', '0d3'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/operations/request-logging',
        component: ComponentCreator('/docs/latest/operations/request-logging', '1eb'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/operations/reset-cluster',
        component: ComponentCreator('/docs/latest/operations/reset-cluster', '7f8'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/operations/rolling-updates',
        component: ComponentCreator('/docs/latest/operations/rolling-updates', 'ea0'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/operations/rule-configuration',
        component: ComponentCreator('/docs/latest/operations/rule-configuration', '019'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/operations/security-overview',
        component: ComponentCreator('/docs/latest/operations/security-overview', '0af'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/operations/security-user-auth',
        component: ComponentCreator('/docs/latest/operations/security-user-auth', '791'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/operations/segment-optimization',
        component: ComponentCreator('/docs/latest/operations/segment-optimization', 'd83'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/operations/single-server',
        component: ComponentCreator('/docs/latest/operations/single-server', 'fa7'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/operations/tls-support',
        component: ComponentCreator('/docs/latest/operations/tls-support', '8b5'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/operations/use_sbt_to_build_fat_jar',
        component: ComponentCreator('/docs/latest/operations/use_sbt_to_build_fat_jar', '6ef'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/operations/web-console',
        component: ComponentCreator('/docs/latest/operations/web-console', '353'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/querying/',
        component: ComponentCreator('/docs/latest/querying/', '882'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/querying/aggregations',
        component: ComponentCreator('/docs/latest/querying/aggregations', '9c5'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/querying/caching',
        component: ComponentCreator('/docs/latest/querying/caching', '00a'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/querying/datasource',
        component: ComponentCreator('/docs/latest/querying/datasource', '3ba'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/querying/datasourcemetadataquery',
        component: ComponentCreator('/docs/latest/querying/datasourcemetadataquery', 'd0a'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/querying/dimensionspecs',
        component: ComponentCreator('/docs/latest/querying/dimensionspecs', '7c1'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/querying/filters',
        component: ComponentCreator('/docs/latest/querying/filters', '188'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/querying/granularities',
        component: ComponentCreator('/docs/latest/querying/granularities', '3a0'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/querying/groupbyquery',
        component: ComponentCreator('/docs/latest/querying/groupbyquery', '690'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/querying/having',
        component: ComponentCreator('/docs/latest/querying/having', '74f'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/querying/hll-old',
        component: ComponentCreator('/docs/latest/querying/hll-old', '1af'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/querying/joins',
        component: ComponentCreator('/docs/latest/querying/joins', '92c'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/querying/limitspec',
        component: ComponentCreator('/docs/latest/querying/limitspec', 'ac1'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/querying/lookups',
        component: ComponentCreator('/docs/latest/querying/lookups', '385'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/querying/multi-value-dimensions',
        component: ComponentCreator('/docs/latest/querying/multi-value-dimensions', 'd97'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/querying/multitenancy',
        component: ComponentCreator('/docs/latest/querying/multitenancy', 'c28'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/querying/nested-columns',
        component: ComponentCreator('/docs/latest/querying/nested-columns', '34a'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/querying/post-aggregations',
        component: ComponentCreator('/docs/latest/querying/post-aggregations', '093'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/querying/query-context',
        component: ComponentCreator('/docs/latest/querying/query-context', 'd70'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/querying/query-execution',
        component: ComponentCreator('/docs/latest/querying/query-execution', '2a6'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/querying/scan-query',
        component: ComponentCreator('/docs/latest/querying/scan-query', '1fe'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/querying/searchquery',
        component: ComponentCreator('/docs/latest/querying/searchquery', '5f6'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/querying/segmentmetadataquery',
        component: ComponentCreator('/docs/latest/querying/segmentmetadataquery', '4cb'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/querying/select-query',
        component: ComponentCreator('/docs/latest/querying/select-query', '1fe'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/querying/sorting-orders',
        component: ComponentCreator('/docs/latest/querying/sorting-orders', '827'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/querying/sql',
        component: ComponentCreator('/docs/latest/querying/sql', '816'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/querying/sql-aggregations',
        component: ComponentCreator('/docs/latest/querying/sql-aggregations', '4d9'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/querying/sql-api',
        component: ComponentCreator('/docs/latest/querying/sql-api', '5bd'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/querying/sql-array-functions',
        component: ComponentCreator('/docs/latest/querying/sql-array-functions', '22d'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/querying/sql-data-types',
        component: ComponentCreator('/docs/latest/querying/sql-data-types', '786'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/querying/sql-functions',
        component: ComponentCreator('/docs/latest/querying/sql-functions', 'a45'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/querying/sql-jdbc',
        component: ComponentCreator('/docs/latest/querying/sql-jdbc', '2f9'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/querying/sql-json-functions',
        component: ComponentCreator('/docs/latest/querying/sql-json-functions', 'd27'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/querying/sql-metadata-tables',
        component: ComponentCreator('/docs/latest/querying/sql-metadata-tables', '863'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/querying/sql-multivalue-string-functions',
        component: ComponentCreator('/docs/latest/querying/sql-multivalue-string-functions', '733'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/querying/sql-operators',
        component: ComponentCreator('/docs/latest/querying/sql-operators', '6ed'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/querying/sql-query-context',
        component: ComponentCreator('/docs/latest/querying/sql-query-context', '9c1'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/querying/sql-scalar',
        component: ComponentCreator('/docs/latest/querying/sql-scalar', '3e4'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/querying/sql-translation',
        component: ComponentCreator('/docs/latest/querying/sql-translation', 'b8a'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/querying/timeboundaryquery',
        component: ComponentCreator('/docs/latest/querying/timeboundaryquery', '2dc'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/querying/timeseriesquery',
        component: ComponentCreator('/docs/latest/querying/timeseriesquery', '96e'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/querying/topnmetricspec',
        component: ComponentCreator('/docs/latest/querying/topnmetricspec', '87a'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/querying/topnquery',
        component: ComponentCreator('/docs/latest/querying/topnquery', '3e9'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/querying/troubleshooting',
        component: ComponentCreator('/docs/latest/querying/troubleshooting', '8f7'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/querying/using-caching',
        component: ComponentCreator('/docs/latest/querying/using-caching', '4c8'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/querying/virtual-columns',
        component: ComponentCreator('/docs/latest/querying/virtual-columns', 'f5e'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/tutorials/',
        component: ComponentCreator('/docs/latest/tutorials/', '676'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/tutorials/cluster',
        component: ComponentCreator('/docs/latest/tutorials/cluster', '555'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/tutorials/docker',
        component: ComponentCreator('/docs/latest/tutorials/docker', '534'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/tutorials/tutorial-batch',
        component: ComponentCreator('/docs/latest/tutorials/tutorial-batch', '20d'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/tutorials/tutorial-batch-hadoop',
        component: ComponentCreator('/docs/latest/tutorials/tutorial-batch-hadoop', '33d'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/tutorials/tutorial-batch-native',
        component: ComponentCreator('/docs/latest/tutorials/tutorial-batch-native', 'e31'),
        exact: true
      },
      {
        path: '/docs/latest/tutorials/tutorial-compaction',
        component: ComponentCreator('/docs/latest/tutorials/tutorial-compaction', '124'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/tutorials/tutorial-delete-data',
        component: ComponentCreator('/docs/latest/tutorials/tutorial-delete-data', '9fa'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/tutorials/tutorial-ingestion-spec',
        component: ComponentCreator('/docs/latest/tutorials/tutorial-ingestion-spec', 'e6b'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/tutorials/tutorial-jdbc',
        component: ComponentCreator('/docs/latest/tutorials/tutorial-jdbc', 'ded'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/tutorials/tutorial-jupyter-docker',
        component: ComponentCreator('/docs/latest/tutorials/tutorial-jupyter-docker', '755'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/tutorials/tutorial-jupyter-index',
        component: ComponentCreator('/docs/latest/tutorials/tutorial-jupyter-index', '9f1'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/tutorials/tutorial-kafka',
        component: ComponentCreator('/docs/latest/tutorials/tutorial-kafka', 'cc2'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/tutorials/tutorial-kerberos-hadoop',
        component: ComponentCreator('/docs/latest/tutorials/tutorial-kerberos-hadoop', '414'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/tutorials/tutorial-msq-convert-spec',
        component: ComponentCreator('/docs/latest/tutorials/tutorial-msq-convert-spec', '6a4'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/tutorials/tutorial-msq-extern',
        component: ComponentCreator('/docs/latest/tutorials/tutorial-msq-extern', 'e2b'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/tutorials/tutorial-query',
        component: ComponentCreator('/docs/latest/tutorials/tutorial-query', 'd68'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/tutorials/tutorial-retention',
        component: ComponentCreator('/docs/latest/tutorials/tutorial-retention', '40b'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/tutorials/tutorial-rollup',
        component: ComponentCreator('/docs/latest/tutorials/tutorial-rollup', '3ce'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/tutorials/tutorial-sketches-theta',
        component: ComponentCreator('/docs/latest/tutorials/tutorial-sketches-theta', '7a1'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/tutorials/tutorial-sql-query-view',
        component: ComponentCreator('/docs/latest/tutorials/tutorial-sql-query-view', 'f5b'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/tutorials/tutorial-transform-spec',
        component: ComponentCreator('/docs/latest/tutorials/tutorial-transform-spec', '294'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/tutorials/tutorial-unnest-arrays',
        component: ComponentCreator('/docs/latest/tutorials/tutorial-unnest-arrays', 'c24'),
        exact: true,
        sidebar: "docs"
      },
      {
        path: '/docs/latest/tutorials/tutorial-update-data',
        component: ComponentCreator('/docs/latest/tutorials/tutorial-update-data', 'dc4'),
        exact: true,
        sidebar: "docs"
      }
    ]
  },
  {
    path: '/',
    component: ComponentCreator('/', 'fac'),
    exact: true
  },
  {
    path: '*',
    component: ComponentCreator('*'),
  },
];
