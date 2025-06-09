/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import type { JsonCompletionRule } from '../../utils';

/**
 * Determines if a type is a supervisor type based on the documentation:
 * "The supervisor type. For streaming ingestion, this can be either kafka, kinesis, or rabbit. 
 * For automatic compaction, set the type to autocompact."
 */
function isSupervisorType(type: string): boolean {
  return type === 'kafka' || type === 'kinesis' || type === 'rabbit' || type === 'autocompact';
}

export const INGESTION_SPEC_COMPLETIONS: JsonCompletionRule[] = [
  // Root level properties (task and supervisor specs)
  {
    path: '$',
    isObject: true,
    completions: [
      { value: 'type', documentation: 'Type of ingestion task or supervisor' },
      { value: 'spec', documentation: 'Specification for the ingestion task or supervisor' },
    ],
  },
  // Supervisor-only root level properties
  {
    path: '$',
    isObject: true,
    condition: (obj) => isSupervisorType(obj.type),
    completions: [
      { value: 'suspended', documentation: 'Whether the supervisor is suspended (supervisor only)' },
    ],
  },
  // Type values for tasks
  {
    path: '$.type',
    completions: [
      { value: 'index_parallel', documentation: 'Native batch ingestion (parallel)' },
      { value: 'index', documentation: 'Native batch ingestion (single task)' },
      { value: 'index_hadoop', documentation: 'Hadoop-based batch ingestion' },
      { value: 'kafka', documentation: 'Kafka supervisor for streaming ingestion' },
      { value: 'kinesis', documentation: 'Kinesis supervisor for streaming ingestion' },
      { value: 'rabbit', documentation: 'RabbitMQ supervisor for streaming ingestion' },
      { value: 'autocompact', documentation: 'Auto-compaction supervisor' },
    ],
  },
  // suspended values (supervisor only)
  {
    path: '$.suspended',
    completions: [
      { value: 'false', documentation: 'Supervisor is running (default)' },
      { value: 'true', documentation: 'Supervisor is suspended' },
    ],
  },
  // spec object properties
  {
    path: '$.spec',
    isObject: true,
    completions: [
      { value: 'dataSchema', documentation: 'Schema configuration for ingestion' },
      { value: 'ioConfig', documentation: 'Input/output configuration' },
      { value: 'tuningConfig', documentation: 'Performance and tuning configuration' },
    ],
  },
  // dataSchema object properties
  {
    path: '$.spec.dataSchema',
    isObject: true,
    completions: [
      { value: 'dataSource', documentation: 'Name of the datasource to ingest into' },
      { value: 'timestampSpec', documentation: 'Primary timestamp configuration' },
      { value: 'dimensionsSpec', documentation: 'Dimensions configuration' },
      { value: 'metricsSpec', documentation: 'Metrics and aggregators configuration' },
      { value: 'granularitySpec', documentation: 'Granularity and rollup configuration' },
      { value: 'transformSpec', documentation: 'Transform and filter configuration' },
    ],
  },
  // timestampSpec object properties
  {
    path: '$.spec.dataSchema.timestampSpec',
    isObject: true,
    completions: [
      { value: 'column', documentation: 'Input column containing the timestamp' },
      { value: 'format', documentation: 'Format of the timestamp' },
      { value: 'missingValue', documentation: 'Default timestamp for missing values' },
    ],
  },
  // timestampSpec.format values
  {
    path: '$.spec.dataSchema.timestampSpec.format',
    completions: [
      { value: 'auto', documentation: 'Automatically detect ISO or millis format (default)' },
      { value: 'iso', documentation: 'ISO8601 format with T separator' },
      { value: 'posix', documentation: 'Seconds since epoch' },
      { value: 'millis', documentation: 'Milliseconds since epoch' },
      { value: 'micro', documentation: 'Microseconds since epoch' },
      { value: 'nano', documentation: 'Nanoseconds since epoch' },
      { value: 'yyyy-MM-dd HH:mm:ss', documentation: 'Custom Joda format' },
      { value: 'yyyy-MM-dd', documentation: 'Date only format' },
    ],
  },
  // timestampSpec.column common values
  {
    path: '$.spec.dataSchema.timestampSpec.column',
    completions: [
      { value: 'timestamp', documentation: 'Default timestamp column name' },
      { value: '__time', documentation: 'Druid internal timestamp column' },
      { value: 'time', documentation: 'Common timestamp column name' },
      { value: 'event_time', documentation: 'Common event time column name' },
      { value: 'created_at', documentation: 'Common creation time column name' },
    ],
  },
  // dimensionsSpec object properties
  {
    path: '$.spec.dataSchema.dimensionsSpec',
    isObject: true,
    completions: [
      { value: 'dimensions', documentation: 'List of dimension specifications' },
      { value: 'dimensionExclusions', documentation: 'Dimensions to exclude from ingestion' },
      { value: 'spatialDimensions', documentation: 'Spatial dimension specifications' },
      { value: 'includeAllDimensions', documentation: 'Include all discovered dimensions' },
      { value: 'useSchemaDiscovery', documentation: 'Enable automatic schema discovery' },
      { value: 'forceSegmentSortByTime', documentation: 'Force segments to be sorted by time first' },
    ],
  },
  // useSchemaDiscovery values
  {
    path: '$.spec.dataSchema.dimensionsSpec.useSchemaDiscovery',
    completions: [
      { value: 'true', documentation: 'Enable automatic type detection and schema discovery' },
      { value: 'false', documentation: 'Use manual dimension specification (default)' },
    ],
  },
  // includeAllDimensions values
  {
    path: '$.spec.dataSchema.dimensionsSpec.includeAllDimensions',
    completions: [
      { value: 'true', documentation: 'Include both explicit and discovered dimensions' },
      { value: 'false', documentation: 'Include only explicit dimensions (default)' },
    ],
  },
  // forceSegmentSortByTime values
  {
    path: '$.spec.dataSchema.dimensionsSpec.forceSegmentSortByTime',
    completions: [
      { value: 'true', documentation: 'Sort by __time first, then dimensions (default)' },
      { value: 'false', documentation: 'Sort by dimensions only (experimental)' },
    ],
  },
  // dimension object properties
  {
    path: '$.spec.dataSchema.dimensionsSpec.dimensions.[]',
    isObject: true,
    completions: [
      { value: 'type', documentation: 'Type of the dimension' },
      { value: 'name', documentation: 'Name of the dimension' },
      { value: 'createBitmapIndex', documentation: 'Whether to create bitmap index (string only)' },
      { value: 'multiValueHandling', documentation: 'How to handle multi-value fields (string only)' },
    ],
  },
  // dimension type values
  {
    path: '$.spec.dataSchema.dimensionsSpec.dimensions.[].type',
    completions: [
      { value: 'auto', documentation: 'Automatically detect type (schema discovery)' },
      { value: 'string', documentation: 'String dimension (default)' },
      { value: 'long', documentation: 'Long integer dimension' },
      { value: 'float', documentation: 'Float dimension' },
      { value: 'double', documentation: 'Double precision dimension' },
      { value: 'json', documentation: 'JSON/nested data dimension' },
    ],
  },
  // createBitmapIndex values
  {
    path: '$.spec.dataSchema.dimensionsSpec.dimensions.[].createBitmapIndex',
    completions: [
      { value: 'true', documentation: 'Create bitmap index (default, faster filtering)' },
      { value: 'false', documentation: 'No bitmap index (saves storage)' },
    ],
  },
  // multiValueHandling values
  {
    path: '$.spec.dataSchema.dimensionsSpec.dimensions.[].multiValueHandling',
    completions: [
      { value: 'sorted_array', documentation: 'Sort multi-values (default)' },
      { value: 'sorted_set', documentation: 'Sort and deduplicate multi-values' },
      { value: 'array', documentation: 'Keep multi-values as-is' },
    ],
  },
  // metricsSpec array properties
  {
    path: '$.spec.dataSchema.metricsSpec.[]',
    isObject: true,
    completions: [
      { value: 'type', documentation: 'Type of aggregator' },
      { value: 'name', documentation: 'Name of the metric' },
      { value: 'fieldName', documentation: 'Input field to aggregate' },
    ],
  },
  // metric aggregator types
  {
    path: '$.spec.dataSchema.metricsSpec.[].type',
    completions: [
      { value: 'count', documentation: 'Count of rows' },
      { value: 'longSum', documentation: 'Sum of long values' },
      { value: 'doubleSum', documentation: 'Sum of double values' },
      { value: 'longMin', documentation: 'Minimum of long values' },
      { value: 'longMax', documentation: 'Maximum of long values' },
      { value: 'doubleMin', documentation: 'Minimum of double values' },
      { value: 'doubleMax', documentation: 'Maximum of double values' },
      { value: 'longFirst', documentation: 'First long value seen' },
      { value: 'longLast', documentation: 'Last long value seen' },
      { value: 'doubleFirst', documentation: 'First double value seen' },
      { value: 'doubleLast', documentation: 'Last double value seen' },
      { value: 'thetaSketch', documentation: 'Theta sketch for approximate counting' },
      { value: 'HLLSketchBuild', documentation: 'HyperLogLog sketch for cardinality' },
      { value: 'quantilesDoublesSketch', documentation: 'Quantiles sketch for percentiles' },
    ],
  },
  // granularitySpec object properties
  {
    path: '$.spec.dataSchema.granularitySpec',
    isObject: true,
    completions: [
      { value: 'type', documentation: 'Type of granularity specification' },
      { value: 'segmentGranularity', documentation: 'Granularity for segment partitioning' },
      { value: 'queryGranularity', documentation: 'Granularity for timestamp truncation' },
      { value: 'rollup', documentation: 'Whether to enable rollup (aggregation)' },
      { value: 'intervals', documentation: 'Time intervals to process (batch only)' },
    ],
  },
  // granularitySpec.type values
  {
    path: '$.spec.dataSchema.granularitySpec.type',
    completions: [
      { value: 'uniform', documentation: 'Uniform granularity (default)' },
      { value: 'arbitrary', documentation: 'Arbitrary granularity (advanced)' },
    ],
  },
  // segmentGranularity values
  {
    path: '$.spec.dataSchema.granularitySpec.segmentGranularity',
    completions: [
      { value: 'SECOND', documentation: 'Second-level segments' },
      { value: 'MINUTE', documentation: 'Minute-level segments' },
      { value: 'HOUR', documentation: 'Hourly segments' },
      { value: 'DAY', documentation: 'Daily segments (common choice)' },
      { value: 'WEEK', documentation: 'Weekly segments (not recommended)' },
      { value: 'MONTH', documentation: 'Monthly segments' },
      { value: 'QUARTER', documentation: 'Quarterly segments' },
      { value: 'YEAR', documentation: 'Yearly segments' },
      { value: 'ALL', documentation: 'Single segment for all data' },
    ],
  },
  // queryGranularity values
  {
    path: '$.spec.dataSchema.granularitySpec.queryGranularity',
    completions: [
      { value: 'NONE', documentation: 'No truncation (millisecond precision)' },
      { value: 'SECOND', documentation: 'Second-level truncation' },
      { value: 'MINUTE', documentation: 'Minute-level truncation' },
      { value: 'HOUR', documentation: 'Hour-level truncation' },
      { value: 'DAY', documentation: 'Day-level truncation' },
      { value: 'WEEK', documentation: 'Week-level truncation' },
      { value: 'MONTH', documentation: 'Month-level truncation' },
      { value: 'QUARTER', documentation: 'Quarter-level truncation' },
      { value: 'YEAR', documentation: 'Year-level truncation' },
    ],
  },
  // rollup values
  {
    path: '$.spec.dataSchema.granularitySpec.rollup',
    completions: [
      { value: 'true', documentation: 'Enable rollup (aggregate identical rows)' },
      { value: 'false', documentation: 'Disable rollup (store raw data)' },
    ],
  },
  // transformSpec object properties
  {
    path: '$.spec.dataSchema.transformSpec',
    isObject: true,
    completions: [
      { value: 'transforms', documentation: 'List of transform expressions' },
      { value: 'filter', documentation: 'Filter to apply during ingestion' },
    ],
  },
  // transform object properties
  {
    path: '$.spec.dataSchema.transformSpec.transforms.[]',
    isObject: true,
    completions: [
      { value: 'type', documentation: 'Type of transform' },
      { value: 'name', documentation: 'Name of the output field' },
      { value: 'expression', documentation: 'Transform expression' },
    ],
  },
  // transform type values
  {
    path: '$.spec.dataSchema.transformSpec.transforms.[].type',
    completions: [
      { value: 'expression', documentation: 'Expression-based transform' },
    ],
  },
  // filter object properties
  {
    path: '$.spec.dataSchema.transformSpec.filter',
    isObject: true,
    completions: [
      { value: 'type', documentation: 'Type of filter' },
      { value: 'dimension', documentation: 'Dimension to filter on' },
      { value: 'value', documentation: 'Value to filter for' },
      { value: 'values', documentation: 'List of values to filter for' },
      { value: 'fields', documentation: 'List of sub-filters (logical filters)' },
    ],
  },
  // filter type values
  {
    path: '$.spec.dataSchema.transformSpec.filter.type',
    completions: [
      { value: 'selector', documentation: 'Exact match filter' },
      { value: 'in', documentation: 'Match any of multiple values' },
      { value: 'like', documentation: 'Pattern matching filter' },
      { value: 'regex', documentation: 'Regular expression filter' },
      { value: 'range', documentation: 'Numeric range filter' },
      { value: 'and', documentation: 'Logical AND filter' },
      { value: 'or', documentation: 'Logical OR filter' },
      { value: 'not', documentation: 'Logical NOT filter' },
    ],
  },
  // ioConfig object properties (general)
  {
    path: '$.spec.ioConfig',
    isObject: true,
    completions: [
      { value: 'type', documentation: 'Type of I/O configuration' },
      { value: 'inputSource', documentation: 'Data input source configuration' },
      { value: 'inputFormat', documentation: 'Input data format configuration' },
    ],
  },
  // ioConfig type values
  {
    path: '$.spec.ioConfig.type',
    completions: [
      { value: 'index_parallel', documentation: 'Parallel batch ingestion' },
      { value: 'index', documentation: 'Single task batch ingestion' },
      { value: 'hadoop', documentation: 'Hadoop-based ingestion' },
      { value: 'kafka', documentation: 'Kafka streaming ingestion' },
      { value: 'kinesis', documentation: 'Kinesis streaming ingestion' },
      { value: 'rabbit', documentation: 'RabbitMQ streaming ingestion' },
    ],
  },
  // Batch ioConfig properties
  {
    path: '$.spec.ioConfig',
    isObject: true,
    condition: (obj) => obj.type === 'index_parallel' || obj.type === 'index',
    completions: [
      { value: 'firehose', documentation: 'Legacy data input (deprecated)' },
      { value: 'appendToExisting', documentation: 'Whether to append to existing segments' },
      { value: 'dropExisting', documentation: 'Whether to drop existing segments' },
    ],
  },
  // Streaming ioConfig properties (Kafka/Kinesis/Rabbit)
  {
    path: '$.spec.ioConfig',
    isObject: true,
    condition: (obj) => isSupervisorType(obj.type) && obj.type !== 'autocompact',
    completions: [
      { value: 'taskCount', documentation: 'Number of reading tasks per replica' },
      { value: 'replicas', documentation: 'Number of replica task sets' },
      { value: 'taskDuration', documentation: 'Duration before tasks stop reading' },
      { value: 'startDelay', documentation: 'Delay before supervisor starts managing tasks' },
      { value: 'period', documentation: 'How often supervisor executes management logic' },
      { value: 'completionTimeout', documentation: 'Timeout for task completion' },
      { value: 'autoScalerConfig', documentation: 'Auto-scaling configuration' },
    ],
  },
  // Kafka-specific ioConfig properties
  {
    path: '$.spec.ioConfig',
    isObject: true,
    condition: (obj) => obj.type === 'kafka',
    completions: [
      { value: 'topic', documentation: 'Kafka topic to consume from' },
      { value: 'consumerProperties', documentation: 'Kafka consumer properties' },
      { value: 'useEarliestOffset', documentation: 'Start from earliest offset when no stored offset' },
    ],
  },
  // Kinesis-specific ioConfig properties
  {
    path: '$.spec.ioConfig',
    isObject: true,
    condition: (obj) => obj.type === 'kinesis',
    completions: [
      { value: 'stream', documentation: 'Kinesis stream to consume from' },
      { value: 'endpoint', documentation: 'Kinesis endpoint URL' },
      { value: 'useEarliestSequenceNumber', documentation: 'Start from earliest when no stored sequence number' },
    ],
  },
  // appendToExisting values
  {
    path: '$.spec.ioConfig.appendToExisting',
    completions: [
      { value: 'false', documentation: 'Overwrite existing data (default)' },
      { value: 'true', documentation: 'Append to existing data' },
    ],
  },
  // dropExisting values
  {
    path: '$.spec.ioConfig.dropExisting',
    completions: [
      { value: 'false', documentation: 'Keep existing segments (default)' },
      { value: 'true', documentation: 'Drop existing segments in intervals' },
    ],
  },
  // useEarliestOffset values
  {
    path: '$.spec.ioConfig.useEarliestOffset',
    completions: [
      { value: 'false', documentation: 'Use latest offset when no stored offset (default)' },
      { value: 'true', documentation: 'Use earliest offset when no stored offset' },
    ],
  },
  // useEarliestSequenceNumber values
  {
    path: '$.spec.ioConfig.useEarliestSequenceNumber',
    completions: [
      { value: 'false', documentation: 'Use latest sequence number (default)' },
      { value: 'true', documentation: 'Use earliest sequence number' },
    ],
  },
  // inputSource object properties
  {
    path: '$.spec.ioConfig.inputSource',
    isObject: true,
    completions: [
      { value: 'type', documentation: 'Type of input source' },
    ],
  },
  // inputSource type values
  {
    path: '$.spec.ioConfig.inputSource.type',
    completions: [
      { value: 'local', documentation: 'Local file system' },
      { value: 'http', documentation: 'HTTP/HTTPS URLs' },
      { value: 's3', documentation: 'Amazon S3' },
      { value: 'gs', documentation: 'Google Cloud Storage' },
      { value: 'azure', documentation: 'Azure Blob Storage' },
      { value: 'hdfs', documentation: 'Hadoop Distributed File System' },
      { value: 'druid', documentation: 'Re-index from existing Druid datasource' },
      { value: 'inline', documentation: 'Inline data in the spec' },
      { value: 'combining', documentation: 'Combine multiple input sources' },
    ],
  },
  // Local input source properties
  {
    path: '$.spec.ioConfig.inputSource',
    isObject: true,
    condition: (obj) => obj.type === 'local',
    completions: [
      { value: 'baseDir', documentation: 'Base directory path' },
      { value: 'filter', documentation: 'File filter pattern' },
      { value: 'files', documentation: 'List of specific file paths' },
    ],
  },
  // HTTP input source properties
  {
    path: '$.spec.ioConfig.inputSource',
    isObject: true,
    condition: (obj) => obj.type === 'http',
    completions: [
      { value: 'uris', documentation: 'List of HTTP/HTTPS URIs' },
      { value: 'httpAuthenticationUsername', documentation: 'HTTP authentication username' },
      { value: 'httpAuthenticationPassword', documentation: 'HTTP authentication password' },
    ],
  },
  // S3 input source properties
  {
    path: '$.spec.ioConfig.inputSource',
    isObject: true,
    condition: (obj) => obj.type === 's3',
    completions: [
      { value: 'uris', documentation: 'List of S3 URIs' },
      { value: 'prefixes', documentation: 'List of S3 prefixes' },
      { value: 'objects', documentation: 'List of S3 objects with bucket and key' },
      { value: 'properties', documentation: 'S3 connection properties' },
    ],
  },
  // inputFormat object properties
  {
    path: '$.spec.ioConfig.inputFormat',
    isObject: true,
    completions: [
      { value: 'type', documentation: 'Type of input format' },
    ],
  },
  // inputFormat type values
  {
    path: '$.spec.ioConfig.inputFormat.type',
    completions: [
      { value: 'json', documentation: 'JSON format' },
      { value: 'csv', documentation: 'Comma-separated values' },
      { value: 'tsv', documentation: 'Tab-separated values' },
      { value: 'delimited', documentation: 'Custom delimiter format' },
      { value: 'regex', documentation: 'Regular expression parsing' },
      { value: 'avro_ocf', documentation: 'Avro Object Container File' },
      { value: 'avro_stream', documentation: 'Avro streaming format' },
      { value: 'parquet', documentation: 'Apache Parquet' },
      { value: 'orc', documentation: 'Apache ORC' },
      { value: 'protobuf', documentation: 'Protocol Buffers' },
    ],
  },
  // JSON input format properties
  {
    path: '$.spec.ioConfig.inputFormat',
    isObject: true,
    condition: (obj) => obj.type === 'json',
    completions: [
      { value: 'flattenSpec', documentation: 'Specification for flattening nested JSON' },
      { value: 'featureSpec', documentation: 'JSON parsing features' },
    ],
  },
  // CSV/TSV input format properties
  {
    path: '$.spec.ioConfig.inputFormat',
    isObject: true,
    condition: (obj) => obj.type === 'csv' || obj.type === 'tsv',
    completions: [
      { value: 'columns', documentation: 'Column names' },
      { value: 'delimiter', documentation: 'Field delimiter' },
      { value: 'listDelimiter', documentation: 'Multi-value delimiter' },
      { value: 'hasHeaderRow', documentation: 'Whether first row contains headers' },
      { value: 'skipHeaderRows', documentation: 'Number of header rows to skip' },
    ],
  },
  // flattenSpec object properties
  {
    path: '$.spec.ioConfig.inputFormat.flattenSpec',
    isObject: true,
    completions: [
      { value: 'useFieldDiscovery', documentation: 'Automatically discover flattened fields' },
      { value: 'fields', documentation: 'Manual field specifications' },
    ],
  },
  // useFieldDiscovery values
  {
    path: '$.spec.ioConfig.inputFormat.flattenSpec.useFieldDiscovery',
    completions: [
      { value: 'true', documentation: 'Automatically discover nested fields' },
      { value: 'false', documentation: 'Use only manually specified fields (default)' },
    ],
  },
  // flatten field object properties
  {
    path: '$.spec.ioConfig.inputFormat.flattenSpec.fields.[]',
    isObject: true,
    completions: [
      { value: 'type', documentation: 'Type of field extraction' },
      { value: 'name', documentation: 'Output field name' },
      { value: 'expr', documentation: 'JSONPath expression for extraction' },
    ],
  },
  // flatten field type values
  {
    path: '$.spec.ioConfig.inputFormat.flattenSpec.fields.[].type',
    completions: [
      { value: 'root', documentation: 'Root-level field' },
      { value: 'path', documentation: 'JSONPath extraction' },
      { value: 'jq', documentation: 'JQ-style extraction' },
      { value: 'tree', documentation: 'Tree-based extraction' },
    ],
  },
  // tuningConfig object properties (general)
  {
    path: '$.spec.tuningConfig',
    isObject: true,
    completions: [
      { value: 'type', documentation: 'Type of tuning configuration' },
      { value: 'maxRowsInMemory', documentation: 'Rows to accumulate before persisting' },
      { value: 'maxBytesInMemory', documentation: 'Bytes to accumulate before persisting' },
      { value: 'indexSpec', documentation: 'Segment storage format options' },
      { value: 'skipBytesInMemoryOverheadCheck', documentation: 'Skip overhead calculation' },
    ],
  },
  // tuningConfig type values
  {
    path: '$.spec.tuningConfig.type',
    completions: [
      { value: 'index_parallel', documentation: 'Parallel batch tuning' },
      { value: 'index', documentation: 'Single task batch tuning' },
      { value: 'hadoop', documentation: 'Hadoop batch tuning' },
      { value: 'kafka', documentation: 'Kafka streaming tuning' },
      { value: 'kinesis', documentation: 'Kinesis streaming tuning' },
      { value: 'rabbit', documentation: 'RabbitMQ streaming tuning' },
    ],
  },
  // Batch tuningConfig properties
  {
    path: '$.spec.tuningConfig',
    isObject: true,
    condition: (obj) => obj.type === 'index_parallel' || obj.type === 'index',
    completions: [
      { value: 'partitionsSpec', documentation: 'Partitioning configuration' },
      { value: 'maxNumConcurrentSubTasks', documentation: 'Maximum concurrent subtasks' },
      { value: 'maxRetry', documentation: 'Maximum task retries' },
      { value: 'taskStatusCheckPeriodMs', documentation: 'Task status check period' },
      { value: 'splitHintSpec', documentation: 'Input splitting hints' },
    ],
  },
  // Streaming tuningConfig properties
  {
    path: '$.spec.tuningConfig',
    isObject: true,
    condition: (obj) => isSupervisorType(obj.type) && obj.type !== 'autocompact',
    completions: [
      { value: 'maxRowsPerSegment', documentation: 'Maximum rows per segment' },
      { value: 'maxTotalRows', documentation: 'Maximum total rows before handoff' },
      { value: 'intermediatePersistPeriod', documentation: 'Period between intermediate persists' },
      { value: 'maxPendingPersists', documentation: 'Maximum pending persist operations' },
      { value: 'reportParseExceptions', documentation: 'Whether to report parse exceptions' },
      { value: 'handoffConditionTimeout', documentation: 'Timeout for segment handoff' },
      { value: 'resetOffsetAutomatically', documentation: 'Reset offset on unavailable offset' },
    ],
  },
  // partitionsSpec object properties
  {
    path: '$.spec.tuningConfig.partitionsSpec',
    isObject: true,
    completions: [
      { value: 'type', documentation: 'Type of partitioning' },
    ],
  },
  // partitionsSpec type values
  {
    path: '$.spec.tuningConfig.partitionsSpec.type',
    completions: [
      { value: 'dynamic', documentation: 'Dynamic partitioning (best-effort rollup)' },
      { value: 'hashed', documentation: 'Hash-based partitioning (perfect rollup)' },
      { value: 'range', documentation: 'Range-based partitioning (perfect rollup)' },
      { value: 'single_dim', documentation: 'Single dimension partitioning (legacy)' },
    ],
  },
  // dynamic partitioning properties
  {
    path: '$.spec.tuningConfig.partitionsSpec',
    isObject: true,
    condition: (obj) => obj.type === 'dynamic',
    completions: [
      { value: 'maxRowsPerSegment', documentation: 'Target rows per segment' },
      { value: 'maxTotalRows', documentation: 'Maximum total rows in memory' },
    ],
  },
  // hashed partitioning properties
  {
    path: '$.spec.tuningConfig.partitionsSpec',
    isObject: true,
    condition: (obj) => obj.type === 'hashed',
    completions: [
      { value: 'targetRowsPerSegment', documentation: 'Target rows per segment' },
      { value: 'numShards', documentation: 'Fixed number of shards' },
      { value: 'partitionDimensions', documentation: 'Dimensions to partition on' },
    ],
  },
  // range partitioning properties
  {
    path: '$.spec.tuningConfig.partitionsSpec',
    isObject: true,
    condition: (obj) => obj.type === 'range',
    completions: [
      { value: 'targetRowsPerSegment', documentation: 'Target rows per segment' },
      { value: 'partitionDimensions', documentation: 'Dimensions to partition on (required)' },
      { value: 'assumeGrouped', documentation: 'Assume input data is grouped' },
    ],
  },
  // indexSpec object properties
  {
    path: '$.spec.tuningConfig.indexSpec',
    isObject: true,
    completions: [
      { value: 'bitmap', documentation: 'Bitmap index configuration' },
      { value: 'dimensionCompression', documentation: 'Dimension compression format' },
      { value: 'metricCompression', documentation: 'Metric compression format' },
      { value: 'longEncoding', documentation: 'Long value encoding format' },
      { value: 'stringDictionaryEncoding', documentation: 'String dictionary encoding' },
      { value: 'jsonCompression', documentation: 'JSON column compression' },
    ],
  },
  // bitmap configuration
  {
    path: '$.spec.tuningConfig.indexSpec.bitmap',
    isObject: true,
    completions: [
      { value: 'type', documentation: 'Bitmap index type' },
    ],
  },
  // bitmap type values
  {
    path: '$.spec.tuningConfig.indexSpec.bitmap.type',
    completions: [
      { value: 'roaring', documentation: 'Roaring bitmap (default, recommended)' },
      { value: 'concise', documentation: 'Concise bitmap (legacy)' },
    ],
  },
  // compression format values
  {
    path: /^.*\.(dimensionCompression|metricCompression|jsonCompression)$/,
    completions: [
      { value: 'lz4', documentation: 'LZ4 compression (default, fast)' },
      { value: 'lzf', documentation: 'LZF compression' },
      { value: 'zstd', documentation: 'Zstandard compression (better ratio)' },
      { value: 'uncompressed', documentation: 'No compression' },
    ],
  },
  // longEncoding values
  {
    path: '$.spec.tuningConfig.indexSpec.longEncoding',
    completions: [
      { value: 'longs', documentation: 'Store as 8-byte longs (default)' },
      { value: 'auto', documentation: 'Use offset/lookup table based on cardinality' },
    ],
  },
  // stringDictionaryEncoding
  {
    path: '$.spec.tuningConfig.indexSpec.stringDictionaryEncoding',
    isObject: true,
    completions: [
      { value: 'type', documentation: 'String dictionary encoding type' },
      { value: 'bucketSize', documentation: 'Bucket size for front coding' },
      { value: 'formatVersion', documentation: 'Format version for front coding' },
    ],
  },
  // stringDictionaryEncoding type values
  {
    path: '$.spec.tuningConfig.indexSpec.stringDictionaryEncoding.type',
    completions: [
      { value: 'utf8', documentation: 'UTF-8 encoding (default)' },
      { value: 'frontCoded', documentation: 'Front-coded dictionary (experimental)' },
    ],
  },
  // Common numeric values for row counts
  {
    path: /^.*\.(?:maxRowsInMemory|maxRowsPerSegment|targetRowsPerSegment)$/,
    completions: [
      { value: '75000', documentation: '75K rows (small segments)' },
      { value: '150000', documentation: '150K rows (streaming default)' },
      { value: '1000000', documentation: '1M rows (medium segments)' },
      { value: '5000000', documentation: '5M rows (recommended default)' },
      { value: '10000000', documentation: '10M rows (large segments)' },
    ],
  },
  // Common duration values
  {
    path: /^.*\.(?:taskDuration|startDelay|period|completionTimeout|intermediatePersistPeriod)$/,
    completions: [
      { value: 'PT5S', documentation: '5 seconds' },
      { value: 'PT30S', documentation: '30 seconds' },
      { value: 'PT1M', documentation: '1 minute' },
      { value: 'PT10M', documentation: '10 minutes' },
      { value: 'PT30M', documentation: '30 minutes' },
      { value: 'PT1H', documentation: '1 hour' },
      { value: 'PT2H', documentation: '2 hours' },
    ],
  },
  // taskCount/replicas values
  {
    path: /^.*\.(?:taskCount|replicas)$/,
    completions: [
      { value: '1', documentation: '1 task/replica (default)' },
      { value: '2', documentation: '2 tasks/replicas' },
      { value: '4', documentation: '4 tasks/replicas' },
      { value: '8', documentation: '8 tasks/replicas' },
    ],
  },
  // Boolean values
  {
    path: /^.*\.(?:reportParseExceptions|resetOffsetAutomatically|skipBytesInMemoryOverheadCheck)$/,
    completions: [
      { value: 'false', documentation: 'Disabled (default)' },
      { value: 'true', documentation: 'Enabled' },
    ],
  },
  // autoScalerConfig object properties
  {
    path: '$.spec.ioConfig.autoScalerConfig',
    isObject: true,
    completions: [
      { value: 'enableTaskAutoScaler', documentation: 'Enable autoscaling' },
      { value: 'taskCountMax', documentation: 'Maximum task count' },
      { value: 'taskCountMin', documentation: 'Minimum task count' },
      { value: 'taskCountStart', documentation: 'Starting task count' },
      { value: 'minTriggerScaleActionFrequencyMillis', documentation: 'Minimum time between scale actions' },
      { value: 'autoScalerStrategy', documentation: 'Autoscaler strategy' },
    ],
  },
  // enableTaskAutoScaler values
  {
    path: '$.spec.ioConfig.autoScalerConfig.enableTaskAutoScaler',
    completions: [
      { value: 'true', documentation: 'Enable task autoscaling' },
      { value: 'false', documentation: 'Disable task autoscaling (default)' },
    ],
  },
  // autoScalerStrategy values
  {
    path: '$.spec.ioConfig.autoScalerConfig.autoScalerStrategy',
    completions: [
      { value: 'lagBased', documentation: 'Scale based on consumer lag' },
    ],
  },
];