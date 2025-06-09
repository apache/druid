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

export const LOOKUP_COMPLETIONS: JsonCompletionRule[] = [
  // Root level properties
  {
    path: '$',
    isObject: true,
    completions: [
      { value: 'type', documentation: 'Type of lookup extractor factory' },
    ],
  },
  // type values
  {
    path: '$.type',
    completions: [
      { value: 'map', documentation: 'Simple map-based lookup with key-value pairs' },
      { value: 'cachedNamespace', documentation: 'Lookup from external source with caching' },
      { value: 'kafka', documentation: 'Lookup from Kafka topic' },
    ],
  },
  // map type properties
  {
    path: '$',
    isObject: true,
    condition: (obj) => obj.type === 'map',
    completions: [
      { value: 'map', documentation: 'Key-value map for lookups' },
    ],
  },
  // cachedNamespace type properties
  {
    path: '$',
    isObject: true,
    condition: (obj) => obj.type === 'cachedNamespace',
    completions: [
      { value: 'extractionNamespace', documentation: 'Configuration for the data source' },
      { value: 'firstCacheTimeout', documentation: 'How long to wait (in ms) for the first cache population' },
      { value: 'injective', documentation: 'Whether the lookup is injective (keys and values are unique)' },
    ],
  },
  // extractionNamespace object properties
  {
    path: '$.extractionNamespace',
    isObject: true,
    completions: [
      { value: 'type', documentation: 'Type of extraction namespace' },
    ],
  },
  // extractionNamespace.type values
  {
    path: '$.extractionNamespace.type',
    completions: [
      { value: 'uri', documentation: 'Load lookup data from URI (file, hdfs, s3, gs)' },
      { value: 'jdbc', documentation: 'Load lookup data from JDBC database' },
    ],
  },
  // uri extraction namespace properties
  {
    path: '$.extractionNamespace',
    isObject: true,
    condition: (obj) => obj.type === 'uri',
    completions: [
      { value: 'uri', documentation: 'URI for the lookup file (file://, hdfs://, s3://, gs://)' },
      { value: 'uriPrefix', documentation: 'URI prefix for directory containing lookup files' },
      { value: 'fileRegex', documentation: 'Regex pattern for matching files under uriPrefix' },
      { value: 'namespaceParseSpec', documentation: 'Specification for parsing the lookup data' },
      { value: 'pollPeriod', documentation: 'Period between polling for updates (ISO 8601 duration)' },
    ],
  },
  // fileRegex default value
  {
    path: '$.extractionNamespace.fileRegex',
    completions: [
      { value: '.*', documentation: 'Match all files (default)' },
      { value: '.*\\.json', documentation: 'Match JSON files' },
      { value: '.*\\.csv', documentation: 'Match CSV files' },
      { value: '.*\\.tsv', documentation: 'Match TSV files' },
    ],
  },
  // namespaceParseSpec object properties
  {
    path: '$.extractionNamespace.namespaceParseSpec',
    isObject: true,
    completions: [
      { value: 'format', documentation: 'Format of the lookup data file' },
    ],
  },
  // namespaceParseSpec.format values
  {
    path: '$.extractionNamespace.namespaceParseSpec.format',
    completions: [
      { value: 'csv', documentation: 'Comma-separated values format' },
      { value: 'tsv', documentation: 'Tab-separated values format' },
      { value: 'simpleJson', documentation: 'Simple JSON format (line-delimited)' },
      { value: 'customJson', documentation: 'Custom JSON format with specified key/value fields' },
    ],
  },
  // CSV/TSV format properties
  {
    path: '$.extractionNamespace.namespaceParseSpec',
    isObject: true,
    condition: (obj) => obj.format === 'csv' || obj.format === 'tsv',
    completions: [
      { value: 'columns', documentation: 'List of column names in the file' },
      { value: 'keyColumn', documentation: 'Name of the column containing keys' },
      { value: 'valueColumn', documentation: 'Name of the column containing values' },
      { value: 'hasHeaderRow', documentation: 'Whether the file has a header row' },
      { value: 'skipHeaderRows', documentation: 'Number of header rows to skip' },
    ],
  },
  // TSV-specific properties
  {
    path: '$.extractionNamespace.namespaceParseSpec',
    isObject: true,
    condition: (obj) => obj.format === 'tsv',
    completions: [
      { value: 'delimiter', documentation: 'Field delimiter character' },
      { value: 'listDelimiter', documentation: 'List delimiter character' },
    ],
  },
  // TSV delimiter values
  {
    path: '$.extractionNamespace.namespaceParseSpec.delimiter',
    completions: [
      { value: '\t', documentation: 'Tab character (default)' },
      { value: ';', documentation: 'Semicolon' },
      { value: '|', documentation: 'Pipe character' },
      { value: '#', documentation: 'Hash character' },
    ],
  },
  // hasHeaderRow values
  {
    path: '$.extractionNamespace.namespaceParseSpec.hasHeaderRow',
    completions: [
      { value: 'true', documentation: 'File has header row with column names' },
      { value: 'false', documentation: 'File has no header row (default)' },
    ],
  },
  // customJson format properties
  {
    path: '$.extractionNamespace.namespaceParseSpec',
    isObject: true,
    condition: (obj) => obj.format === 'customJson',
    completions: [
      { value: 'keyFieldName', documentation: 'Name of the field containing keys' },
      { value: 'valueFieldName', documentation: 'Name of the field containing values' },
    ],
  },
  // JDBC extraction namespace properties
  {
    path: '$.extractionNamespace',
    isObject: true,
    condition: (obj) => obj.type === 'jdbc',
    completions: [
      { value: 'connectorConfig', documentation: 'Database connection configuration' },
      { value: 'table', documentation: 'Database table containing lookup data' },
      { value: 'keyColumn', documentation: 'Column containing lookup keys' },
      { value: 'valueColumn', documentation: 'Column containing lookup values' },
      { value: 'tsColumn', documentation: 'Column containing timestamps (optional)' },
      { value: 'filter', documentation: 'WHERE clause filter for the lookup query' },
      { value: 'pollPeriod', documentation: 'Period between polling for updates (ISO 8601 duration)' },
      { value: 'jitterSeconds', documentation: 'Random jitter to add to polling (in seconds)' },
      { value: 'loadTimeoutSeconds', documentation: 'Timeout for loading lookup data (in seconds)' },
      { value: 'maxHeapPercentage', documentation: 'Maximum heap percentage for lookup storage' },
    ],
  },
  // connectorConfig object properties
  {
    path: '$.extractionNamespace.connectorConfig',
    isObject: true,
    completions: [
      { value: 'createTables', documentation: 'Whether to create tables if they do not exist' },
      { value: 'connectURI', documentation: 'JDBC connection URI' },
      { value: 'user', documentation: 'Database username' },
      { value: 'password', documentation: 'Database password' },
    ],
  },
  // createTables values
  {
    path: '$.extractionNamespace.connectorConfig.createTables',
    completions: [
      { value: 'true', documentation: 'Create tables if they do not exist' },
      { value: 'false', documentation: 'Do not create tables' },
    ],
  },
  // pollPeriod common values
  {
    path: /^.*\.pollPeriod$/,
    completions: [
      { value: 'PT1M', documentation: '1 minute' },
      { value: 'PT10M', documentation: '10 minutes' },
      { value: 'PT30M', documentation: '30 minutes' },
      { value: 'PT1H', documentation: '1 hour (default)' },
      { value: 'PT6H', documentation: '6 hours' },
      { value: 'P1D', documentation: '1 day' },
    ],
  },
  // firstCacheTimeout values
  {
    path: '$.firstCacheTimeout',
    completions: [
      { value: '0', documentation: 'Do not wait for first cache (default)' },
      { value: '120000', documentation: '2 minutes' },
      { value: '300000', documentation: '5 minutes' },
      { value: '600000', documentation: '10 minutes' },
    ],
  },
  // injective values
  {
    path: '$.injective',
    completions: [
      { value: 'true', documentation: 'Keys and values are unique (enables optimizations)' },
      { value: 'false', documentation: 'Keys and values may not be unique (default)' },
    ],
  },
  // kafka type properties
  {
    path: '$',
    isObject: true,
    condition: (obj) => obj.type === 'kafka',
    completions: [
      { value: 'kafkaTopic', documentation: 'Kafka topic to read lookup data from' },
      { value: 'kafkaProperties', documentation: 'Kafka consumer properties' },
      { value: 'connectTimeout', documentation: 'Connection timeout in milliseconds' },
      { value: 'isOneToOne', documentation: 'Whether the lookup is one-to-one (keys and values are unique)' },
    ],
  },
  // kafkaProperties object properties
  {
    path: '$.kafkaProperties',
    isObject: true,
    completions: [
      { value: 'bootstrap.servers', documentation: 'Kafka bootstrap servers (required)' },
      { value: 'group.id', documentation: 'Kafka consumer group ID' },
      { value: 'auto.offset.reset', documentation: 'What to do when there is no initial offset' },
      { value: 'enable.auto.commit', documentation: 'Whether to auto-commit offsets' },
    ],
  },
  // auto.offset.reset values
  {
    path: '$.kafkaProperties["auto.offset.reset"]',
    completions: [
      { value: 'earliest', documentation: 'Start from the earliest available message' },
      { value: 'latest', documentation: 'Start from the latest available message' },
      { value: 'none', documentation: 'Throw exception if no previous offset found' },
    ],
  },
  // enable.auto.commit values
  {
    path: '$.kafkaProperties["enable.auto.commit"]',
    completions: [
      { value: 'true', documentation: 'Auto-commit offsets (default)' },
      { value: 'false', documentation: 'Do not auto-commit offsets' },
    ],
  },
  // connectTimeout values
  {
    path: '$.connectTimeout',
    completions: [
      { value: '0', documentation: 'No timeout (default)' },
      { value: '5000', documentation: '5 seconds' },
      { value: '30000', documentation: '30 seconds' },
      { value: '60000', documentation: '1 minute' },
    ],
  },
  // isOneToOne values
  {
    path: '$.isOneToOne',
    completions: [
      { value: 'true', documentation: 'Keys and values are unique (enables optimizations)' },
      { value: 'false', documentation: 'Keys and values may not be unique (default)' },
    ],
  },
];