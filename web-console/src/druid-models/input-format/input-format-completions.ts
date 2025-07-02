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
import { typeIs } from '../../utils';

// Jackson JSON parser features for featureSpec only
export const FEATURE_SPEC_COMPLETIONS: JsonCompletionRule[] = [
  {
    path: '$',
    isObject: true,
    completions: [
      { value: 'ALLOW_COMMENTS', documentation: 'Allow comments in JSON (/* */ and // style)' },
      {
        value: 'ALLOW_SINGLE_QUOTES',
        documentation: 'Allow single quotes around field names and string values',
      },
      {
        value: 'ALLOW_UNQUOTED_FIELD_NAMES',
        documentation: 'Allow unquoted field names in JSON objects',
      },
      {
        value: 'ALLOW_NUMERIC_LEADING_ZEROS',
        documentation: 'Allow leading zeros in numbers (e.g., 007)',
      },
      {
        value: 'ALLOW_NON_NUMERIC_NUMBERS',
        documentation: 'Allow NaN and Infinity as floating point numbers',
      },
      {
        value: 'ALLOW_MISSING_VALUES',
        documentation: 'Allow missing values in JSON arrays and objects',
      },
      {
        value: 'ALLOW_TRAILING_COMMA',
        documentation: 'Allow trailing commas in JSON objects and arrays',
      },
      {
        value: 'STRICT_DUPLICATE_DETECTION',
        documentation: 'Enable strict detection of duplicate field names',
      },
      {
        value: 'IGNORE_UNDEFINED',
        documentation: 'Ignore undefined values instead of throwing exception',
      },
      {
        value: 'INCLUDE_SOURCE_IN_LOCATION',
        documentation: 'Include source reference in location information',
      },
    ],
  },
];

// Avro bytes decoder completions for avroBytesDecoder field
export const AVRO_BYTES_DECODER_COMPLETIONS: JsonCompletionRule[] = [
  {
    path: '$',
    isObject: true,
    completions: [{ value: 'type', documentation: 'Type of Avro bytes decoder' }],
  },
  {
    path: '$.type',
    completions: [
      { value: 'schema_repo', documentation: 'Use schema repository for Avro schema' },
      { value: 'inline', documentation: 'Inline Avro schema definition' },
      { value: 'schema_registry', documentation: 'Use schema registry for Avro schema' },
    ],
  },
  {
    path: '$',
    isObject: true,
    condition: typeIs('schema_repo'),
    completions: [
      { value: 'subjectAndIdStr', documentation: 'Subject and ID string for schema repository' },
      { value: 'schemaRepository', documentation: 'Schema repository configuration' },
    ],
  },
  {
    path: '$',
    isObject: true,
    condition: typeIs('inline'),
    completions: [{ value: 'schema', documentation: 'Inline Avro schema as JSON object' }],
  },
  {
    path: '$',
    isObject: true,
    condition: typeIs('schema_registry'),
    completions: [
      { value: 'url', documentation: 'Schema registry URL' },
      { value: 'capacity', documentation: 'Schema registry cache capacity' },
      { value: 'urls', documentation: 'List of schema registry URLs' },
      { value: 'config', documentation: 'Additional schema registry configuration' },
      { value: 'headers', documentation: 'HTTP headers for schema registry requests' },
    ],
  },
];

// Protobuf bytes decoder completions for protoBytesDecoder field
export const PROTO_BYTES_DECODER_COMPLETIONS: JsonCompletionRule[] = [
  {
    path: '$',
    isObject: true,
    completions: [{ value: 'type', documentation: 'Type of Protobuf bytes decoder' }],
  },
  {
    path: '$.type',
    completions: [
      { value: 'file', documentation: 'Use file-based Protobuf descriptor' },
      { value: 'inline', documentation: 'Inline Protobuf descriptor' },
    ],
  },
  {
    path: '$',
    isObject: true,
    condition: typeIs('file'),
    completions: [
      { value: 'descriptor', documentation: 'Path to Protobuf descriptor file' },
      { value: 'protoMessageType', documentation: 'Protobuf message type name' },
    ],
  },
  {
    path: '$',
    isObject: true,
    condition: typeIs('inline'),
    completions: [
      { value: 'descriptorString', documentation: 'Base64 encoded Protobuf descriptor' },
      { value: 'protoMessageType', documentation: 'Protobuf message type name' },
    ],
  },
];
