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

export const INDEX_SPEC_COMPLETIONS: JsonCompletionRule[] = [
  // Root level properties
  {
    path: '$',
    isObject: true,
    completions: [
      { value: 'bitmap', documentation: 'Compression format for bitmap indexes' },
      {
        value: 'dimensionCompression',
        documentation: 'Compression format for dimension columns (default: lz4)',
      },
      {
        value: 'stringDictionaryEncoding',
        documentation: 'Encoding format for string value dictionaries',
      },
      {
        value: 'metricCompression',
        documentation: 'Compression format for primitive type metric columns (default: lz4)',
      },
      {
        value: 'longEncoding',
        documentation: 'Encoding format for long-typed columns (default: longs)',
      },
      {
        value: 'jsonCompression',
        documentation: 'Compression format for nested column raw data (default: lz4)',
      },
    ],
  },
  // bitmap object properties
  {
    path: '$.bitmap',
    isObject: true,
    completions: [{ value: 'type', documentation: 'Bitmap compression type' }],
  },
  // bitmap.type values
  {
    path: '$.bitmap.type',
    completions: [
      { value: 'roaring', documentation: 'Roaring bitmap compression (default)' },
      { value: 'concise', documentation: 'Concise bitmap compression' },
    ],
  },
  // dimensionCompression values
  {
    path: '$.dimensionCompression',
    completions: [
      { value: 'lz4', documentation: 'LZ4 compression (default)' },
      { value: 'lzf', documentation: 'LZF compression' },
      { value: 'zstd', documentation: 'Zstandard compression' },
      { value: 'uncompressed', documentation: 'No compression' },
    ],
  },
  // metricCompression values
  {
    path: '$.metricCompression',
    completions: [
      { value: 'lz4', documentation: 'LZ4 compression (default)' },
      { value: 'lzf', documentation: 'LZF compression' },
      { value: 'zstd', documentation: 'Zstandard compression' },
      { value: 'uncompressed', documentation: 'No compression' },
      {
        value: 'none',
        documentation: 'More efficient than uncompressed (requires newer Druid versions)',
      },
    ],
  },
  // longEncoding values
  {
    path: '$.longEncoding',
    completions: [
      { value: 'longs', documentation: 'Store as-is with 8 bytes each (default)' },
      { value: 'auto', documentation: 'Use offset or lookup table based on cardinality' },
    ],
  },
  // jsonCompression values
  {
    path: '$.jsonCompression',
    completions: [
      { value: 'lz4', documentation: 'LZ4 compression (default)' },
      { value: 'lzf', documentation: 'LZF compression' },
      { value: 'zstd', documentation: 'Zstandard compression' },
      { value: 'uncompressed', documentation: 'No compression' },
    ],
  },
  // stringDictionaryEncoding object properties
  {
    path: '$.stringDictionaryEncoding',
    isObject: true,
    completions: [{ value: 'type', documentation: 'String dictionary encoding type' }],
  },
  // stringDictionaryEncoding.type values
  {
    path: '$.stringDictionaryEncoding.type',
    completions: [
      { value: 'utf8', documentation: 'UTF-8 encoding (default)' },
      {
        value: 'frontCoded',
        documentation: 'Front coding for optimized string compression (experimental)',
      },
    ],
  },
  // bucketSize for frontCoded encoding
  {
    path: '$.stringDictionaryEncoding',
    isObject: true,
    condition: obj => obj.type === 'frontCoded',
    completions: [
      {
        value: 'bucketSize',
        documentation: 'Number of values per bucket for delta encoding (power of 2, max 128)',
      },
      { value: 'formatVersion', documentation: 'Front coding format version (0 or 1)' },
    ],
  },
  // bucketSize values (powers of 2)
  {
    path: '$.stringDictionaryEncoding.bucketSize',
    completions: [
      { value: '1', documentation: '1 value per bucket' },
      { value: '2', documentation: '2 values per bucket' },
      { value: '4', documentation: '4 values per bucket (default)' },
      { value: '8', documentation: '8 values per bucket' },
      { value: '16', documentation: '16 values per bucket' },
      { value: '32', documentation: '32 values per bucket' },
      { value: '64', documentation: '64 values per bucket' },
      { value: '128', documentation: '128 values per bucket (maximum)' },
    ],
  },
  // formatVersion values
  {
    path: '$.stringDictionaryEncoding.formatVersion',
    completions: [
      { value: '0', documentation: 'Format version 0 (default, compatible with Druid 25+)' },
      { value: '1', documentation: 'Format version 1 (faster and smaller, requires Druid 26+)' },
    ],
  },
];
