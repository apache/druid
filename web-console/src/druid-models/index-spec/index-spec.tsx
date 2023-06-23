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

import { Code } from '@blueprintjs/core';
import React from 'react';

import type { Field } from '../../components';
import { deepGet } from '../../utils';

export interface IndexSpec {
  bitmap?: Bitmap;
  dimensionCompression?: string;
  stringDictionaryEncoding?: { type: 'utf8' | 'frontCoded'; bucketSize: number };
  metricCompression?: string;
  longEncoding?: string;
  jsonCompression?: string;
}

export interface Bitmap {
  type: string;
}

export function summarizeIndexSpec(indexSpec: IndexSpec | undefined): string {
  if (!indexSpec) return '';

  const { stringDictionaryEncoding, bitmap, longEncoding } = indexSpec;

  const ret: string[] = [];
  if (stringDictionaryEncoding) {
    switch (stringDictionaryEncoding.type) {
      case 'frontCoded':
        ret.push(`frontCoded(${stringDictionaryEncoding.bucketSize || 4})`);
        break;

      default:
        ret.push(stringDictionaryEncoding.type);
        break;
    }
  }

  if (bitmap) {
    ret.push(bitmap.type);
  }

  if (longEncoding) {
    ret.push(longEncoding);
  }

  return ret.join('; ');
}

export const INDEX_SPEC_FIELDS: Field<IndexSpec>[] = [
  {
    name: 'stringDictionaryEncoding.type',
    label: 'String dictionary encoding',
    type: 'string',
    defaultValue: 'utf8',
    suggestions: ['utf8', 'frontCoded'],
    info: (
      <>
        Encoding format for STRING value dictionaries used by STRING and COMPLEX&lt;json&gt;
        columns.
      </>
    ),
  },
  {
    name: 'stringDictionaryEncoding.bucketSize',
    label: 'String dictionary encoding bucket size',
    type: 'number',
    defaultValue: 4,
    min: 1,
    max: 128,
    defined: spec => deepGet(spec, 'stringDictionaryEncoding.type') === 'frontCoded',
    info: (
      <>
        The number of values to place in a bucket to perform delta encoding. Must be a power of 2,
        maximum is 128.
      </>
    ),
  },

  {
    name: 'bitmap.type',
    label: 'Bitmap type',
    type: 'string',
    defaultValue: 'roaring',
    suggestions: ['roaring', 'concise'],
    info: <>Compression format for bitmap indexes.</>,
  },

  {
    name: 'dimensionCompression',
    type: 'string',
    defaultValue: 'lz4',
    suggestions: ['lz4', 'lzf', 'zstd', 'uncompressed'],
    info: <>Compression format for dimension columns.</>,
  },

  {
    name: 'longEncoding',
    type: 'string',
    defaultValue: 'longs',
    suggestions: ['longs', 'auto'],
    info: (
      <>
        Encoding format for long-typed columns. Applies regardless of whether they are dimensions or
        metrics. <Code>auto</Code> encodes the values using offset or lookup table depending on
        column cardinality, and store them with variable size. <Code>longs</Code> stores the value
        as-is with 8 bytes each.
      </>
    ),
  },
  {
    name: 'metricCompression',
    type: 'string',
    defaultValue: 'lz4',
    suggestions: ['lz4', 'lzf', 'zstd', 'uncompressed'],
    info: <>Compression format for primitive type metric columns.</>,
  },

  {
    name: 'jsonCompression',
    label: 'JSON compression',
    type: 'string',
    defaultValue: 'lz4',
    suggestions: ['lz4', 'lzf', 'zstd', 'uncompressed'],
    info: <>Compression format to use for nested column raw data. </>,
  },
];
