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

import { AutoForm, ExternalLink, Field } from '../../components';
import { getLink } from '../../links';
import { oneOf, typeIs } from '../../utils';
import { FlattenSpec } from '../flatten-spec/flatten-spec';

export interface InputFormat {
  readonly type: string;
  readonly findColumnsFromHeader?: boolean;
  readonly skipHeaderRows?: number;
  readonly columns?: string[];
  readonly delimiter?: string;
  readonly listDelimiter?: string | null;
  readonly pattern?: string;
  readonly function?: string;
  readonly flattenSpec?: FlattenSpec | null;
  readonly featureSpec?: Record<string, boolean>;
  readonly keepNullColumns?: boolean;
}

export const INPUT_FORMAT_FIELDS: Field<InputFormat>[] = [
  {
    name: 'type',
    label: 'Input format',
    type: 'string',
    suggestions: ['json', 'csv', 'tsv', 'parquet', 'orc', 'avro_ocf', 'avro_stream', 'regex'],
    required: true,
    info: (
      <>
        <p>The parser used to parse the data.</p>
        <p>
          For more information see{' '}
          <ExternalLink href={`${getLink('DOCS')}/ingestion/data-formats.html`}>
            the documentation
          </ExternalLink>
          .
        </p>
      </>
    ),
  },
  {
    name: 'featureSpec',
    label: 'JSON parser features',
    type: 'json',
    defined: typeIs('json'),
    info: (
      <>
        <p>
          <ExternalLink href="https://github.com/FasterXML/jackson-core/wiki/JsonParser-Features">
            JSON parser features
          </ExternalLink>{' '}
          supported by Jackson library. Those features will be applied when parsing the input JSON
          data.
        </p>
        <p>
          Example:{' '}
          <Code>{`{ "ALLOW_SINGLE_QUOTES": true, "ALLOW_UNQUOTED_FIELD_NAMES": true }`}</Code>
        </p>
      </>
    ),
  },
  {
    name: 'delimiter',
    type: 'string',
    defaultValue: '\t',
    suggestions: ['\t', ';', '|', '#'],
    defined: typeIs('tsv'),
    info: <>A custom delimiter for data values.</>,
  },
  {
    name: 'pattern',
    type: 'string',
    defined: typeIs('regex'),
    required: true,
  },
  {
    name: 'function',
    type: 'string',
    defined: typeIs('javascript'),
    required: true,
  },
  {
    name: 'skipHeaderRows',
    type: 'number',
    defaultValue: 0,
    defined: typeIs('csv', 'tsv'),
    min: 0,
    info: (
      <>
        If this is set, skip the first <Code>skipHeaderRows</Code> rows from each file.
      </>
    ),
  },
  {
    name: 'findColumnsFromHeader',
    type: 'boolean',
    defined: typeIs('csv', 'tsv'),
    required: true,
    info: (
      <>
        If this is set, find the column names from the header row. Note that
        <Code>skipHeaderRows</Code> will be applied before finding column names from the header. For
        example, if you set <Code>skipHeaderRows</Code> to 2 and <Code>findColumnsFromHeader</Code>{' '}
        to true, the task will skip the first two lines and then extract column information from the
        third line.
      </>
    ),
  },
  {
    name: 'columns',
    type: 'string-array',
    required: true,
    defined: p =>
      (oneOf(p.type, 'csv', 'tsv') && p.findColumnsFromHeader === false) || p.type === 'regex',
    info: (
      <>
        Specifies the columns of the data. The columns should be in the same order with the columns
        of your data.
      </>
    ),
  },
  {
    name: 'listDelimiter',
    type: 'string',
    defaultValue: '\x01',
    suggestions: ['\x01', '\x00'],
    defined: typeIs('csv', 'tsv', 'regex'),
    info: <>A custom delimiter for multi-value dimensions.</>,
  },
  {
    name: 'binaryAsString',
    type: 'boolean',
    defaultValue: false,
    defined: typeIs('parquet', 'orc', 'avro_ocf', 'avro_stream'),
    info: (
      <>
        Specifies if the binary column which is not logically marked as a string should be treated
        as a UTF-8 encoded string.
      </>
    ),
  },
];

export function issueWithInputFormat(inputFormat: InputFormat | undefined): string | undefined {
  return AutoForm.issueWithModel(inputFormat, INPUT_FORMAT_FIELDS);
}

export const inputFormatCanFlatten: (inputFormat: InputFormat) => boolean = typeIs(
  'json',
  'parquet',
  'orc',
  'avro_ocf',
  'avro_stream',
);
