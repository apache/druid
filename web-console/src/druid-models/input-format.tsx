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

import { AutoForm, ExternalLink, Field } from '../components';
import { getLink } from '../links';
import { oneOf } from '../utils';

import { FlattenSpec } from './flatten-spec';

export interface InputFormat {
  type: string;
  findColumnsFromHeader?: boolean;
  skipHeaderRows?: number;
  columns?: string[];
  listDelimiter?: string;
  pattern?: string;
  function?: string;
  flattenSpec?: FlattenSpec;
  keepNullColumns?: boolean;
}

export const INPUT_FORMAT_FIELDS: Field<InputFormat>[] = [
  {
    name: 'type',
    label: 'Input format',
    type: 'string',
    suggestions: ['json', 'csv', 'tsv', 'regex', 'parquet', 'orc', 'avro_ocf', 'avro_stream'],
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
    name: 'pattern',
    type: 'string',
    required: true,
    defined: (p: InputFormat) => p.type === 'regex',
  },
  {
    name: 'function',
    type: 'string',
    required: true,
    defined: (p: InputFormat) => p.type === 'javascript',
  },
  {
    name: 'skipHeaderRows',
    type: 'number',
    defaultValue: 0,
    defined: (p: InputFormat) => oneOf(p.type, 'csv', 'tsv'),
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
    required: true,
    defined: (p: InputFormat) => oneOf(p.type, 'csv', 'tsv'),
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
    defined: (p: InputFormat) =>
      (oneOf(p.type, 'csv', 'tsv') && p.findColumnsFromHeader === false) || p.type === 'regex',
    info: (
      <>
        Specifies the columns of the data. The columns should be in the same order with the columns
        of your data.
      </>
    ),
  },
  {
    name: 'delimiter',
    type: 'string',
    defaultValue: '\t',
    defined: (p: InputFormat) => p.type === 'tsv',
    info: <>A custom delimiter for data values.</>,
  },
  {
    name: 'listDelimiter',
    type: 'string',
    defined: (p: InputFormat) => oneOf(p.type, 'csv', 'tsv', 'regex'),
    placeholder: '(optional, default = ctrl+A)',
    info: <>A custom delimiter for multi-value dimensions.</>,
  },
  {
    name: 'binaryAsString',
    type: 'boolean',
    defaultValue: false,
    defined: (p: InputFormat) => oneOf(p.type, 'parquet', 'orc', 'avro_ocf', 'avro_stream'),
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

export function inputFormatCanFlatten(inputFormat: InputFormat): boolean {
  return oneOf(inputFormat.type, 'json', 'parquet', 'orc', 'avro_ocf', 'avro_stream');
}
