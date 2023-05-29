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
import { AutoForm, ExternalLink } from '../../components';
import { getLink } from '../../links';
import { compact, deepGet, deepSet, oneOf, typeIs } from '../../utils';
import type { FlattenSpec } from '../flatten-spec/flatten-spec';

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
  readonly assumeNewlineDelimited?: boolean;
  readonly useJsonNodeReader?: boolean;

  // type: kafka
  readonly timestampColumnName?: string;
  readonly headerFormat?: { type: 'string'; encoding?: string };
  readonly headerColumnPrefix?: string;
  readonly keyFormat?: InputFormat;
  readonly keyColumnName?: string;
  readonly valueFormat?: InputFormat;
}

function generateInputFormatFields(streaming: boolean) {
  return compact([
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
    streaming
      ? {
          name: 'assumeNewlineDelimited',
          type: 'boolean',
          defined: typeIs('json'),
          disabled: inputFormat => Boolean(inputFormat.useJsonNodeReader),
          defaultValue: false,
          info: (
            <>
              <p>
                In streaming ingestion, multi-line JSON events can be ingested (i.e. where a single
                JSON event spans multiple lines). However, if a parsing exception occurs, all JSON
                events that are present in the same streaming record will be discarded.
              </p>
              <p>
                <Code>assumeNewlineDelimited</Code> and <Code>useJsonNodeReader</Code> (at most one
                can be <Code>true</Code>) affect only how parsing exceptions are handled.
              </p>
              <p>
                If the input is known to be newline delimited JSON (each individual JSON event is
                contained in a single line, separated by newlines), setting this option to true
                allows for more flexible parsing exception handling. Only the lines with invalid
                JSON syntax will be discarded, while lines containing valid JSON events will still
                be ingested.
              </p>
            </>
          ),
        }
      : undefined,
    streaming
      ? {
          name: 'useJsonNodeReader',
          label: 'Use JSON node reader',
          type: 'boolean',
          defined: typeIs('json'),
          disabled: inputFormat => Boolean(inputFormat.assumeNewlineDelimited),
          defaultValue: false,
          info: (
            <>
              {' '}
              <p>
                In streaming ingestion, multi-line JSON events can be ingested (i.e. where a single
                JSON event spans multiple lines). However, if a parsing exception occurs, all JSON
                events that are present in the same streaming record will be discarded.
              </p>
              <p>
                <Code>assumeNewlineDelimited</Code> and <Code>useJsonNodeReader</Code> (at most one
                can be <Code>true</Code>) affect only how parsing exceptions are handled.
              </p>
              <p>
                When ingesting multi-line JSON events, enabling this option will enable the use of a
                JSON parser which will retain any valid JSON events encountered within a streaming
                record prior to when a parsing exception occurred.
              </p>
            </>
          ),
        }
      : undefined,
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
          <Code>skipHeaderRows</Code> will be applied before finding column names from the header.
          For example, if you set <Code>skipHeaderRows</Code> to 2 and{' '}
          <Code>findColumnsFromHeader</Code> to true, the task will skip the first two lines and
          then extract column information from the third line.
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
          Specifies the columns of the data. The columns should be in the same order with the
          columns of your data.
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
  ] as (Field<InputFormat> | undefined)[]);
}

export const BATCH_INPUT_FORMAT_FIELDS: Field<InputFormat>[] = generateInputFormatFields(false);
export const STREAMING_INPUT_FORMAT_FIELDS: Field<InputFormat>[] = generateInputFormatFields(true);
export const KAFKA_METADATA_INPUT_FORMAT_FIELDS: Field<InputFormat>[] = [
  {
    name: 'timestampColumnName',
    label: 'Kafka timestamp column name',
    type: 'string',
    defaultValue: 'kafka.timestamp',
    defined: typeIs('kafka'),
    info: `Name of the column for the kafka record's timestamp.`,
  },

  // -----------------------------------------------------
  // keyFormat fields

  {
    name: 'keyFormat.type',
    label: 'Kafka key input format',
    type: 'string',
    suggestions: [
      undefined,
      'json',
      'csv',
      'tsv',
      'parquet',
      'orc',
      'avro_ocf',
      'avro_stream',
      'regex',
    ],
    placeholder: `(don't parse Kafka key)`,
    defined: typeIs('kafka'),
    info: (
      <>
        <p>The parser used to parse the key of the Kafka message.</p>
        <p>
          For more information see{' '}
          <ExternalLink href={`${getLink('DOCS')}/ingestion/data-formats.html`}>
            the documentation
          </ExternalLink>
          .
        </p>
      </>
    ),
    adjustment: inputFormat => {
      const keyFormatType = deepGet(inputFormat, 'keyFormat.type');
      // If the user selects one of these formats then populate the columns (that are in any case meaningless in this context)
      // with an initial value.
      switch (keyFormatType) {
        case 'regex':
          inputFormat = deepSet(inputFormat, 'keyFormat.pattern', '([\\s\\S]*)');
          inputFormat = deepSet(inputFormat, 'keyFormat.columns', ['x']);
          break;

        case 'csv':
        case 'tsv':
          inputFormat = deepSet(inputFormat, 'keyFormat.findColumnsFromHeader', false);
          inputFormat = deepSet(inputFormat, 'keyFormat.columns', ['x']);
          break;
      }
      return inputFormat;
    },
  },
  {
    name: 'keyFormat.featureSpec',
    label: 'Kafka key JSON parser features',
    type: 'json',
    defined: inputFormat => deepGet(inputFormat, 'keyFormat.type') === 'json',
    hideInMore: true,
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
    name: 'keyFormat.assumeNewlineDelimited',
    label: 'Kafka key assume newline delimited',
    type: 'boolean',
    defined: inputFormat => deepGet(inputFormat, 'keyFormat.type') === 'json',
    disabled: inputFormat => Boolean(inputFormat.useJsonNodeReader),
    defaultValue: false,
    hideInMore: true,
    info: (
      <>
        <p>
          In streaming ingestion, multi-line JSON events can be ingested (i.e. where a single JSON
          event spans multiple lines). However, if a parsing exception occurs, all JSON events that
          are present in the same streaming record will be discarded.
        </p>
        <p>
          <Code>assumeNewlineDelimited</Code> and <Code>useJsonNodeReader</Code> (at most one can be{' '}
          <Code>true</Code>) affect only how parsing exceptions are handled.
        </p>
        <p>
          If the input is known to be newline delimited JSON (each individual JSON event is
          contained in a single line, separated by newlines), setting this option to true allows for
          more flexible parsing exception handling. Only the lines with invalid JSON syntax will be
          discarded, while lines containing valid JSON events will still be ingested.
        </p>
      </>
    ),
  },
  {
    name: 'keyFormat.useJsonNodeReader',
    label: 'Kafka key use JSON node reader',
    type: 'boolean',
    defined: inputFormat => deepGet(inputFormat, 'keyFormat.type') === 'json',
    disabled: inputFormat => Boolean(inputFormat.assumeNewlineDelimited),
    defaultValue: false,
    hideInMore: true,
    info: (
      <>
        {' '}
        <p>
          In streaming ingestion, multi-line JSON events can be ingested (i.e. where a single JSON
          event spans multiple lines). However, if a parsing exception occurs, all JSON events that
          are present in the same streaming record will be discarded.
        </p>
        <p>
          <Code>assumeNewlineDelimited</Code> and <Code>useJsonNodeReader</Code> (at most one can be{' '}
          <Code>true</Code>) affect only how parsing exceptions are handled.
        </p>
        <p>
          When ingesting multi-line JSON events, enabling this option will enable the use of a JSON
          parser which will retain any valid JSON events encountered within a streaming record prior
          to when a parsing exception occurred.
        </p>
      </>
    ),
  },
  {
    name: 'keyFormat.delimiter',
    label: 'Kafka key delimiter',
    type: 'string',
    defaultValue: '\t',
    suggestions: ['\t', ';', '|', '#'],
    defined: inputFormat => deepGet(inputFormat, 'keyFormat.type') === 'tsv',
    info: <>A custom delimiter for data values.</>,
  },
  {
    name: 'keyFormat.pattern',
    label: 'Kafka key pattern',
    type: 'string',
    defined: inputFormat => deepGet(inputFormat, 'keyFormat.type') === 'regex',
    required: true,
  },
  {
    name: 'keyFormat.skipHeaderRows',
    label: 'Kafka key skip header rows',
    type: 'number',
    defaultValue: 0,
    defined: inputFormat => oneOf(deepGet(inputFormat, 'keyFormat.type'), 'csv', 'tsv'),
    min: 0,
    info: (
      <>
        If this is set, skip the first <Code>skipHeaderRows</Code> rows from each file.
      </>
    ),
  },
  {
    name: 'keyFormat.findColumnsFromHeader',
    label: 'Kafka key find columns from header',
    type: 'boolean',
    defined: inputFormat => oneOf(deepGet(inputFormat, 'keyFormat.type'), 'csv', 'tsv'),
    required: true,
    hideInMore: true,
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
    name: 'keyFormat.columns',
    label: 'Kafka key columns',
    type: 'string-array',
    required: true,
    defined: inputFormat =>
      (oneOf(deepGet(inputFormat, 'keyFormat.type'), 'csv', 'tsv') &&
        deepGet(inputFormat, 'keyFormat.findColumnsFromHeader') === false) ||
      deepGet(inputFormat, 'keyFormat.type') === 'regex',
    hideInMore: true,
    info: (
      <>
        Only the value of the first column will be read, the name of the column will be ignored so
        enter anything here.
      </>
    ),
  },
  {
    name: 'keyFormat.listDelimiter',
    label: 'Kafka key list delimiter',
    type: 'string',
    defaultValue: '\x01',
    suggestions: ['\x01', '\x00'],
    defined: inputFormat => oneOf(deepGet(inputFormat, 'keyFormat.type'), 'csv', 'tsv', 'regex'),
    info: <>A custom delimiter for multi-value dimensions.</>,
  },
  {
    name: 'keyFormat.binaryAsString',
    label: 'Kafka key list binary as string',
    type: 'boolean',
    defaultValue: false,
    defined: inputFormat =>
      oneOf(deepGet(inputFormat, 'valueFormat.type'), 'parquet', 'orc', 'avro_ocf', 'avro_stream'),
    info: (
      <>
        Specifies if the binary column which is not logically marked as a string should be treated
        as a UTF-8 encoded string.
      </>
    ),
  },

  // keyColumnName
  {
    name: 'keyColumnName',
    label: 'Kafka key column name',
    type: 'string',
    defaultValue: 'kafka.key',
    defined: inputFormat => Boolean(deepGet(inputFormat, 'keyFormat.type')),
    info: `Custom prefix for all the header columns.`,
  },

  // -----------------------------------------------------

  {
    name: 'headerFormat.type',
    label: 'Kafka header format type',
    type: 'string',
    defined: typeIs('kafka'),
    placeholder: `(don't parse Kafka herders)`,
    suggestions: [undefined, 'string'],
  },
  {
    name: 'headerFormat.encoding',
    label: 'Kafka header format encoding',
    type: 'string',
    defaultValue: 'UTF-8',
    defined: inputFormat => deepGet(inputFormat, 'headerFormat.type') === 'string',
    suggestions: ['UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16', 'US-ASCII', 'ISO-8859-1'],
  },
  {
    name: 'headerColumnPrefix',
    label: 'Kafka header column prefix',
    type: 'string',
    defaultValue: 'kafka.header.',
    defined: inputFormat => deepGet(inputFormat, 'headerFormat.type') === 'string',
    info: `Custom prefix for all the header columns.`,
  },
];

export function issueWithInputFormat(inputFormat: InputFormat | undefined): string | undefined {
  return AutoForm.issueWithModel(inputFormat, BATCH_INPUT_FORMAT_FIELDS);
}

export const inputFormatCanProduceNestedData: (inputFormat: InputFormat) => boolean = typeIs(
  'json',
  'parquet',
  'orc',
  'avro_ocf',
  'avro_stream',
);
