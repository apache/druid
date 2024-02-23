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
import { AutoForm } from '../../components';
import { deepGet, deepSet, oneOf, oneOfKnown, pluralIfNeeded, typeIsKnown } from '../../utils';

export interface ExtractionNamespaceSpec {
  readonly type: string;
  readonly uri?: string;
  readonly uriPrefix?: string;
  readonly fileRegex?: string;
  readonly namespaceParseSpec?: NamespaceParseSpec;
  readonly connectorConfig?: {
    readonly createTables: boolean;
    readonly connectURI: string;
    readonly user: string;
    readonly password: string;
  };
  readonly table?: string;
  readonly keyColumn?: string;
  readonly valueColumn?: string;
  readonly filter?: any;
  readonly tsColumn?: string;
  readonly pollPeriod?: number | string;
  readonly jitterSeconds?: number;
  readonly loadTimeoutSeconds?: number;
  readonly maxHeapPercentage?: number;
}

export interface NamespaceParseSpec {
  readonly format: string;
  readonly columns?: string[];
  readonly keyColumn?: string;
  readonly valueColumn?: string;
  readonly hasHeaderRow?: boolean;
  readonly skipHeaderRows?: number;
  readonly keyFieldName?: string;
  readonly valueFieldName?: string;
  readonly delimiter?: string;
  readonly listDelimiter?: string;
}

export interface LookupSpec {
  readonly type: string;

  // type: map
  readonly map?: Record<string, string | number>;

  // type: cachedNamespace
  readonly extractionNamespace?: ExtractionNamespaceSpec;
  readonly firstCacheTimeout?: number;
  readonly injective?: boolean;

  // type: kafka
  readonly kafkaTopic?: string;
  readonly kafkaProperties?: Record<string, any>;
  readonly connectTimeout?: number;
  readonly isOneToOne?: boolean;
}

function issueWithUri(uri: string): string | undefined {
  if (!uri) return;
  const m = /^(\w+):/.exec(uri);
  if (!m) return `URI is invalid, must start with 'file:', 'hdfs:', 's3:', or 'gs:`;
  if (!oneOf(m[1], 'file', 'hdfs', 's3', 'gs')) {
    return `Unsupported location '${m[1]}:'. Only 'file:', 'hdfs:', 's3:', and 'gs:' locations are supported`;
  }
  return;
}

function issueWithConnectUri(uri: string): string | undefined {
  if (!uri) return;
  if (!uri.startsWith('jdbc:')) return `connectURI is invalid, must start with 'jdbc:'`;
  return;
}

const KNOWN_TYPES = ['map', 'cachedNamespace', 'kafka'];
const KNOWN_EXTRACTION_NAMESPACE_TYPES = ['uri', 'jdbc'];
const KNOWN_NAMESPACE_PARSE_SPEC_FORMATS = ['csv', 'tsv', 'simpleJson', 'customJson'];
export const LOOKUP_FIELDS: Field<LookupSpec>[] = [
  {
    name: 'type',
    type: 'string',
    suggestions: KNOWN_TYPES,
    required: true,
    adjustment: l => {
      if (l.type === 'map' && !l.map) {
        return deepSet(l, 'map', {});
      }
      if (l.type === 'cachedNamespace' && !deepGet(l, 'extractionNamespace.type')) {
        return deepSet(l, 'extractionNamespace', { type: 'uri', pollPeriod: 'PT1H' });
      }
      if (l.type === 'kafka' && !deepGet(l, 'kafkaProperties')) {
        return deepSet(l, 'kafkaProperties', { 'bootstrap.servers': '' });
      }
      return l;
    },
  },

  // map lookups are simple
  {
    name: 'map',
    type: 'json',
    height: '60vh',
    defined: typeIsKnown(KNOWN_TYPES, 'map'),
    required: true,
    issueWithValue: value => {
      if (!value) return 'map must be defined';
      if (typeof value !== 'object') return `map must be an object`;
      for (const k in value) {
        const typeValue = typeof value[k];
        if (typeValue !== 'string' && typeValue !== 'number') {
          return `map key '${k}' is of the wrong type '${typeValue}'`;
        }
      }
      return;
    },
  },

  // cachedNamespace lookups have more options
  {
    name: 'extractionNamespace.type',
    label: 'Extraction type',
    type: 'string',
    placeholder: 'uri',
    suggestions: KNOWN_EXTRACTION_NAMESPACE_TYPES,
    defined: typeIsKnown(KNOWN_TYPES, 'cachedNamespace'),
    required: true,
  },

  {
    name: 'extractionNamespace.uriPrefix',
    label: 'URI prefix',
    type: 'string',
    placeholder: 's3://bucket/some/key/prefix/',
    defined: l =>
      oneOfKnown(deepGet(l, 'extractionNamespace.type'), KNOWN_EXTRACTION_NAMESPACE_TYPES, 'uri') &&
      !deepGet(l, 'extractionNamespace.uri'),
    required: l =>
      !deepGet(l, 'extractionNamespace.uriPrefix') && !deepGet(l, 'extractionNamespace.uri'),
    issueWithValue: issueWithUri,
    info: (
      <p>
        A URI which specifies a directory (or other searchable resource) in which to search for
        files specified as a <Code>file</Code>, <Code>hdfs</Code>, <Code>s3</Code>, or{' '}
        <Code>gs</Code> path prefix.
      </p>
    ),
  },
  {
    name: 'extractionNamespace.uri',
    type: 'string',
    label: 'URI (deprecated)',
    placeholder: 's3://bucket/some/key/prefix/lookups-01.gz',
    defined: l =>
      oneOfKnown(deepGet(l, 'extractionNamespace.type'), KNOWN_EXTRACTION_NAMESPACE_TYPES, 'uri') &&
      !deepGet(l, 'extractionNamespace.uriPrefix'),
    required: l =>
      !deepGet(l, 'extractionNamespace.uriPrefix') && !deepGet(l, 'extractionNamespace.uri'),
    issueWithValue: issueWithUri,
    info: (
      <>
        <p>
          URI for the file of interest, specified as a <Code>file</Code>, <Code>hdfs</Code>,{' '}
          <Code>s3</Code>, or <Code>gs</Code> path
        </p>
        <p>The URI prefix option is strictly better than URI and should be used instead</p>
      </>
    ),
  },
  {
    name: 'extractionNamespace.fileRegex',
    label: 'File regex',
    type: 'string',
    defaultValue: '.*',
    defined: l =>
      oneOfKnown(deepGet(l, 'extractionNamespace.type'), KNOWN_EXTRACTION_NAMESPACE_TYPES, 'uri') &&
      Boolean(deepGet(l, 'extractionNamespace.uriPrefix')),
    info: 'Optional regex for matching the file name under uriPrefix.',
  },

  // namespaceParseSpec
  {
    name: 'extractionNamespace.namespaceParseSpec.format',
    label: 'Parse format',
    type: 'string',
    suggestions: KNOWN_NAMESPACE_PARSE_SPEC_FORMATS,
    defined: l =>
      oneOfKnown(deepGet(l, 'extractionNamespace.type'), KNOWN_EXTRACTION_NAMESPACE_TYPES, 'uri'),
    required: true,
    info: (
      <>
        <p>The format of the data in the lookup files.</p>
        <p>
          The <Code>simpleJson</Code> lookupParseSpec does not take any parameters. It is simply a
          line delimited JSON file where the field is the key, and the field&apos;s value is the
          value.
        </p>
      </>
    ),
  },

  // TSV only
  {
    name: 'extractionNamespace.namespaceParseSpec.delimiter',
    type: 'string',
    defaultValue: '\t',
    suggestions: ['\t', ';', '|', '#'],
    defined: l =>
      oneOfKnown(
        deepGet(l, 'extractionNamespace.namespaceParseSpec.format'),
        KNOWN_NAMESPACE_PARSE_SPEC_FORMATS,
        'tsv',
      ),
  },

  // CSV + TSV
  {
    name: 'extractionNamespace.namespaceParseSpec.skipHeaderRows',
    type: 'number',
    defaultValue: 0,
    defined: l =>
      oneOfKnown(
        deepGet(l, 'extractionNamespace.namespaceParseSpec.format'),
        KNOWN_NAMESPACE_PARSE_SPEC_FORMATS,
        'csv',
        'tsv',
      ),
    info: `Number of header rows to be skipped.`,
  },
  {
    name: 'extractionNamespace.namespaceParseSpec.hasHeaderRow',
    type: 'boolean',
    defaultValue: false,
    defined: l =>
      oneOfKnown(
        deepGet(l, 'extractionNamespace.namespaceParseSpec.format'),
        KNOWN_NAMESPACE_PARSE_SPEC_FORMATS,
        'csv',
        'tsv',
      ),
    info: `A flag to indicate that column information can be extracted from the input files' header row`,
  },
  {
    name: 'extractionNamespace.namespaceParseSpec.columns',
    type: 'string-array',
    placeholder: 'key, value',
    defined: l =>
      oneOfKnown(
        deepGet(l, 'extractionNamespace.namespaceParseSpec.format'),
        KNOWN_NAMESPACE_PARSE_SPEC_FORMATS,
        'csv',
        'tsv',
      ),
    required: l => !deepGet(l, 'extractionNamespace.namespaceParseSpec.hasHeaderRow'),
    info: 'The list of columns in the csv file',
  },
  {
    name: 'extractionNamespace.namespaceParseSpec.keyColumn',
    type: 'string',
    placeholder: '(optional - defaults to the first column)',
    defined: l =>
      oneOfKnown(
        deepGet(l, 'extractionNamespace.namespaceParseSpec.format'),
        KNOWN_NAMESPACE_PARSE_SPEC_FORMATS,
        'csv',
        'tsv',
      ),
    info: 'The name of the column containing the key',
  },
  {
    name: 'extractionNamespace.namespaceParseSpec.valueColumn',
    type: 'string',
    placeholder: '(optional - defaults to the second column)',
    defined: l =>
      oneOfKnown(
        deepGet(l, 'extractionNamespace.namespaceParseSpec.format'),
        KNOWN_NAMESPACE_PARSE_SPEC_FORMATS,
        'csv',
        'tsv',
      ),
    info: 'The name of the column containing the value',
  },

  // Custom JSON
  {
    name: 'extractionNamespace.namespaceParseSpec.keyFieldName',
    type: 'string',
    placeholder: `key`,
    defined: l =>
      oneOfKnown(
        deepGet(l, 'extractionNamespace.namespaceParseSpec.format'),
        KNOWN_NAMESPACE_PARSE_SPEC_FORMATS,
        'customJson',
      ),
    required: true,
  },
  {
    name: 'extractionNamespace.namespaceParseSpec.valueFieldName',
    type: 'string',
    placeholder: `value`,
    defined: l =>
      oneOfKnown(
        deepGet(l, 'extractionNamespace.namespaceParseSpec.format'),
        KNOWN_NAMESPACE_PARSE_SPEC_FORMATS,
        'customJson',
      ),
    required: true,
  },

  // JDBC stuff
  {
    name: 'extractionNamespace.connectorConfig.connectURI',
    label: 'Connect URI',
    type: 'string',
    defined: l =>
      oneOfKnown(deepGet(l, 'extractionNamespace.type'), KNOWN_EXTRACTION_NAMESPACE_TYPES, 'jdbc'),
    required: true,
    issueWithValue: issueWithConnectUri,
    info: 'Defines the connectURI for connecting to the database',
  },
  {
    name: 'extractionNamespace.connectorConfig.user',
    type: 'string',
    defined: l =>
      oneOfKnown(deepGet(l, 'extractionNamespace.type'), KNOWN_EXTRACTION_NAMESPACE_TYPES, 'jdbc'),
    info: 'Defines the user to be used by the connector config',
  },
  {
    name: 'extractionNamespace.connectorConfig.password',
    type: 'string',
    defined: l =>
      oneOfKnown(deepGet(l, 'extractionNamespace.type'), KNOWN_EXTRACTION_NAMESPACE_TYPES, 'jdbc'),
    info: 'Defines the password to be used by the connector config',
  },
  {
    name: 'extractionNamespace.table',
    type: 'string',
    placeholder: 'lookup_table',
    defined: l =>
      oneOfKnown(deepGet(l, 'extractionNamespace.type'), KNOWN_EXTRACTION_NAMESPACE_TYPES, 'jdbc'),
    required: true,
    info: (
      <>
        <p>
          The table which contains the key value pairs. This will become the table value in the SQL
          query:
        </p>
        <p>
          SELECT keyColumn, valueColumn, tsColumn? FROM <strong>table</strong> WHERE filter
        </p>
      </>
    ),
  },
  {
    name: 'extractionNamespace.keyColumn',
    type: 'string',
    placeholder: 'key_column',
    defined: l =>
      oneOfKnown(deepGet(l, 'extractionNamespace.type'), KNOWN_EXTRACTION_NAMESPACE_TYPES, 'jdbc'),
    required: true,
    info: (
      <>
        <p>
          The column in the table which contains the keys. This will become the keyColumn value in
          the SQL query:
        </p>
        <p>
          SELECT <strong>keyColumn</strong>, valueColumn, tsColumn? FROM table WHERE filter
        </p>
      </>
    ),
  },
  {
    name: 'extractionNamespace.valueColumn',
    type: 'string',
    placeholder: 'value_column',
    defined: l =>
      oneOfKnown(deepGet(l, 'extractionNamespace.type'), KNOWN_EXTRACTION_NAMESPACE_TYPES, 'jdbc'),
    required: true,
    info: (
      <>
        <p>
          The column in table which contains the values. This will become the valueColumn value in
          the SQL query:
        </p>
        <p>
          SELECT keyColumn, <strong>valueColumn</strong>, tsColumn? FROM table WHERE filter
        </p>
      </>
    ),
  },
  {
    name: 'extractionNamespace.tsColumn',
    type: 'string',
    label: 'Timestamp column',
    placeholder: 'timestamp_column (optional)',
    defined: l =>
      oneOfKnown(deepGet(l, 'extractionNamespace.type'), KNOWN_EXTRACTION_NAMESPACE_TYPES, 'jdbc'),
    info: (
      <>
        <p>
          The column in table which contains when the key was updated. This will become the Value in
          the SQL query:
        </p>
        <p>
          SELECT keyColumn, valueColumn, <strong>tsColumn</strong>? FROM table WHERE filter
        </p>
      </>
    ),
  },
  {
    name: 'extractionNamespace.filter',
    type: 'string',
    placeholder: 'for_lookup = 1 (optional)',
    defined: l =>
      oneOfKnown(deepGet(l, 'extractionNamespace.type'), KNOWN_EXTRACTION_NAMESPACE_TYPES, 'jdbc'),
    info: (
      <>
        <p>
          The filter to be used when selecting lookups, this is used to create a where clause on
          lookup population. This will become the expression filter in the SQL query:
        </p>
        <p>
          SELECT keyColumn, valueColumn, tsColumn? FROM table WHERE <strong>filter</strong>
        </p>
      </>
    ),
  },

  {
    name: 'extractionNamespace.pollPeriod',
    type: 'duration',
    defined: l =>
      oneOfKnown(
        deepGet(l, 'extractionNamespace.type'),
        KNOWN_EXTRACTION_NAMESPACE_TYPES,
        'uri',
        'jdbc',
      ),
    info: `Period between polling for updates`,
    required: true,
    suggestions: ['PT1M', 'PT10M', 'PT30M', 'PT1H', 'PT6H', 'P1D'],
  },
  {
    name: 'extractionNamespace.jitterSeconds',
    type: 'number',
    defaultValue: 0,
    defined: l =>
      oneOfKnown(deepGet(l, 'extractionNamespace.type'), KNOWN_EXTRACTION_NAMESPACE_TYPES, 'jdbc'),
    info: 'How much jitter to add (in seconds) up to maximum as a delay (actual value will be used as random from 0 to jitterSeconds), used to distribute db load more evenly. Default is 0.',
    required: false,
    suggestions: [],
  },
  {
    name: 'extractionNamespace.loadTimeoutSeconds',
    type: 'number',
    defaultValue: 60,
    defined: l =>
      oneOfKnown(deepGet(l, 'extractionNamespace.type'), KNOWN_EXTRACTION_NAMESPACE_TYPES, 'jdbc'),
    info: 'How much time (in seconds) it can take to query and populate lookup values. It will be helpful in lookup updates. On lookup update, it will wait maximum of `loadTimeoutSeconds` for new lookup to come up and continue serving from old lookup until new lookup successfully loads. Default is 60 Sec.',
    required: false,
    suggestions: [],
  },
  {
    name: 'extractionNamespace.maxHeapPercentage',
    type: 'number',
    defaultValue: 10,
    defined: l =>
      oneOfKnown(deepGet(l, 'extractionNamespace.type'), KNOWN_EXTRACTION_NAMESPACE_TYPES, 'jdbc'),
    info: 'The maximum percentage of heap size that the lookup should consume. If the lookup grows beyond this size, warning messages will be logged in the respective service logs. Default is 10 % of jvm size.',
    required: false,
    suggestions: [],
  },

  // Extra cachedNamespace things
  {
    name: 'firstCacheTimeout',
    type: 'number',
    defaultValue: 0,
    defined: typeIsKnown(KNOWN_TYPES, 'cachedNamespace'),
    info: `How long to wait (in ms) for the first run of the cache to populate. 0 indicates to not wait`,
  },
  {
    name: 'injective',
    type: 'boolean',
    defaultValue: false,
    defined: typeIsKnown(KNOWN_TYPES, 'cachedNamespace'),
    info: `If the underlying map is injective (keys and values are unique) then optimizations can occur internally by setting this to true`,
  },

  // kafka lookups
  {
    name: 'kafkaTopic',
    type: 'string',
    defined: typeIsKnown(KNOWN_TYPES, 'kafka'),
    required: true,
    info: `The Kafka topic to read the data from`,
  },
  {
    name: 'kafkaProperties',
    type: 'json',
    height: '100px',
    defined: typeIsKnown(KNOWN_TYPES, 'kafka'),
    required: true,
    issueWithValue: value => {
      if (!value) return 'kafkaProperties must be defined';
      if (typeof value !== 'object') return `kafkaProperties must be an object`;
      if (!value['bootstrap.servers']) return 'bootstrap.servers must be defined';
      return;
    },
  },
  {
    name: 'connectTimeout',
    type: 'number',
    defaultValue: 0,
    defined: typeIsKnown(KNOWN_TYPES, 'kafka'),
    info: `How long to wait for an initial connection`,
  },
  {
    name: 'isOneToOne',
    type: 'boolean',
    defaultValue: false,
    defined: typeIsKnown(KNOWN_TYPES, 'kafka'),
    info: `If the underlying map is one-to-one (keys and values are unique) then optimizations can occur internally by setting this to true`,
  },
];

export function isLookupInvalid(
  lookupId: string | undefined,
  lookupVersion: string | undefined,
  lookupTier: string | undefined,
  lookupSpec: Partial<LookupSpec>,
) {
  return (
    !lookupId || !lookupVersion || !lookupTier || !AutoForm.isValidModel(lookupSpec, LOOKUP_FIELDS)
  );
}

export function lookupSpecSummary(spec: LookupSpec): string {
  const { type, map, extractionNamespace, kafkaTopic, kafkaProperties } = spec;

  switch (type) {
    case 'map':
      if (!map) return 'No map';
      return pluralIfNeeded(Object.keys(map).length, 'key');

    case 'cachedNamespace':
      if (!extractionNamespace) return 'No extractionNamespace';
      switch (extractionNamespace.type) {
        case 'uri':
          if (extractionNamespace.uriPrefix) {
            return `URI prefix: ${extractionNamespace.uriPrefix}, Match: ${
              extractionNamespace.fileRegex || '.*'
            }`;
          }
          if (extractionNamespace.uri) {
            return `URI: ${extractionNamespace.uri}`;
          }
          return 'Unknown extractionNamespace lookup';

        case 'jdbc': {
          const columns = [
            `${extractionNamespace.keyColumn} AS key`,
            `${extractionNamespace.valueColumn} AS value`,
          ];
          if (extractionNamespace.tsColumn) {
            columns.push(`${extractionNamespace.tsColumn} AS ts`);
          }
          const queryParts = ['SELECT', columns.join(', '), `FROM ${extractionNamespace.table}`];
          if (extractionNamespace.filter) {
            queryParts.push(`WHERE ${extractionNamespace.filter}`);
          }
          return `${
            extractionNamespace.connectorConfig?.connectURI || 'No connectURI'
          } [${queryParts.join(' ')}]`;
        }

        default:
          return `Unknown lookup extractionNamespace type ${extractionNamespace.type}`;
      }

    case 'kafka': {
      const servers = kafkaProperties?.['bootstrap.servers'];
      return `Topic: ${kafkaTopic}` + (servers ? ` (on: ${servers})` : '');
    }

    default:
      return `Unknown lookup type ${type}`;
  }
}
