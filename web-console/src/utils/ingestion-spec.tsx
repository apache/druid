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
import { number } from 'prop-types';
import * as React from 'react';

import { Field } from '../components/auto-form';
import { ExternalLink } from '../components/external-link';

import { TIMESTAMP_FORMAT_VALUES } from './druid-time';
import { deepGet, deepSet } from './object-change';

export interface IngestionSpec {
  type?: IngestionType;
  dataSchema: DataSchema;
  ioConfig: IoConfig;
  tuningConfig?: TuningConfig;
}

export type IngestionType = 'kafka' | 'kinesis' | 'index_hadoop' | 'index' | 'index_parallel';

// A combination of IngestionType and firehose
export type IngestionComboType =
  'kafka' |
  'kinesis' |
  'index:http' |
  'index:local' |
  'index:static-s3' |
  'index:static-google-blobstore';

function ingestionTypeToIoAndTuningConfigType(ingestionType: IngestionType): string {
  switch (ingestionType) {
    case 'kafka':
    case 'kinesis':
    case 'index':
    case 'index_parallel':
      return ingestionType;

    case 'index_hadoop':
      return 'hadoop';

    default:
      throw new Error(`unknown type '${ingestionType}'`);
  }
}

export function getIngestionComboType(spec: IngestionSpec): IngestionComboType | null {
  const ioConfig = deepGet(spec, 'ioConfig') || {};

  switch (ioConfig.type) {
    case 'kafka':
    case 'kinesis':
      return ioConfig.type;

    case 'index':
    case 'index_parallel':
      const firehose = deepGet(spec, 'ioConfig.firehose') || {};
      switch (firehose.type) {
        case 'local':
        case 'http':
        case 'static-s3':
        case 'static-google-blobstore':
          return `index:${firehose.type}` as any;
      }
  }

  return null;
}

// --------------

export interface DataSchema {
  dataSource: string;
  parser: Parser;
  transformSpec?: TransformSpec;
  granularitySpec?: GranularitySpec;
  metricsSpec?: MetricSpec[];
}

export interface Parser {
  type?: string;
  parseSpec: ParseSpec;
}

export interface ParseSpec {
  format: string;
  hasHeaderRow?: boolean;
  skipHeaderRows?: number;
  columns?: string[];
  listDelimiter?: string;
  pattern?: string;
  'function'?: string;

  timestampSpec: TimestampSpec;
  dimensionsSpec: DimensionsSpec;
  flattenSpec?: FlattenSpec;
}

export function hasParallelAbility(spec: IngestionSpec): boolean {
  return spec.type === 'index' || spec.type === 'index_parallel';
}

export function isParallel(spec: IngestionSpec): boolean {
  return spec.type === 'index_parallel';
}

export type DimensionMode = 'specific' | 'auto-detect';

export function getDimensionMode(spec: IngestionSpec): DimensionMode {
  const dimensions = deepGet(spec, 'dataSchema.parser.parseSpec.dimensionsSpec.dimensions') || [];
  return Array.isArray(dimensions) && dimensions.length === 0 ? 'auto-detect' : 'specific';
}

export function getRollup(spec: IngestionSpec): boolean {
  const specRollup = deepGet(spec, 'dataSchema.granularitySpec.rollup');
  return typeof specRollup === 'boolean' ? specRollup : true;
}

export function changeParallel(spec: IngestionSpec, parallel: boolean): IngestionSpec {
  if (!hasParallelAbility(spec)) return spec;
  const newType = parallel ? 'index_parallel' : 'index';
  let newSpec = spec;
  newSpec = deepSet(newSpec, 'type', newType);
  newSpec = deepSet(newSpec, 'ioConfig.type', newType);
  newSpec = deepSet(newSpec, 'tuningConfig.type', newType);
  return newSpec;
}

const PARSE_SPEC_FORM_FIELDS: Field<ParseSpec>[] = [
  {
    name: 'format',
    label: 'Parser to use',
    type: 'string',
    suggestions: ['json', 'csv', 'tsv', 'regex'],
    info: <>
      <p>The parser used to parse the data.</p>
      <p>For more information see <ExternalLink href="http://druid.io/docs/latest/ingestion/data-formats.html">the documentation</ExternalLink>.</p>
    </>
  },
  {
    name: 'pattern',
    type: 'string',
    isDefined: (p: ParseSpec) => p.format === 'regex'
  },
  {
    name: 'function',
    type: 'string',
    isDefined: (p: ParseSpec) => p.format === 'javascript'
  },
  {
    name: 'hasHeaderRow',
    type: 'boolean',
    defaultValue: true,
    isDefined: (p: ParseSpec) => p.format === 'csv' || p.format === 'tsv'
  },
  {
    name: 'skipHeaderRows',
    type: 'number',
    defaultValue: 0,
    isDefined: (p: ParseSpec) => p.format === 'csv' || p.format === 'tsv',
    min: 0,
    info: <>
      If both skipHeaderRows and hasHeaderRow options are set, skipHeaderRows is first applied. For example, if you set skipHeaderRows to 2 and hasHeaderRow to true, Druid will skip the first two lines and then extract column information from the third line.
    </>
  },
  {
    name: 'columns',
    type: 'string-array',
    isDefined: (p: ParseSpec) => ((p.format === 'csv' || p.format === 'tsv') && !p.hasHeaderRow) || p.format === 'regex'
  },
  {
    name: 'listDelimiter',
    type: 'string',
    defaultValue: '|',
    isDefined: (p: ParseSpec) => p.format === 'csv' || p.format === 'tsv'
  }
];

export function getParseSpecFormFields() {
  return PARSE_SPEC_FORM_FIELDS;
}

export function issueWithParser(parser: Parser | undefined): string | null {
  if (!parser) return 'no parser';
  if (parser.type === 'map') return null;

  const { parseSpec } = parser;
  if (!parseSpec) return 'no parse spec';
  if (!parseSpec.format) return 'missing a format';
  switch (parseSpec.format) {
    case 'regex':
      if (!parseSpec.pattern) return "must have a 'pattern'";
      break;

    case 'javascript':
      if (!parseSpec['function']) return "must have a 'function'";
      break;

  }
  return null;
}

export function parseSpecHasFlatten(parseSpec: ParseSpec): boolean {
  return parseSpec.format === 'json';
}

export interface TimestampSpec {
  column?: string;
  format?: string;
  missingValue?: string;
}

export function getTimestampSpecColumn(timestampSpec: TimestampSpec) {
  // https://github.com/apache/incubator-druid/blob/master/core/src/main/java/org/apache/druid/data/input/impl/TimestampSpec.java#L44
  return timestampSpec.column || 'timestamp';
}

const NO_SUCH_COLUMN = '!!!_no_such_column_!!!';

const EMPTY_TIMESTAMP_SPEC: TimestampSpec = {
  column: NO_SUCH_COLUMN,
  missingValue: '2010-01-01T00:00:00Z'
};

export function getEmptyTimestampSpec() {
  return EMPTY_TIMESTAMP_SPEC;
}

export function isColumnTimestampSpec(timestampSpec: TimestampSpec) {
  return (deepGet(timestampSpec, 'column') || 'timestamp') !== NO_SUCH_COLUMN;
}

const TIMESTAMP_SPEC_FORM_FIELDS: Field<TimestampSpec>[] = [
  {
    name: 'column',
    type: 'string',
    isDefined: (timestampSpec: TimestampSpec) => isColumnTimestampSpec(timestampSpec)
  },
  {
    name: 'format',
    type: 'string',
    suggestions: ['auto'].concat(TIMESTAMP_FORMAT_VALUES),
    isDefined: (timestampSpec: TimestampSpec) => isColumnTimestampSpec(timestampSpec),
    info: <p>
      Please specify your timestamp format by using the suggestions menu or typing in a <ExternalLink href="https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html">format string</ExternalLink>.
    </p>
  },
  {
    name: 'missingValue',
    type: 'string',
    isDefined: (timestampSpec: TimestampSpec) => !isColumnTimestampSpec(timestampSpec)
  }
];

export function getTimestampSpecFormFields() {
  return TIMESTAMP_SPEC_FORM_FIELDS;
}

export function issueWithTimestampSpec(timestampSpec: TimestampSpec | undefined): string | null {
  if (!timestampSpec) return 'no spec';
  if (!timestampSpec.column && !timestampSpec.missingValue) return 'timestamp spec is blank';
  return null;
}

export interface DimensionsSpec {
  dimensions?: (string | DimensionSpec)[];
  dimensionExclusions?: string[];
  spatialDimensions?: any[];
}

export interface DimensionSpec {
  type: string;
  name: string;
  createBitmapIndex?: boolean;
}

const DIMENSION_SPEC_FORM_FIELDS: Field<DimensionSpec>[] = [
  {
    name: 'name',
    type: 'string'
  },
  {
    name: 'type',
    type: 'string',
    suggestions: ['string', 'long', 'float']
  },
  {
    name: 'createBitmapIndex',
    type: 'boolean',
    defaultValue: true,
    isDefined: (dimensionSpec: DimensionSpec) => dimensionSpec.type === 'string'
  }
];

export function getDimensionSpecFormFields() {
  return DIMENSION_SPEC_FORM_FIELDS;
}

export function getDimensionSpecName(dimensionSpec: string | DimensionSpec): string {
  return typeof dimensionSpec === 'string' ? dimensionSpec : dimensionSpec.name;
}

export function getDimensionSpecType(dimensionSpec: string | DimensionSpec): string {
  return typeof dimensionSpec === 'string' ? 'string' : dimensionSpec.type;
}

export function inflateDimensionSpec(dimensionSpec: string | DimensionSpec): DimensionSpec {
  return typeof dimensionSpec === 'string' ? { name: dimensionSpec, type: 'string' } : dimensionSpec;
}

export interface FlattenSpec {
  useFieldDiscovery?: boolean;
  fields?: FlattenField[];
}

export interface FlattenField {
  name: string;
  type: string;
  expr: string;
}

const FLATTEN_FIELD_FORM_FIELDS: Field<FlattenField>[] = [
  {
    name: 'name',
    type: 'string',
    placeholder: 'column_name'
  },
  {
    name: 'type',
    type: 'string',
    suggestions: ['path', 'jq', 'root']
  },
  {
    name: 'expr',
    type: 'string',
    placeholder: '$.thing',
    isDefined: (flattenField: FlattenField) => flattenField.type === 'path' || flattenField.type === 'jq',
    info: <>
      Specify a flatten <ExternalLink href="http://druid.io/docs/latest/ingestion/flatten-json">expression</ExternalLink>.
    </>
  }
];

export function getFlattenFieldFormFields() {
  return FLATTEN_FIELD_FORM_FIELDS;
}

export interface TransformSpec {
  transforms: Transform[];
  filter?: any;
}

export interface Transform {
  type: string;
  name: string;
  expression: string;
}

const TRANSFORM_FORM_FIELDS: Field<Transform>[] = [
  {
    name: 'name',
    type: 'string',
    placeholder: 'output_name'
  },
  {
    name: 'type',
    type: 'string',
    suggestions: ['expression']
  },
  {
    name: 'expression',
    type: 'string',
    placeholder: '"foo" + "bar"',
    info: <>
      A valid Druid <ExternalLink href="http://druid.io/docs/latest/misc/math-expr.html">expression</ExternalLink>.
    </>
  }
];

export function getTransformFormFields() {
  return TRANSFORM_FORM_FIELDS;
}

export interface GranularitySpec {
  type?: string;
  queryGranularity?: string;
  segmentGranularity?: string;
  rollup?: boolean;
  intervals?: string;
}

export interface MetricSpec {
  type: string;
  name?: string;
  fieldName?: string;
  maxStringBytes?: number;
  filterNullValues?: boolean;
  fieldNames?: string[];
  fnAggregate?: string;
  fnCombine?: string;
  fnReset?: string;
  fields?: string[];
  byRow?: boolean;
  round?: boolean;
  isInputHyperUnique?: boolean;
  filter?: any;
  aggregator?: MetricSpec;
}

const METRIC_SPEC_FORM_FIELDS: Field<MetricSpec>[] = [
  {
    name: 'name',
    type: 'string'
  },
  {
    name: 'type',
    type: 'string',
    suggestions: [
      'count',
      {
        group: 'sum',
        suggestions: [
          'longSum',
          'doubleSum',
          'floatSum'
        ]
      },
      {
        group: 'min',
        suggestions: [
          'longMin',
          'doubleMin',
          'floatMin'
        ]
      },
      {
        group: 'max',
        suggestions: [
          'longMax',
          'doubleMax',
          'floatMax'
        ]
      },
      {
        group: 'first',
        suggestions: [
          'longFirst',
          'doubleFirst',
          'floatFirst'
        ]
      },
      {
        group: 'last',
        suggestions: [
          'longLast',
          'doubleLast',
          'floatLast'
        ]
      },
      'cardinality',
      'hyperUnique',
      'filtered'
    ]
  },
  {
    name: 'fieldName',
    type: 'string',
    isDefined: m => {
      return [
        'longSum',
        'doubleSum',
        'floatSum',
        'longMin',
        'doubleMin',
        'floatMin',
        'longMax',
        'doubleMax',
        'floatMax',
        'longFirst',
        'doubleFirst',
        'floatFirst',
        'stringFirst',
        'longLast',
        'doubleLast',
        'floatLast',
        'stringLast',
        'cardinality',
        'hyperUnique'
      ].includes(m.type);
    }
  },
  {
    name: 'maxStringBytes',
    type: 'number',
    defaultValue: 1024,
    isDefined: m => {
      return ['stringFirst', 'stringLast'].includes(m.type);
    }
  },
  {
    name: 'filterNullValues',
    type: 'boolean',
    defaultValue: false,
    isDefined: m => {
      return ['stringFirst', 'stringLast'].includes(m.type);
    }
  },
  {
    name: 'filter',
    type: 'json',
    isDefined: m => {
      return m.type === 'filtered';
    }
  },
  {
    name: 'aggregator',
    type: 'json',
    isDefined: m => {
      return m.type === 'filtered';
    }
  }
  // ToDo: fill in approximates
];

export function getMetricSpecFormFields() {
  return METRIC_SPEC_FORM_FIELDS;
}

export function getMetricSpecName(metricSpec: MetricSpec): string {
  return metricSpec.name || (metricSpec.aggregator ? getMetricSpecName(metricSpec.aggregator) : '?');
}

// --------------

export interface IoConfig {
  type: string;
  firehose?: Firehose;
  appendToExisting?: boolean;
  topic?: string;
  consumerProperties?: any;
  replicas?: number;
  taskCount?: number;
  taskDuration?: string;
  startDelay?: string;
  period?: string;
  useEarliestOffset?: boolean;
  stream?: string;
  region?: string;
  useEarliestSequenceNumber?: boolean;
}

export interface Firehose {
  type: string;
  baseDir?: string;
  filter?: string;
  uris?: string[];
  prefixes?: string[];
  blobs?: { bucket: string, path: string }[];
  fetchTimeout?: number;
}

export function getIoConfigFormFields(ingestionComboType: IngestionComboType): Field<IoConfig>[] {
  const firehoseType: Field<IoConfig> = {
    name: 'firehose.type',
    label: 'Firehose type',
    type: 'string',
    suggestions: ['local', 'http', 'static-s3', 'static-google-blobstore'],
    info: <>
      <p>
        Druid connects to raw data through <ExternalLink href="http://druid.io/docs/latest/ingestion/firehose.html">firehoses</ExternalLink>.
        You can change your selected firehose here.
      </p>
    </>
  };

  switch (ingestionComboType) {
    case 'index:http':
      return [
        firehoseType,
        {
          name: 'firehose.uris',
          label: 'URIs',
          type: 'string-array',
          placeholder: 'https://example.com/path/to/file.ext',
          info: <>
            <p>The full URI of your file. To ingest from multiple URIs, use commas to separate each individual URI.</p>
          </>
        }
      ];

    case 'index:local':
      return [
        firehoseType,
        {
          name: 'firehose.baseDir',
          label: 'Base directory',
          type: 'string',
          placeholder: '/path/to/files/',
          info: <>
            <ExternalLink href="http://druid.io/docs/latest/ingestion/firehose.html#localfirehose">firehose.baseDir</ExternalLink>
            <p>Specifies the directory to search recursively for files to be ingested.</p>
          </>
        },
        {
          name: 'firehose.filter',
          label: 'File filter',
          type: 'string',
          defaultValue: '*.*',
          info: <>
            <ExternalLink href="http://druid.io/docs/latest/ingestion/firehose.html#localfirehose">firehose.filter</ExternalLink>
            <p>A wildcard filter for files. See <ExternalLink href="https://commons.apache.org/proper/commons-io/apidocs/org/apache/commons/io/filefilter/WildcardFileFilter.html">here</ExternalLink> for format information.</p>
          </>
        }
      ];

    case 'index:static-s3':
      return [
        firehoseType,
        {
          name: 'firehose.uris',
          label: 'S3 URIs',
          type: 'string-array',
          placeholder: 's3://your-bucket/some-file.extension',
          isDefined: (ioConfig) => !deepGet(ioConfig, 'firehose.prefixes'),
          info: <>
            <p>The full S3 URI of your file. To ingest from multiple URIs, use commas to separate each individual URI.</p>
            <p>Either S3 URIs or S3 prefixes must be set.</p>
          </>
        },
        {
          name: 'firehose.prefixes',
          label: 'S3 prefixes',
          type: 'string-array',
          placeholder: 's3://your-bucket/some-path',
          isDefined: (ioConfig) => !deepGet(ioConfig, 'firehose.uris'),
          info: <>
            <p>A list of paths (with bucket) where your files are stored.</p>
            <p>Either S3 URIs or S3 prefixes must be set.</p>
          </>
        }
      ];

    case 'index:static-google-blobstore':
      return [
        firehoseType,
        {
          name: 'firehose.blobs',
          label: 'Google blobs',
          type: 'json',
          info: <>
            <p>JSON array of <ExternalLink href="http://druid.io/docs/latest/development/extensions-contrib/google.html">Google Blobs</ExternalLink>.</p>
          </>
        }
      ];

    case 'kafka':
      return [
        {
          name: 'consumerProperties.{bootstrap.servers}',
          label: 'Bootstrap servers',
          type: 'string',
          info: <>
            <ExternalLink href="http://druid.io/docs/latest/development/extensions-core/kafka-ingestion#kafkasupervisorioconfig">consumerProperties</ExternalLink>
            <p>A list of Kafka brokers in the form: <Code>{`<BROKER_1>:<PORT_1>,<BROKER_2>:<PORT_2>,...`}</Code></p>
          </>
        },
        {
          name: 'topic',
          type: 'string',
          isDefined: (i: IoConfig) => i.type === 'kafka'
        },
        {
          name: 'consumerProperties',
          type: 'json',
          defaultValue: {},
          info: <>
            <ExternalLink href="http://druid.io/docs/latest/development/extensions-core/kafka-ingestion#kafkasupervisorioconfig">consumerProperties</ExternalLink>
            <p>A map of properties to be passed to the Kafka consumer.</p>
          </>
        }
      ];

    case 'kinesis':
      return [
        {
          name: 'stream',
          type: 'string'
        },
        {
          name: 'region',
          type: 'string'
        },
        {
          name: 'useEarliestOffset',
          type: 'boolean',
          defaultValue: true,
          isDefined: (i: IoConfig) => i.type === 'kafka' || i.type === 'kinesis'
        },
        {
          name: 'useEarliestSequenceNumber',
          type: 'boolean',
          isDefined: (i: IoConfig) => i.type === 'kinesis'
        }
      ];
  }

  throw new Error(`unknown input type ${ingestionComboType}`);
}

function nonEmptyArray(a: any) {
  return Array.isArray(a) && Boolean(a.length);
}

function issueWithFirehose(firehose: Firehose | undefined): string | null {
  if (!firehose) return 'does not exist';
  if (!firehose.type) return 'missing a type';
  switch (firehose.type) {
    case 'local':
      if (!firehose.baseDir) return "must have a 'baseDir'";
      if (!firehose.filter) return "must have a 'filter'";
      break;

    case 'http':
      if (!nonEmptyArray(firehose.uris)) return 'must have at least one uri';
      break;

    case 'static-s3':
      if (!nonEmptyArray(firehose.uris) && !nonEmptyArray(firehose.prefixes)) return 'must have at least one uri or prefix';
      break;

    case 'static-google-blobstore':
      if (!nonEmptyArray(firehose.blobs)) return 'must have at least one blob';
      break;
  }
  return null;
}

export function issueWithIoConfig(ioConfig: IoConfig | undefined): string | null {
  if (!ioConfig) return 'does not exist';
  if (!ioConfig.type) return 'missing a type';
  switch (ioConfig.type) {
    case 'index':
    case 'index_parallel':
      if (issueWithFirehose(ioConfig.firehose)) return `firehose: '${issueWithFirehose(ioConfig.firehose)}'`;
      break;

    case 'kafka':
      if (!ioConfig.topic) return 'must have a topic';
      break;

    case 'kinesis':
      // if (!ioConfig.stream) return "must have a stream";
      break;
  }

  return null;
}

export function getIoConfigTuningFormFields(ingestionComboType: IngestionComboType): Field<IoConfig>[] {
  switch (ingestionComboType) {
    case 'index:http':
    case 'index:static-s3':
    case 'index:static-google-blobstore':
      const objectType = ingestionComboType === 'index:http' ? 'http' : 'S3';
      return [
        {
          name: 'firehose.maxCacheCapacityBytes',
          label: 'Max cache capacity bytes',
          type: 'number',
          defaultValue: 1073741824,
          info: <>
            <p>Maximum size of the cache space in bytes. 0 means disabling cache. Cached files are not removed until the ingestion task completes.</p>
          </>
        },
        {
          name: 'firehose.maxFetchCapacityBytes',
          label: 'Max fetch capacity bytes',
          type: 'number',
          defaultValue: 1073741824,
          info: <>
            <p>Maximum size of the fetch space in bytes. 0 means disabling prefetch. Prefetched files are removed immediately once they are read.</p>
          </>
        },
        {
          name: 'firehose.prefetchTriggerBytes',
          label: 'Prefetch trigger bytes',
          type: 'number',
          info: <>
            <p>Threshold to trigger prefetching {objectType} objects.</p>
            <p>Default: maxFetchCapacityBytes / 2</p>
          </>
        },
        {
          name: 'firehose.fetchTimeout',
          label: 'Fetch timeout',
          type: 'number',
          defaultValue: 60000,
          info: <>
            <p>Timeout for fetching a http object.</p>
          </>
        },
        {
          name: 'firehose.maxFetchRetry',
          label: 'Max fetch retry',
          type: 'number',
          defaultValue: 3,
          info: <>
            <p>Maximum retry for fetching a {objectType} object.</p>
          </>
        }
      ];

    case 'index:local':
      return [];

    case 'kafka':
      return [
        // ToDo: fill this in
      ];

    case 'kinesis':
      return [
        // ToDo: fill this in
      ];
  }

  throw new Error(`unknown input type ${ingestionComboType}`);
}

// ---------------------------------------

function filenameFromPath(path: string | undefined): string | null {
  if (!path) return null;
  const m = path.match(/([^\/.]+)[^\/]*?\/?$/);
  return m ? m[1] : null;
}

export function fillDataSourceName(spec: IngestionSpec): IngestionSpec {
  const ioConfig = deepGet(spec, 'ioConfig');
  if (!ioConfig) return spec;
  const possibleName = guessDataSourceName(ioConfig);
  if (!possibleName) return spec;
  return deepSet(spec, 'dataSchema.dataSource', possibleName);
}

export function guessDataSourceName(ioConfig: IoConfig): string | null {
  switch (ioConfig.type) {
    case 'index':
    case 'index_parallel':
      const firehose = ioConfig.firehose;
      if (!firehose) return null;

      switch (firehose.type) {
        case 'local':
          return filenameFromPath(firehose.baseDir);

        case 'static-s3':
          return filenameFromPath((firehose.uris || [])[0] || (firehose.prefixes || [])[0]);

        case 'http':
          return filenameFromPath(firehose.uris ? firehose.uris[0] : undefined);
      }

      return null;

    case 'kafka':
      return ioConfig.topic || null;

    case 'kinesis':
      return ioConfig.stream || null;

    default:
      return null;
  }
}

// --------------

export interface TuningConfig {
  type: string;
  targetPartitionSize?: number;
  maxRowsInMemory?: number;
  maxBytesInMemory?: number;
  maxTotalRows?: number;
  numShards?: number;
  maxPendingPersists?: number;
  indexSpec?: IndexSpec;
  forceExtendableShardSpecs?: boolean;
  forceGuaranteedRollup?: boolean;
  reportParseExceptions?: boolean;
  pushTimeout?: number;
  segmentWriteOutMediumFactory?: any;
  // ...
  maxParseExceptions?: number;
  maxSavedParseExceptions?: number;
}

const TUNING_CONFIG_FORM_FIELDS: Field<TuningConfig>[] = [
  {
    name: 'maxRowsInMemory',
    type: 'number',
    defaultValue: 1000000,
    info: <>
      Used in determining when intermediate persists to disk should occur.
    </>
  },
  {
    name: 'maxBytesInMemory',
    type: 'number',
    placeholder: 'Default: 1/6 of max JVM memory',
    info: <>
      Used in determining when intermediate persists to disk should occur.
    </>
  },
  {
    name: 'maxPendingPersists',
    type: 'number'
  },
  {
    name: 'forceExtendableShardSpecs',
    type: 'boolean'
  },
  {
    name: 'reportParseExceptions',
    type: 'boolean'
  },
  {
    name: 'pushTimeout',
    type: 'number',
    defaultValue: 0,
    info: <>
      Milliseconds to wait for pushing segments.
      It must be >= 0, where 0 means to wait forever.
    </>
  },
  {
    name: 'maxNumSubTasks',
    type: 'number',
    defaultValue: 1,
    info: <>
      Maximum number of tasks which can be run at the same time.
      The supervisor task would spawn worker tasks up to maxNumSubTasks regardless of the available task slots.
      If this value is set to 1, the supervisor task processes data ingestion on its own instead of spawning worker tasks.
      If this value is set to too large, too many worker tasks can be created which might block other ingestion.
    </>
  },
  {
    name: 'maxRetry',
    type: 'number',
    defaultValue: 3,
    info: <>
      Maximum number of retries on task failures.
    </>
  },
  {
    name: 'taskStatusCheckPeriodMs',
    type: 'number',
    defaultValue: 1000,
    info: <>
      Polling period in milliseconds to check running task statuses.
    </>
  },
  {
    name: 'chatHandlerTimeout',
    type: 'string',
    defaultValue: 'PT10S',
    info: <>
      Timeout for reporting the pushed segments in worker tasks.
    </>
  },
  {
    name: 'chatHandlerNumRetries',
    type: 'number',
    defaultValue: 5,
    info: <>
      Retries for reporting the pushed segments in worker tasks.
    </>
  }
];

export function getTuningSpecFormFields() {
  return TUNING_CONFIG_FORM_FIELDS;
}

export interface IndexSpec {
  bitmap?: Bitmap;
  dimensionCompression?: string;
  metricCompression?: string;
  longEncoding?: string;
}

export interface Bitmap {
  type: string;
  compressRunOnSerialization?: boolean;
}

// --------------

export function getBlankSpec(ingestionType: IngestionType = 'index', firehoseType: string | null = null): IngestionSpec {
  const ioAndTuningConfigType = ingestionTypeToIoAndTuningConfigType(ingestionType);

  const granularitySpec: GranularitySpec = {
    type: 'uniform',
    segmentGranularity: ['index', 'index_parallel'].includes(ingestionType) ? 'DAY' : 'HOUR',
    queryGranularity: 'HOUR'
  };

  const spec: IngestionSpec = {
    type: ingestionType,
    dataSchema: {
      dataSource: 'new-data-source',
      granularitySpec
    },
    ioConfig: {
      type: ioAndTuningConfigType
    },
    tuningConfig: {
      type: ioAndTuningConfigType
    }
  } as any;

  if (firehoseType) {
    spec.ioConfig.firehose = {
      type: firehoseType
    };
  }

  return spec;
}

export function fillParser(spec: IngestionSpec, sampleData: string[]): IngestionSpec {
  if (deepGet(spec, 'ioConfig.firehose.type') === 'sql') {
    return deepSet(spec, 'dataSchema.parser', { type: 'map' });
  }

  const parseSpec =  guessParseSpec(sampleData);
  if (!parseSpec) return spec;

  return deepSet(spec, 'dataSchema.parser', { type: 'string', parseSpec });
}

function guessParseSpec(sampleData: string[]): ParseSpec | null {
  const sampleDatum = sampleData[0];
  if (!sampleDatum) return null;

  if (sampleDatum.startsWith('{') && sampleDatum.endsWith('}')) {
    return parseSpecFromFormat('json');
  }

  if (sampleDatum.split('\t').length > 3) {
    return parseSpecFromFormat('tsv', !/\t\d+\t/.test(sampleDatum));
  }

  if (sampleDatum.split(',').length > 3) {
    return parseSpecFromFormat('csv', !/,\d+,/.test(sampleDatum));
  }

  return parseSpecFromFormat('regex');
}

function parseSpecFromFormat(format: string, hasHeaderRow: boolean | null = null): ParseSpec {
  const parseSpec: ParseSpec = {
    format,
    timestampSpec: {},
    dimensionsSpec: {}
  };

  if (typeof hasHeaderRow === 'boolean') {
    parseSpec.hasHeaderRow = hasHeaderRow;
  }

  return parseSpec;
}

export type DruidFilter = Record<string, any>;

export interface DimensionFiltersWithRest {
  dimensionFilters: DruidFilter[];
  restFilter: DruidFilter | null;
}

export function splitFilter(filter: DruidFilter | null): DimensionFiltersWithRest {
  const inputAndFilters: DruidFilter[] = filter ? ((filter.type === 'and' && Array.isArray(filter.fields)) ? filter.fields : [filter]) : [];
  const dimensionFilters: DruidFilter[] = inputAndFilters.filter(f => typeof f.dimension === 'string');
  const restFilters: DruidFilter[] = inputAndFilters.filter(f => typeof f.dimension !== 'string');

  return {
    dimensionFilters,
    restFilter: restFilters.length ? (restFilters.length > 1 ? { type: 'and', filters: restFilters } : restFilters[0]) : null
  };
}

export function joinFilter(dimensionFiltersWithRest: DimensionFiltersWithRest): DruidFilter | null {
  const { dimensionFilters, restFilter } = dimensionFiltersWithRest;
  let newFields = dimensionFilters || [];
  if (restFilter && restFilter.type) newFields = newFields.concat([restFilter]);

  if (!newFields.length) return null;
  if (newFields.length === 1) return newFields[0];
  return { type: 'and', fields: newFields };
}

const FILTER_FORM_FIELDS: Field<DruidFilter>[] = [
  {
    name: 'type',
    type: 'string',
    suggestions: ['selector', 'in']
  },
  {
    name: 'dimension',
    type: 'string'
  },
  {
    name: 'value',
    type: 'string',
    isDefined: (druidFilter: DruidFilter) => druidFilter.type === 'selector'
  },
  {
    name: 'values',
    type: 'string-array',
    isDefined: (druidFilter: DruidFilter) => druidFilter.type === 'in'
  }
];

export function getFilterFormFields() {
  return FILTER_FORM_FIELDS;
}
