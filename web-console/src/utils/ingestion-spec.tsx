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

import { Field } from '../components/auto-form/auto-form';
import { ExternalLink } from '../components/external-link/external-link';

import {
  BASIC_TIME_FORMATS,
  DATE_ONLY_TIME_FORMATS,
  DATETIME_TIME_FORMATS,
  OTHER_TIME_FORMATS,
} from './druid-time';
import { deepGet, deepSet } from './object-change';

export const MAX_INLINE_DATA_LENGTH = 65536;

// These constants are used to make sure that they are not constantly recreated thrashing the pure components
export const EMPTY_OBJECT: any = {};
export const EMPTY_ARRAY: any[] = [];

const CURRENT_YEAR = new Date().getUTCFullYear();

export interface IngestionSpec {
  type?: IngestionType;
  dataSchema: DataSchema;
  ioConfig: IoConfig;
  tuningConfig?: TuningConfig;
}

export function isEmptyIngestionSpec(spec: IngestionSpec) {
  return Object.keys(spec).length === 0;
}

export type IngestionType = 'kafka' | 'kinesis' | 'index_hadoop' | 'index' | 'index_parallel';

// A combination of IngestionType and firehose
export type IngestionComboType =
  | 'kafka'
  | 'kinesis'
  | 'index:http'
  | 'index:local'
  | 'index:ingestSegment'
  | 'index:inline'
  | 'index:static-s3'
  | 'index:static-google-blobstore';

// Some extra values that can be selected in the initial screen
export type IngestionComboTypeWithExtra = IngestionComboType | 'hadoop' | 'example' | 'other';

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
  const ioConfig = deepGet(spec, 'ioConfig') || EMPTY_OBJECT;

  switch (ioConfig.type) {
    case 'kafka':
    case 'kinesis':
      return ioConfig.type;

    case 'index':
    case 'index_parallel':
      const firehose = deepGet(spec, 'ioConfig.firehose') || EMPTY_OBJECT;
      switch (firehose.type) {
        case 'local':
        case 'http':
        case 'ingestSegment':
        case 'inline':
        case 'static-s3':
        case 'static-google-blobstore':
          return `index:${firehose.type}` as IngestionComboType;
      }
  }

  return null;
}

export function getIngestionTitle(ingestionType: IngestionComboTypeWithExtra): string {
  switch (ingestionType) {
    case 'index:local':
      return 'Local disk';

    case 'index:http':
      return 'HTTP(s)';

    case 'index:ingestSegment':
      return 'Reindex from Druid';

    case 'index:inline':
      return 'Paste data';

    case 'index:static-s3':
      return 'Amazon S3';

    case 'index:static-google-blobstore':
      return 'Google Cloud Storage';

    case 'kafka':
      return 'Apache Kafka';

    case 'kinesis':
      return 'Amazon Kinesis';

    case 'hadoop':
      return 'HDFS';

    case 'example':
      return 'Example data';

    case 'other':
      return 'Other';

    default:
      return 'Unknown ingestion';
  }
}

export function getIngestionImage(ingestionType: IngestionComboTypeWithExtra): string {
  const parts = ingestionType.split(':');
  if (parts.length === 2) return parts[1].toLowerCase();
  return ingestionType;
}

export function getRequiredModule(ingestionType: IngestionComboTypeWithExtra): string | undefined {
  switch (ingestionType) {
    case 'index:static-s3':
      return 'druid-s3-extensions';

    case 'index:static-google-blobstore':
      return 'druid-google-extensions';

    case 'kafka':
      return 'druid-kafka-indexing-service';

    case 'kinesis':
      return 'druid-kinesis-indexing-service';

    default:
      return;
  }
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
  function?: string;

  timestampSpec: TimestampSpec;
  dimensionsSpec: DimensionsSpec;
  flattenSpec?: FlattenSpec;
}

export function hasParallelAbility(spec: IngestionSpec): boolean {
  const specType = getSpecType(spec);
  return specType === 'index' || specType === 'index_parallel';
}

export function isParallel(spec: IngestionSpec): boolean {
  const specType = getSpecType(spec);
  return specType === 'index_parallel';
}

export type DimensionMode = 'specific' | 'auto-detect';

export function getDimensionMode(spec: IngestionSpec): DimensionMode {
  const dimensions =
    deepGet(spec, 'dataSchema.parser.parseSpec.dimensionsSpec.dimensions') || EMPTY_ARRAY;
  return Array.isArray(dimensions) && dimensions.length === 0 ? 'auto-detect' : 'specific';
}

export function getRollup(spec: IngestionSpec): boolean {
  const specRollup = deepGet(spec, 'dataSchema.granularitySpec.rollup');
  return typeof specRollup === 'boolean' ? specRollup : true;
}

export function getSpecType(spec: Partial<IngestionSpec>): IngestionType | undefined {
  return (
    deepGet(spec, 'type') || deepGet(spec, 'ioConfig.type') || deepGet(spec, 'tuningConfig.type')
  );
}

export function isTask(spec: IngestionSpec) {
  const type = String(getSpecType(spec));
  return (
    type.startsWith('index_') ||
    ['index', 'compact', 'kill', 'append', 'merge', 'same_interval_merge'].includes(type)
  );
}

export function isIngestSegment(spec: IngestionSpec): boolean {
  return deepGet(spec, 'ioConfig.firehose.type') === 'ingestSegment';
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

/**
 * Make sure that the types are set in the root, ioConfig, and tuningConfig
 * @param spec
 */
export function normalizeSpec(spec: Partial<IngestionSpec>): IngestionSpec {
  if (!spec || typeof spec !== 'object') {
    // This does not match the type of IngestionSpec but this dialog is robust enough to deal with anything but spec must be an object
    spec = {};
  }

  // Make sure that if we actually get a task payload we extract the spec
  if (typeof (spec as any).spec === 'object') spec = (spec as any).spec;

  const specType = getSpecType(spec);
  if (!specType) return spec as IngestionSpec;
  if (!deepGet(spec, 'type')) spec = deepSet(spec, 'type', specType);
  if (!deepGet(spec, 'ioConfig.type')) spec = deepSet(spec, 'ioConfig.type', specType);
  if (!deepGet(spec, 'tuningConfig.type')) spec = deepSet(spec, 'tuningConfig.type', specType);
  return spec as IngestionSpec;
}

const PARSE_SPEC_FORM_FIELDS: Field<ParseSpec>[] = [
  {
    name: 'format',
    label: 'Parser to use',
    type: 'string',
    suggestions: ['json', 'csv', 'tsv', 'regex'],
    info: (
      <>
        <p>The parser used to parse the data.</p>
        <p>
          For more information see{' '}
          <ExternalLink href="https://druid.apache.org/docs/latest/ingestion/data-formats.html">
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
    defined: (p: ParseSpec) => p.format === 'regex',
  },
  {
    name: 'function',
    type: 'string',
    required: true,
    defined: (p: ParseSpec) => p.format === 'javascript',
  },
  {
    name: 'hasHeaderRow',
    type: 'boolean',
    defaultValue: false,
    defined: (p: ParseSpec) => p.format === 'csv' || p.format === 'tsv',
  },
  {
    name: 'skipHeaderRows',
    type: 'number',
    defaultValue: 0,
    defined: (p: ParseSpec) => p.format === 'csv' || p.format === 'tsv',
    min: 0,
    info: (
      <>
        If both skipHeaderRows and hasHeaderRow options are set, skipHeaderRows is first applied.
        For example, if you set skipHeaderRows to 2 and hasHeaderRow to true, Druid will skip the
        first two lines and then extract column information from the third line.
      </>
    ),
  },
  {
    name: 'columns',
    type: 'string-array',
    defined: (p: ParseSpec) =>
      ((p.format === 'csv' || p.format === 'tsv') && !p.hasHeaderRow) || p.format === 'regex',
  },
  {
    name: 'listDelimiter',
    type: 'string',
    defaultValue: '|',
    defined: (p: ParseSpec) => p.format === 'csv' || p.format === 'tsv',
  },
];

export function getParseSpecFormFields() {
  return PARSE_SPEC_FORM_FIELDS;
}

export function issueWithParser(parser: Parser | undefined): string | undefined {
  if (!parser) return 'no parser';
  if (parser.type === 'map') return;

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
  return;
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
  missingValue: '2010-01-01T00:00:00Z',
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
    defaultValue: 'timestamp',
  },
  {
    name: 'format',
    type: 'string',
    defaultValue: 'auto',
    suggestions: [
      ...BASIC_TIME_FORMATS,
      {
        group: 'Date and time formats',
        suggestions: DATETIME_TIME_FORMATS,
      },
      {
        group: 'Date only formats',
        suggestions: DATE_ONLY_TIME_FORMATS,
      },
      {
        group: 'Other time formats',
        suggestions: OTHER_TIME_FORMATS,
      },
    ],
    defined: (timestampSpec: TimestampSpec) => isColumnTimestampSpec(timestampSpec),
    info: (
      <p>
        Please specify your timestamp format by using the suggestions menu or typing in a{' '}
        <ExternalLink href="https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html">
          format string
        </ExternalLink>
        .
      </p>
    ),
  },
  {
    name: 'missingValue',
    type: 'string',
    placeholder: '(optional)',
    info: <p>This value will be used if the specified column can not be found.</p>,
  },
];

const CONSTANT_TIMESTAMP_SPEC_FORM_FIELDS: Field<TimestampSpec>[] = [
  {
    name: 'missingValue',
    label: 'Constant value',
    type: 'string',
    info: <p>The dummy value that will be used as the timestamp.</p>,
  },
];

export function getTimestampSpecFormFields(timestampSpec: TimestampSpec) {
  if (isColumnTimestampSpec(timestampSpec)) {
    return TIMESTAMP_SPEC_FORM_FIELDS;
  } else {
    return CONSTANT_TIMESTAMP_SPEC_FORM_FIELDS;
  }
}

export function issueWithTimestampSpec(
  timestampSpec: TimestampSpec | undefined,
): string | undefined {
  if (!timestampSpec) return 'no spec';
  if (!timestampSpec.column && !timestampSpec.missingValue) return 'timestamp spec is blank';
  return;
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
    type: 'string',
  },
  {
    name: 'type',
    type: 'string',
    suggestions: ['string', 'long', 'float'],
  },
  {
    name: 'createBitmapIndex',
    type: 'boolean',
    defaultValue: true,
    defined: (dimensionSpec: DimensionSpec) => dimensionSpec.type === 'string',
  },
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
  return typeof dimensionSpec === 'string'
    ? { name: dimensionSpec, type: 'string' }
    : dimensionSpec;
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
    placeholder: 'column_name',
    required: true,
  },
  {
    name: 'type',
    type: 'string',
    suggestions: ['path', 'jq', 'root'],
    required: true,
  },
  {
    name: 'expr',
    type: 'string',
    placeholder: '$.thing',
    defined: (flattenField: FlattenField) =>
      flattenField.type === 'path' || flattenField.type === 'jq',
    required: true,
    info: (
      <>
        Specify a flatten{' '}
        <ExternalLink href="https://druid.apache.org/docs/latest/ingestion/flatten-json">
          expression
        </ExternalLink>
        .
      </>
    ),
  },
];

export function getFlattenFieldFormFields() {
  return FLATTEN_FIELD_FORM_FIELDS;
}

export interface TransformSpec {
  transforms?: Transform[];
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
    placeholder: 'output_name',
    required: true,
  },
  {
    name: 'type',
    type: 'string',
    suggestions: ['expression'],
    required: true,
  },
  {
    name: 'expression',
    type: 'string',
    placeholder: '"foo" + "bar"',
    required: true,
    info: (
      <>
        A valid Druid{' '}
        <ExternalLink href="https://druid.apache.org/docs/latest/misc/math-expr.html">
          expression
        </ExternalLink>
        .
      </>
    ),
  },
];

export function getTransformFormFields() {
  return TRANSFORM_FORM_FIELDS;
}

export interface GranularitySpec {
  type?: string;
  queryGranularity?: string;
  segmentGranularity?: string;
  rollup?: boolean;
  intervals?: string | string[];
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
    type: 'string',
  },
  {
    name: 'type',
    type: 'string',
    suggestions: [
      'count',
      {
        group: 'sum',
        suggestions: ['longSum', 'doubleSum', 'floatSum'],
      },
      {
        group: 'min',
        suggestions: ['longMin', 'doubleMin', 'floatMin'],
      },
      {
        group: 'max',
        suggestions: ['longMax', 'doubleMax', 'floatMax'],
      },
      {
        group: 'first',
        suggestions: ['longFirst', 'doubleFirst', 'floatFirst'],
      },
      {
        group: 'last',
        suggestions: ['longLast', 'doubleLast', 'floatLast'],
      },
      'cardinality',
      'hyperUnique',
      'filtered',
    ],
  },
  {
    name: 'fieldName',
    type: 'string',
    defined: m => {
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
        'hyperUnique',
      ].includes(m.type);
    },
  },
  {
    name: 'maxStringBytes',
    type: 'number',
    defaultValue: 1024,
    defined: m => {
      return ['stringFirst', 'stringLast'].includes(m.type);
    },
  },
  {
    name: 'filterNullValues',
    type: 'boolean',
    defaultValue: false,
    defined: m => {
      return ['stringFirst', 'stringLast'].includes(m.type);
    },
  },
  {
    name: 'filter',
    type: 'json',
    defined: m => {
      return m.type === 'filtered';
    },
  },
  {
    name: 'aggregator',
    type: 'json',
    defined: m => {
      return m.type === 'filtered';
    },
  },
  // ToDo: fill in approximates
];

export function getMetricSpecFormFields() {
  return METRIC_SPEC_FORM_FIELDS;
}

export function getMetricSpecName(metricSpec: MetricSpec): string {
  return (
    metricSpec.name || (metricSpec.aggregator ? getMetricSpecName(metricSpec.aggregator) : '?')
  );
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
  endpoint?: string;
  useEarliestSequenceNumber?: boolean;
}

export function invalidIoConfig(ioConfig: IoConfig): boolean {
  return (
    (ioConfig.type === 'kafka' && ioConfig.useEarliestOffset == null) ||
    (ioConfig.type === 'kinesis' && ioConfig.useEarliestSequenceNumber == null)
  );
}

export interface Firehose {
  type: string;
  baseDir?: string;
  filter?: any;
  uris?: string[];
  prefixes?: string[];
  blobs?: { bucket: string; path: string }[];
  fetchTimeout?: number;

  // ingestSegment
  dataSource?: string;
  interval?: string;
  dimensions?: string[];
  metrics?: string[];
  maxInputSegmentBytesPerTask?: number;

  // inline
  data?: string;
}

export function getIoConfigFormFields(ingestionComboType: IngestionComboType): Field<IoConfig>[] {
  const firehoseType: Field<IoConfig> = {
    name: 'firehose.type',
    label: 'Firehose type',
    type: 'string',
    suggestions: ['local', 'http', 'inline', 'static-s3', 'static-google-blobstore'],
    info: (
      <p>
        Druid connects to raw data through{' '}
        <ExternalLink href="https://druid.apache.org/docs/latest/ingestion/firehose.html">
          firehoses
        </ExternalLink>
        . You can change your selected firehose here.
      </p>
    ),
  };

  switch (ingestionComboType) {
    case 'index:http':
      return [
        firehoseType,
        {
          name: 'firehose.uris',
          label: 'URIs',
          type: 'string-array',
          placeholder:
            'https://example.com/path/to/file1.ext, https://example.com/path/to/file2.ext',
          required: true,
          info: (
            <p>
              The full URI of your file. To ingest from multiple URIs, use commas to separate each
              individual URI.
            </p>
          ),
        },
        {
          name: 'firehose.httpAuthenticationUsername',
          label: 'HTTP auth username',
          type: 'string',
          placeholder: '(optional)',
          info: <p>Username to use for authentication with specified URIs</p>,
        },
        {
          name: 'firehose.httpAuthenticationPassword',
          label: 'HTTP auth password',
          type: 'string',
          placeholder: '(optional)',
          info: <p>Password to use for authentication with specified URIs</p>,
        },
      ];

    case 'index:local':
      return [
        firehoseType,
        {
          name: 'firehose.baseDir',
          label: 'Base directory',
          type: 'string',
          placeholder: '/path/to/files/',
          required: true,
          info: (
            <>
              <ExternalLink href="https://druid.apache.org/docs/latest/ingestion/firehose.html#localfirehose">
                firehose.baseDir
              </ExternalLink>
              <p>Specifies the directory to search recursively for files to be ingested.</p>
            </>
          ),
        },
        {
          name: 'firehose.filter',
          label: 'File filter',
          type: 'string',
          required: true,
          suggestions: ['*', '*.json', '*.json.gz', '*.csv', '*.tsv'],
          info: (
            <>
              <ExternalLink href="https://druid.apache.org/docs/latest/ingestion/firehose.html#localfirehose">
                firehose.filter
              </ExternalLink>
              <p>
                A wildcard filter for files. See{' '}
                <ExternalLink href="https://commons.apache.org/proper/commons-io/apidocs/org/apache/commons/io/filefilter/WildcardFileFilter.html">
                  here
                </ExternalLink>{' '}
                for format information.
              </p>
            </>
          ),
        },
      ];

    case 'index:ingestSegment':
      return [
        firehoseType,
        {
          name: 'firehose.dataSource',
          label: 'Datasource',
          type: 'string',
          required: true,
          info: <p>The datasource to fetch rows from.</p>,
        },
        {
          name: 'firehose.interval',
          label: 'Interval',
          type: 'string',
          placeholder: `${CURRENT_YEAR}-01-01/${CURRENT_YEAR + 1}-01-01`,
          suggestions: [
            `${CURRENT_YEAR}-01-01T00:00:00/${CURRENT_YEAR + 1}-01-01T00:00:00`,
            `${CURRENT_YEAR}-01-01/${CURRENT_YEAR + 1}-01-01`,
            `${CURRENT_YEAR}/${CURRENT_YEAR + 1}`,
          ],
          required: true,
          info: (
            <p>
              A String representing ISO-8601 Interval. This defines the time range to fetch the data
              over.
            </p>
          ),
        },
        {
          name: 'firehose.dimensions',
          label: 'Dimensions',
          type: 'string-array',
          placeholder: '(optional)',
          info: (
            <p>
              The list of dimensions to select. If left empty, no dimensions are returned. If left
              null or not defined, all dimensions are returned.
            </p>
          ),
        },
        {
          name: 'firehose.metrics',
          label: 'Metrics',
          type: 'string-array',
          placeholder: '(optional)',
          info: (
            <p>
              The list of metrics to select. If left empty, no metrics are returned. If left null or
              not defined, all metrics are selected.
            </p>
          ),
        },
        {
          name: 'firehose.filter',
          label: 'Filter',
          type: 'json',
          placeholder: '(optional)',
          info: (
            <p>
              The{' '}
              <ExternalLink href="https://druid.apache.org/docs/latest/querying/filters.html">
                filter
              </ExternalLink>{' '}
              to apply to the data as part of querying.
            </p>
          ),
        },
      ];

    case 'index:inline':
      return [
        firehoseType,
        // do not add 'data' here as it has special handling in the load-data view
      ];

    case 'index:static-s3':
      return [
        firehoseType,
        {
          name: 'firehose.uris',
          label: 'S3 URIs',
          type: 'string-array',
          placeholder: 's3://your-bucket/some-file1.ext, s3://your-bucket/some-file2.ext',
          required: true,
          defined: ioConfig => !deepGet(ioConfig, 'firehose.prefixes'),
          info: (
            <>
              <p>
                The full S3 URI of your file. To ingest from multiple URIs, use commas to separate
                each individual URI.
              </p>
              <p>Either S3 URIs or S3 prefixes must be set.</p>
            </>
          ),
        },
        {
          name: 'firehose.prefixes',
          label: 'S3 prefixes',
          type: 'string-array',
          placeholder: 's3://your-bucket/some-path1, s3://your-bucket/some-path2',
          required: true,
          defined: ioConfig => !deepGet(ioConfig, 'firehose.uris'),
          info: (
            <>
              <p>A list of paths (with bucket) where your files are stored.</p>
              <p>Either S3 URIs or S3 prefixes must be set.</p>
            </>
          ),
        },
      ];

    case 'index:static-google-blobstore':
      return [
        firehoseType,
        {
          name: 'firehose.blobs',
          label: 'Google blobs',
          type: 'json',
          required: true,
          info: (
            <>
              <p>
                JSON array of{' '}
                <ExternalLink href="https://druid.apache.org/docs/latest/development/extensions-contrib/google.html">
                  Google Blobs
                </ExternalLink>
                .
              </p>
            </>
          ),
        },
      ];

    case 'kafka':
      return [
        {
          name: 'consumerProperties.{bootstrap.servers}',
          label: 'Bootstrap servers',
          type: 'string',
          required: true,
          info: (
            <>
              <ExternalLink href="https://druid.apache.org/docs/latest/development/extensions-core/kafka-ingestion#kafkasupervisorioconfig">
                consumerProperties
              </ExternalLink>
              <p>
                A list of Kafka brokers in the form:{' '}
                <Code>{`<BROKER_1>:<PORT_1>,<BROKER_2>:<PORT_2>,...`}</Code>
              </p>
            </>
          ),
        },
        {
          name: 'topic',
          type: 'string',
          required: true,
          defined: (i: IoConfig) => i.type === 'kafka',
        },
        {
          name: 'consumerProperties',
          type: 'json',
          defaultValue: {},
          info: (
            <>
              <ExternalLink href="https://druid.apache.org/docs/latest/development/extensions-core/kafka-ingestion#kafkasupervisorioconfig">
                consumerProperties
              </ExternalLink>
              <p>A map of properties to be passed to the Kafka consumer.</p>
            </>
          ),
        },
      ];

    case 'kinesis':
      return [
        {
          name: 'stream',
          type: 'string',
          placeholder: 'your-kinesis-stream',
          required: true,
          info: <>The Kinesis stream to read.</>,
        },
        {
          name: 'endpoint',
          type: 'string',
          defaultValue: 'kinesis.us-east-1.amazonaws.com',
          suggestions: [
            'kinesis.us-east-2.amazonaws.com',
            'kinesis.us-east-1.amazonaws.com',
            'kinesis.us-west-1.amazonaws.com',
            'kinesis.us-west-2.amazonaws.com',
            'kinesis.ap-east-1.amazonaws.com',
            'kinesis.ap-south-1.amazonaws.com',
            'kinesis.ap-northeast-3.amazonaws.com',
            'kinesis.ap-northeast-2.amazonaws.com',
            'kinesis.ap-southeast-1.amazonaws.com',
            'kinesis.ap-southeast-2.amazonaws.com',
            'kinesis.ap-northeast-1.amazonaws.com',
            'kinesis.ca-central-1.amazonaws.com',
            'kinesis.cn-north-1.amazonaws.com.com',
            'kinesis.cn-northwest-1.amazonaws.com.com',
            'kinesis.eu-central-1.amazonaws.com',
            'kinesis.eu-west-1.amazonaws.com',
            'kinesis.eu-west-2.amazonaws.com',
            'kinesis.eu-west-3.amazonaws.com',
            'kinesis.eu-north-1.amazonaws.com',
            'kinesis.sa-east-1.amazonaws.com',
            'kinesis.us-gov-east-1.amazonaws.com',
            'kinesis.us-gov-west-1.amazonaws.com',
          ],
          required: true,
          info: (
            <>
              The Amazon Kinesis stream endpoint for a region. You can find a list of endpoints{' '}
              <ExternalLink href="http://docs.aws.amazon.com/general/latest/gr/rande.html#ak_region">
                here
              </ExternalLink>
              .
            </>
          ),
        },
        {
          name: 'awsAssumedRoleArn',
          label: 'AWS assumed role ARN',
          type: 'string',
          placeholder: 'optional',
          info: <>The AWS assumed role to use for additional permissions.</>,
        },
        {
          name: 'awsExternalId',
          label: 'AWS external ID',
          type: 'string',
          placeholder: 'optional',
          info: <>The AWS external id to use for additional permissions.</>,
        },
      ];
  }

  throw new Error(`unknown input type ${ingestionComboType}`);
}

function nonEmptyArray(a: any) {
  return Array.isArray(a) && Boolean(a.length);
}

function issueWithFirehose(firehose: Firehose | undefined): string | undefined {
  if (!firehose) return 'does not exist';
  if (!firehose.type) return 'missing a type';
  switch (firehose.type) {
    case 'local':
      if (!firehose.baseDir) return `must have a 'baseDir'`;
      if (!firehose.filter) return `must have a 'filter'`;
      break;

    case 'http':
      if (!nonEmptyArray(firehose.uris)) {
        return 'must have at least one uri';
      }
      break;

    case 'ingestSegment':
      if (!firehose.dataSource) return `must have a 'dataSource'`;
      if (!firehose.interval) return `must have an 'interval'`;
      break;

    case 'inline':
      if (!firehose.data) return `must have 'data'`;
      break;

    case 'static-s3':
      if (!nonEmptyArray(firehose.uris) && !nonEmptyArray(firehose.prefixes)) {
        return 'must have at least one uri or prefix';
      }
      break;

    case 'static-google-blobstore':
      if (!nonEmptyArray(firehose.blobs)) {
        return 'must have at least one blob';
      }
      break;
  }
  return;
}

export function issueWithIoConfig(ioConfig: IoConfig | undefined): string | undefined {
  if (!ioConfig) return 'does not exist';
  if (!ioConfig.type) return 'missing a type';
  switch (ioConfig.type) {
    case 'index':
    case 'index_parallel':
      if (issueWithFirehose(ioConfig.firehose)) {
        return `firehose: '${issueWithFirehose(ioConfig.firehose)}'`;
      }
      break;

    case 'kafka':
      if (!ioConfig.topic) return 'must have a topic';
      break;

    case 'kinesis':
      if (!ioConfig.stream) return 'must have a stream';
      break;
  }

  return;
}

export function getIoConfigTuningFormFields(
  ingestionComboType: IngestionComboType,
): Field<IoConfig>[] {
  switch (ingestionComboType) {
    case 'index:http':
    case 'index:static-s3':
    case 'index:static-google-blobstore':
      return [
        {
          name: 'firehose.fetchTimeout',
          label: 'Fetch timeout',
          type: 'number',
          defaultValue: 60000,
          info: (
            <>
              <p>Timeout for fetching the object.</p>
            </>
          ),
        },
        {
          name: 'firehose.maxFetchRetry',
          label: 'Max fetch retry',
          type: 'number',
          defaultValue: 3,
          info: (
            <>
              <p>Maximum retry for fetching the object.</p>
            </>
          ),
        },
        {
          name: 'firehose.maxCacheCapacityBytes',
          label: 'Max cache capacity bytes',
          type: 'number',
          defaultValue: 1073741824,
          info: (
            <>
              <p>
                Maximum size of the cache space in bytes. 0 means disabling cache. Cached files are
                not removed until the ingestion task completes.
              </p>
            </>
          ),
        },
        {
          name: 'firehose.maxFetchCapacityBytes',
          label: 'Max fetch capacity bytes',
          type: 'number',
          defaultValue: 1073741824,
          info: (
            <>
              <p>
                Maximum size of the fetch space in bytes. 0 means disabling prefetch. Prefetched
                files are removed immediately once they are read.
              </p>
            </>
          ),
        },
        {
          name: 'firehose.prefetchTriggerBytes',
          label: 'Prefetch trigger bytes',
          type: 'number',
          placeholder: 'maxFetchCapacityBytes / 2',
          info: (
            <>
              <p>Threshold to trigger prefetching the objects.</p>
            </>
          ),
        },
      ];

    case 'index:local':
    case 'index:inline':
      return [];

    case 'index:ingestSegment':
      return [
        {
          name: 'firehose.maxFetchCapacityBytes',
          label: 'Max fetch capacity bytes',
          type: 'number',
          defaultValue: 157286400,
          info: (
            <p>
              When used with the native parallel index task, the maximum number of bytes of input
              segments to process in a single task. If a single segment is larger than this number,
              it will be processed by itself in a single task (input segments are never split across
              tasks). Defaults to 150MB.
            </p>
          ),
        },
      ];

    case 'kafka':
    case 'kinesis':
      return [
        {
          name: 'useEarliestOffset',
          type: 'boolean',
          defined: (i: IoConfig) => i.type === 'kafka',
          required: true,
          info: (
            <>
              <p>
                If a supervisor is managing a dataSource for the first time, it will obtain a set of
                starting offsets from Kafka. This flag determines whether it retrieves the earliest
                or latest offsets in Kafka. Under normal circumstances, subsequent tasks will start
                from where the previous segments ended so this flag will only be used on first run.
              </p>
            </>
          ),
        },
        {
          name: 'useEarliestSequenceNumber',
          type: 'boolean',
          defined: (i: IoConfig) => i.type === 'kinesis',
          required: true,
          info: (
            <>
              If a supervisor is managing a dataSource for the first time, it will obtain a set of
              starting sequence numbers from Kinesis. This flag determines whether it retrieves the
              earliest or latest sequence numbers in Kinesis. Under normal circumstances, subsequent
              tasks will start from where the previous segments ended so this flag will only be used
              on first run.
            </>
          ),
        },
        {
          name: 'taskDuration',
          type: 'duration',
          defaultValue: 'PT1H',
          info: (
            <>
              <p>
                The length of time before tasks stop reading and begin publishing their segment.
              </p>
            </>
          ),
        },
        {
          name: 'taskCount',
          type: 'number',
          defaultValue: 1,
          info: (
            <>
              <p>
                The maximum number of reading tasks in a replica set. This means that the maximum
                number of reading tasks will be <Code>taskCount * replicas</Code> and the total
                number of tasks (reading + publishing) will be higher than this. See 'Capacity
                Planning' below for more details.
              </p>
            </>
          ),
        },
        {
          name: 'replicas',
          type: 'number',
          defaultValue: 1,
          info: (
            <>
              <p>
                The number of replica sets, where 1 means a single set of tasks (no replication).
                Replica tasks will always be assigned to different workers to provide resiliency
                against process failure.
              </p>
            </>
          ),
        },
        {
          name: 'completionTimeout',
          type: 'duration',
          defaultValue: 'PT30M',
          info: (
            <>
              <p>
                The length of time to wait before declaring a publishing task as failed and
                terminating it. If this is set too low, your tasks may never publish. The publishing
                clock for a task begins roughly after taskDuration elapses.
              </p>
            </>
          ),
        },
        {
          name: 'recordsPerFetch',
          type: 'number',
          defaultValue: 2000,
          defined: (i: IoConfig) => i.type === 'kinesis',
          info: <>The number of records to request per GetRecords call to Kinesis.</>,
        },
        {
          name: 'pollTimeout',
          type: 'number',
          defaultValue: 100,
          defined: (i: IoConfig) => i.type === 'kafka',
          info: (
            <>
              <p>
                The length of time to wait for the kafka consumer to poll records, in milliseconds.
              </p>
            </>
          ),
        },
        {
          name: 'fetchDelayMillis',
          type: 'number',
          defaultValue: 1000,
          defined: (i: IoConfig) => i.type === 'kinesis',
          info: <>Time in milliseconds to wait between subsequent GetRecords calls to Kinesis.</>,
        },
        {
          name: 'deaggregate',
          type: 'boolean',
          defaultValue: false,
          defined: (i: IoConfig) => i.type === 'kinesis',
          info: <>Whether to use the de-aggregate function of the KCL.</>,
        },
        {
          name: 'startDelay',
          type: 'duration',
          defaultValue: 'PT5S',
          info: (
            <>
              <p>The period to wait before the supervisor starts managing tasks.</p>
            </>
          ),
        },
        {
          name: 'period',
          label: 'Management period',
          type: 'duration',
          defaultValue: 'PT30S',
          info: (
            <>
              <p>How often the supervisor will execute its management logic.</p>
              <p>
                Note that the supervisor will also run in response to certain events (such as tasks
                succeeding, failing, and reaching their taskDuration) so this value specifies the
                maximum time between iterations.
              </p>
            </>
          ),
        },
        {
          name: 'lateMessageRejectionPeriod',
          type: 'string',
          placeholder: '(none)',
          info: (
            <>
              <p>
                Configure tasks to reject messages with timestamps earlier than this period before
                the task was created; for example if this is set to PT1H and the supervisor creates
                a task at 2016-01-01T12:00Z, messages with timestamps earlier than 2016-01-01T11:00Z
                will be dropped.
              </p>
              <p>
                This may help prevent concurrency issues if your data stream has late messages and
                you have multiple pipelines that need to operate on the same segments (e.g. a
                realtime and a nightly batch ingestion pipeline).
              </p>
            </>
          ),
        },
        {
          name: 'earlyMessageRejectionPeriod',
          type: 'string',
          placeholder: '(none)',
          info: (
            <>
              <p>
                Configure tasks to reject messages with timestamps later than this period after the
                task reached its taskDuration; for example if this is set to PT1H, the taskDuration
                is set to PT1H and the supervisor creates a task at 2016-01-01T12:00Z, messages with
                timestamps later than 2016-01-01T14:00Z will be dropped.
              </p>
            </>
          ),
        },
        {
          name: 'skipOffsetGaps',
          type: 'boolean',
          defaultValue: false,
          defined: (i: IoConfig) => i.type === 'kafka',
          info: (
            <>
              <p>
                Whether or not to allow gaps of missing offsets in the Kafka stream. This is
                required for compatibility with implementations such as MapR Streams which does not
                guarantee consecutive offsets. If this is false, an exception will be thrown if
                offsets are not consecutive.
              </p>
            </>
          ),
        },
      ];
  }

  throw new Error(`unknown ingestion combo type ${ingestionComboType}`);
}

// ---------------------------------------

function filterIsFilename(filter: string): boolean {
  return !/[*?]/.test(filter);
}

function filenameFromPath(path: string): string | undefined {
  const m = path.match(/([^\/.]+)[^\/]*?\/?$/);
  if (!m) return;
  return m[1];
}

function basenameFromFilename(filename: string): string | undefined {
  return filename.split('.')[0];
}

export function fillDataSourceNameIfNeeded(spec: IngestionSpec): IngestionSpec {
  const possibleName = guessDataSourceName(spec);
  if (!possibleName) return spec;
  return deepSet(spec, 'dataSchema.dataSource', possibleName);
}

export function guessDataSourceName(spec: IngestionSpec): string | undefined {
  const ioConfig = deepGet(spec, 'ioConfig');
  if (!ioConfig) return;

  switch (ioConfig.type) {
    case 'index':
    case 'index_parallel':
      const firehose = ioConfig.firehose;
      if (!firehose) return;

      switch (firehose.type) {
        case 'local':
          if (firehose.filter && filterIsFilename(firehose.filter)) {
            return basenameFromFilename(firehose.filter);
          } else if (firehose.baseDir) {
            return filenameFromPath(firehose.baseDir);
          } else {
            return;
          }

        case 'static-s3':
          const s3Path = (firehose.uris || EMPTY_ARRAY)[0] || (firehose.prefixes || EMPTY_ARRAY)[0];
          return s3Path ? filenameFromPath(s3Path) : undefined;

        case 'http':
          return Array.isArray(firehose.uris) ? filenameFromPath(firehose.uris[0]) : undefined;

        case 'ingestSegment':
          return firehose.dataSource;

        case 'inline':
          return 'inline_data';
      }

      return;

    case 'kafka':
      return ioConfig.topic;

    case 'kinesis':
      return ioConfig.stream;

    default:
      return;
  }
}

// --------------

export interface TuningConfig {
  type: string;
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
  intermediateHandoffPeriod?: string;
  handoffConditionTimeout?: number;
  resetOffsetAutomatically?: boolean;
  workerThreads?: number;
  chatThreads?: number;
  chatRetries?: number;
  httpTimeout?: string;
  shutdownTimeout?: string;
  offsetFetchPeriod?: string;
  maxParseExceptions?: number;
  maxSavedParseExceptions?: number;
  recordBufferSize?: number;
  recordBufferOfferTimeout?: number;
  recordBufferFullWait?: number;
  fetchSequenceNumberTimeout?: number;
  fetchThreads?: number;
}

export function invalidTuningConfig(tuningConfig: TuningConfig, intervals: any): boolean {
  return Boolean(
    tuningConfig.type === 'index_parallel' &&
      tuningConfig.forceGuaranteedRollup &&
      (!tuningConfig.numShards || !intervals),
  );
}

export function getPartitionRelatedTuningSpecFormFields(
  specType: IngestionType,
): Field<TuningConfig>[] {
  switch (specType) {
    case 'index':
    case 'index_parallel':
      return [
        {
          name: 'forceGuaranteedRollup',
          type: 'boolean',
          defaultValue: false,
          info: (
            <>
              <p>
                Forces guaranteeing the perfect rollup. The perfect rollup optimizes the total size
                of generated segments and querying time while indexing time will be increased. If
                this is set to true, the index task will read the entire input data twice: one for
                finding the optimal number of partitions per time chunk and one for generating
                segments.
              </p>
            </>
          ),
        },
        {
          name: 'numShards', // This is mandatory if index_parallel and forceGuaranteedRollup
          type: 'number',
          defined: (t: TuningConfig) => Boolean(t.forceGuaranteedRollup),
          required: (t: TuningConfig) =>
            Boolean(t.type === 'index_parallel' && t.forceGuaranteedRollup),
          info: (
            <>
              Directly specify the number of shards to create. If this is specified and 'intervals'
              is specified in the granularitySpec, the index task can skip the determine
              intervals/partitions pass through the data. numShards cannot be specified if
              maxRowsPerSegment is set.
            </>
          ),
        },
        {
          name: 'partitionDimensions',
          type: 'string-array',
          defined: (t: TuningConfig) => Boolean(t.forceGuaranteedRollup),
          info: (
            <>
              <p>Does not currently work with parallel ingestion</p>
              <p>
                The dimensions to partition on. Leave blank to select all dimensions. Only used with
                forceGuaranteedRollup = true, will be ignored otherwise.
              </p>
            </>
          ),
        },
        {
          name: 'maxRowsPerSegment',
          type: 'number',
          defaultValue: 5000000,
          defined: (t: TuningConfig) => !t.forceGuaranteedRollup && t.numShards == null, // Can not be set if numShards is specified
          info: <>Determines how many rows are in each segment.</>,
        },
        {
          name: 'maxTotalRows',
          type: 'number',
          defaultValue: 20000000,
          defined: (t: TuningConfig) => !t.forceGuaranteedRollup,
          info: <>Total number of rows in segments waiting for being pushed.</>,
        },
      ];

    case 'kafka':
    case 'kinesis':
      return [
        {
          name: 'maxRowsPerSegment',
          type: 'number',
          defaultValue: 5000000,
          info: <>Determines how many rows are in each segment.</>,
        },
        {
          name: 'maxTotalRows',
          type: 'number',
          defaultValue: 20000000,
          info: <>Total number of rows in segments waiting for being pushed.</>,
        },
      ];
  }

  throw new Error(`unknown spec type ${specType}`);
}

const TUNING_CONFIG_FORM_FIELDS: Field<TuningConfig>[] = [
  {
    name: 'maxNumConcurrentSubTasks',
    type: 'number',
    defaultValue: 1,
    defined: (t: TuningConfig) => t.type === 'index_parallel',
    info: (
      <>
        Maximum number of tasks which can be run at the same time. The supervisor task would spawn
        worker tasks up to maxNumConcurrentSubTasks regardless of the available task slots. If this
        value is set to 1, the supervisor task processes data ingestion on its own instead of
        spawning worker tasks. If this value is set to too large, too many worker tasks can be
        created which might block other ingestion.
      </>
    ),
  },
  {
    name: 'maxRetry',
    type: 'number',
    defaultValue: 3,
    defined: (t: TuningConfig) => t.type === 'index_parallel',
    info: <>Maximum number of retries on task failures.</>,
  },
  {
    name: 'taskStatusCheckPeriodMs',
    type: 'number',
    defaultValue: 1000,
    defined: (t: TuningConfig) => t.type === 'index_parallel',
    info: <>Polling period in milliseconds to check running task statuses.</>,
  },
  {
    name: 'maxRowsInMemory',
    type: 'number',
    defaultValue: 1000000,
    info: <>Used in determining when intermediate persists to disk should occur.</>,
  },
  {
    name: 'maxBytesInMemory',
    type: 'number',
    placeholder: 'Default: 1/6 of max JVM memory',
    info: <>Used in determining when intermediate persists to disk should occur.</>,
  },
  {
    name: 'maxNumMergeTasks',
    type: 'number',
    defaultValue: 10,
    defined: (t: TuningConfig) => Boolean(t.type === 'index_parallel' && t.forceGuaranteedRollup),
    info: <>Number of tasks to merge partial segments after shuffle.</>,
  },
  {
    name: 'maxNumSegmentsToMerge',
    type: 'number',
    defaultValue: 100,
    defined: (t: TuningConfig) => Boolean(t.type === 'index_parallel' && t.forceGuaranteedRollup),
    info: (
      <>
        Max limit for the number of segments a single task can merge at the same time after shuffle.
      </>
    ),
  },
  {
    name: 'resetOffsetAutomatically',
    type: 'boolean',
    defaultValue: false,
    defined: (t: TuningConfig) => t.type === 'kafka' || t.type === 'kinesis',
    info: (
      <>
        Whether to reset the consumer offset if the next offset that it is trying to fetch is less
        than the earliest available offset for that particular partition.
      </>
    ),
  },
  {
    name: 'intermediatePersistPeriod',
    type: 'duration',
    defaultValue: 'PT10M',
    defined: (t: TuningConfig) => t.type === 'kafka' || t.type === 'kinesis',
    info: <>The period that determines the rate at which intermediate persists occur.</>,
  },
  {
    name: 'intermediateHandoffPeriod',
    type: 'duration',
    defaultValue: 'P2147483647D',
    defined: (t: TuningConfig) => t.type === 'kafka' || t.type === 'kinesis',
    info: (
      <>
        How often the tasks should hand off segments. Handoff will happen either if
        maxRowsPerSegment or maxTotalRows is hit or every intermediateHandoffPeriod, whichever
        happens earlier.
      </>
    ),
  },
  {
    name: 'maxPendingPersists',
    type: 'number',
    info: (
      <>
        Maximum number of persists that can be pending but not started. If this limit would be
        exceeded by a new intermediate persist, ingestion will block until the currently-running
        persist finishes.
      </>
    ),
  },
  {
    name: 'pushTimeout',
    type: 'number',
    defaultValue: 0,
    info: (
      <>
        Milliseconds to wait for pushing segments. It must be >= 0, where 0 means to wait forever.
      </>
    ),
  },
  {
    name: 'handoffConditionTimeout',
    type: 'number',
    defaultValue: 0,
    defined: (t: TuningConfig) => t.type === 'kafka' || t.type === 'kinesis',
    info: <>Milliseconds to wait for segment handoff. 0 means to wait forever.</>,
  },
  {
    name: 'indexSpec.bitmap.type',
    label: 'Index bitmap type',
    type: 'string',
    defaultValue: 'concise',
    suggestions: ['concise', 'roaring'],
    info: <>Compression format for bitmap indexes.</>,
  },
  {
    name: 'indexSpec.dimensionCompression',
    label: 'Index dimension compression',
    type: 'string',
    defaultValue: 'lz4',
    suggestions: ['lz4', 'lzf', 'uncompressed'],
    info: <>Compression format for dimension columns.</>,
  },
  {
    name: 'indexSpec.metricCompression',
    label: 'Index metric compression',
    type: 'string',
    defaultValue: 'lz4',
    suggestions: ['lz4', 'lzf', 'uncompressed'],
    info: <>Compression format for metric columns.</>,
  },
  {
    name: 'indexSpec.longEncoding',
    label: 'Index long encoding',
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
    name: 'chatHandlerTimeout',
    type: 'duration',
    defaultValue: 'PT10S',
    defined: (t: TuningConfig) => t.type === 'index_parallel',
    info: <>Timeout for reporting the pushed segments in worker tasks.</>,
  },
  {
    name: 'chatHandlerNumRetries',
    type: 'number',
    defaultValue: 5,
    defined: (t: TuningConfig) => t.type === 'index_parallel',
    info: <>Retries for reporting the pushed segments in worker tasks.</>,
  },
  {
    name: 'workerThreads',
    type: 'number',
    placeholder: 'min(10, taskCount)',
    defined: (t: TuningConfig) => t.type === 'kafka' || t.type === 'kinesis',
    info: (
      <>The number of threads that will be used by the supervisor for asynchronous operations.</>
    ),
  },
  {
    name: 'chatThreads',
    type: 'number',
    placeholder: 'min(10, taskCount * replicas)',
    defined: (t: TuningConfig) => t.type === 'kafka' || t.type === 'kinesis',
    info: <>The number of threads that will be used for communicating with indexing tasks.</>,
  },
  {
    name: 'chatRetries',
    type: 'number',
    defaultValue: 8,
    defined: (t: TuningConfig) => t.type === 'kafka' || t.type === 'kinesis',
    info: (
      <>
        The number of times HTTP requests to indexing tasks will be retried before considering tasks
        unresponsive.
      </>
    ),
  },
  {
    name: 'httpTimeout',
    type: 'duration',
    defaultValue: 'PT10S',
    defined: (t: TuningConfig) => t.type === 'kafka' || t.type === 'kinesis',
    info: <>How long to wait for a HTTP response from an indexing task.</>,
  },
  {
    name: 'shutdownTimeout',
    type: 'duration',
    defaultValue: 'PT80S',
    defined: (t: TuningConfig) => t.type === 'kafka' || t.type === 'kinesis',
    info: (
      <>
        How long to wait for the supervisor to attempt a graceful shutdown of tasks before exiting.
      </>
    ),
  },
  {
    name: 'offsetFetchPeriod',
    type: 'duration',
    defaultValue: 'PT30S',
    defined: (t: TuningConfig) => t.type === 'kafka',
    info: (
      <>
        How often the supervisor queries Kafka and the indexing tasks to fetch current offsets and
        calculate lag.
      </>
    ),
  },
  {
    name: 'recordBufferSize',
    type: 'number',
    defaultValue: 10000,
    defined: (t: TuningConfig) => t.type === 'kinesis',
    info: (
      <>
        Size of the buffer (number of events) used between the Kinesis fetch threads and the main
        ingestion thread.
      </>
    ),
  },
  {
    name: 'recordBufferOfferTimeout',
    type: 'number',
    defaultValue: 5000,
    defined: (t: TuningConfig) => t.type === 'kinesis',
    info: (
      <>
        Length of time in milliseconds to wait for space to become available in the buffer before
        timing out.
      </>
    ),
  },
  {
    name: 'recordBufferFullWait',
    type: 'number',
    defaultValue: 5000,
    defined: (t: TuningConfig) => t.type === 'kinesis',
    info: (
      <>
        Length of time in milliseconds to wait for the buffer to drain before attempting to fetch
        records from Kinesis again.
      </>
    ),
  },
  {
    name: 'fetchSequenceNumberTimeout',
    type: 'number',
    defaultValue: 60000,
    defined: (t: TuningConfig) => t.type === 'kinesis',
    info: (
      <>
        Length of time in milliseconds to wait for Kinesis to return the earliest or latest sequence
        number for a shard. Kinesis will not return the latest sequence number if no data is
        actively being written to that shard. In this case, this fetch call will repeatedly timeout
        and retry until fresh data is written to the stream.
      </>
    ),
  },
  {
    name: 'fetchThreads',
    type: 'number',
    placeholder: 'max(1, {numProcessors} - 1)',
    defined: (t: TuningConfig) => t.type === 'kinesis',
    info: (
      <>
        Size of the pool of threads fetching data from Kinesis. There is no benefit in having more
        threads than Kinesis shards.
      </>
    ),
  },
  {
    name: 'maxRecordsPerPoll',
    type: 'number',
    defaultValue: 100,
    defined: (t: TuningConfig) => t.type === 'kinesis',
    info: (
      <>
        The maximum number of records/events to be fetched from buffer per poll. The actual maximum
        will be <Code>max(maxRecordsPerPoll, max(bufferSize, 1))</Code>.
      </>
    ),
  },
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

export function updateIngestionType(
  spec: IngestionSpec,
  comboType: IngestionComboType,
): IngestionSpec {
  let [ingestionType, firehoseType] = comboType.split(':');
  if (ingestionType === 'index') ingestionType = 'index_parallel';
  const ioAndTuningConfigType = ingestionTypeToIoAndTuningConfigType(
    ingestionType as IngestionType,
  );

  let newSpec = spec;
  newSpec = deepSet(newSpec, 'type', ingestionType);
  newSpec = deepSet(newSpec, 'ioConfig.type', ioAndTuningConfigType);
  newSpec = deepSet(newSpec, 'tuningConfig.type', ioAndTuningConfigType);

  if (firehoseType) {
    newSpec = deepSet(newSpec, 'ioConfig.firehose', { type: firehoseType });

    if (firehoseType === 'local') {
      newSpec = deepSet(newSpec, 'ioConfig.firehose.filter', '*');
    }
  }

  if (!deepGet(spec, 'dataSchema.dataSource')) {
    newSpec = deepSet(newSpec, 'dataSchema.dataSource', 'new-data-source');
  }

  if (!deepGet(spec, 'dataSchema.granularitySpec')) {
    const granularitySpec: GranularitySpec = {
      type: 'uniform',
      queryGranularity: 'HOUR',
    };
    if (ingestionType !== 'index_parallel') {
      granularitySpec.segmentGranularity = 'HOUR';
    }

    newSpec = deepSet(newSpec, 'dataSchema.granularitySpec', granularitySpec);
  }

  return newSpec;
}

export function fillParser(spec: IngestionSpec, sampleData: string[]): IngestionSpec {
  const firehoseType = deepGet(spec, 'ioConfig.firehose.type');

  if (firehoseType === 'sql') {
    return deepSet(spec, 'dataSchema.parser', { type: 'map' });
  }

  if (firehoseType === 'ingestSegment') {
    return deepSet(spec, 'dataSchema.parser', {
      type: 'string',
      parseSpec: { format: 'timeAndDims' },
    });
  }

  const parseSpec = guessParseSpec(sampleData);
  if (!parseSpec) return spec;

  return deepSet(spec, 'dataSchema.parser', { type: 'string', parseSpec });
}

function guessParseSpec(sampleData: string[]): ParseSpec | undefined {
  const sampleDatum = sampleData[0];
  if (!sampleDatum) return;

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

function parseSpecFromFormat(format: string, hasHeaderRow?: boolean): ParseSpec {
  const parseSpec: ParseSpec = {
    format,
    timestampSpec: {},
    dimensionsSpec: {},
  };

  if (format === 'regex') {
    parseSpec.pattern = '(.*)';
    parseSpec.columns = ['column1'];
  }

  if (typeof hasHeaderRow === 'boolean') {
    parseSpec.hasHeaderRow = hasHeaderRow;
  }

  return parseSpec;
}

export type DruidFilter = Record<string, any>;

export interface DimensionFiltersWithRest {
  dimensionFilters: DruidFilter[];
  restFilter?: DruidFilter;
}

export function splitFilter(filter: DruidFilter | null): DimensionFiltersWithRest {
  const inputAndFilters: DruidFilter[] = filter
    ? filter.type === 'and' && Array.isArray(filter.fields)
      ? filter.fields
      : [filter]
    : EMPTY_ARRAY;
  const dimensionFilters: DruidFilter[] = inputAndFilters.filter(
    f => typeof f.dimension === 'string',
  );
  const restFilters: DruidFilter[] = inputAndFilters.filter(f => typeof f.dimension !== 'string');

  return {
    dimensionFilters,
    restFilter: restFilters.length
      ? restFilters.length > 1
        ? { type: 'and', filters: restFilters }
        : restFilters[0]
      : undefined,
  };
}

export function joinFilter(
  dimensionFiltersWithRest: DimensionFiltersWithRest,
): DruidFilter | undefined {
  const { dimensionFilters, restFilter } = dimensionFiltersWithRest;
  let newFields = dimensionFilters || EMPTY_ARRAY;
  if (restFilter && restFilter.type) newFields = newFields.concat([restFilter]);

  if (!newFields.length) return;
  if (newFields.length === 1) return newFields[0];
  return { type: 'and', fields: newFields };
}

const FILTER_FORM_FIELDS: Field<DruidFilter>[] = [
  {
    name: 'type',
    type: 'string',
    suggestions: ['selector', 'in', 'regex', 'like', 'not'],
  },
  {
    name: 'dimension',
    type: 'string',
    defined: (df: DruidFilter) => ['selector', 'in', 'regex', 'like'].includes(df.type),
  },
  {
    name: 'value',
    type: 'string',
    defined: (df: DruidFilter) => df.type === 'selector',
  },
  {
    name: 'values',
    type: 'string-array',
    defined: (df: DruidFilter) => df.type === 'in',
  },
  {
    name: 'pattern',
    type: 'string',
    defined: (df: DruidFilter) => ['regex', 'like'].includes(df.type),
  },

  {
    name: 'field.type',
    label: 'Sub-filter type',
    type: 'string',
    suggestions: ['selector', 'in', 'regex', 'like'],
    defined: (df: DruidFilter) => df.type === 'not',
  },
  {
    name: 'field.dimension',
    label: 'Sub-filter dimension',
    type: 'string',
    defined: (df: DruidFilter) => df.type === 'not',
  },
  {
    name: 'field.value',
    label: 'Sub-filter value',
    type: 'string',
    defined: (df: DruidFilter) => df.type === 'not' && deepGet(df, 'field.type') === 'selector',
  },
  {
    name: 'field.values',
    label: 'Sub-filter values',
    type: 'string-array',
    defined: (df: DruidFilter) => df.type === 'not' && deepGet(df, 'field.type') === 'in',
  },
  {
    name: 'field.pattern',
    label: 'Sub-filter pattern',
    type: 'string',
    defined: (df: DruidFilter) =>
      df.type === 'not' && ['regex', 'like'].includes(deepGet(df, 'field.type')),
  },
];

export function getFilterFormFields() {
  return FILTER_FORM_FIELDS;
}
