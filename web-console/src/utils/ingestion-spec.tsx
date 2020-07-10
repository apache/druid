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
import { getLink } from '../links';

import {
  BASIC_TIME_FORMATS,
  DATE_ONLY_TIME_FORMATS,
  DATETIME_TIME_FORMATS,
  OTHER_TIME_FORMATS,
} from './druid-time';
import { deepDelete, deepGet, deepMove, deepSet } from './object-change';

export const MAX_INLINE_DATA_LENGTH = 65536;

// These constants are used to make sure that they are not constantly recreated thrashing the pure components
export const EMPTY_OBJECT: any = {};
export const EMPTY_ARRAY: any[] = [];

const CURRENT_YEAR = new Date().getUTCFullYear();

export interface IngestionSpec {
  type: IngestionType;
  spec: IngestionSpecInner;
}

export interface IngestionSpecInner {
  ioConfig: IoConfig;
  dataSchema: DataSchema;
  tuningConfig?: TuningConfig;
}

export function isEmptyIngestionSpec(spec: IngestionSpec) {
  return Object.keys(spec).length === 0;
}

export type IngestionType = 'kafka' | 'kinesis' | 'index_parallel';

// A combination of IngestionType and inputSourceType
export type IngestionComboType =
  | 'kafka'
  | 'kinesis'
  | 'index_parallel:http'
  | 'index_parallel:local'
  | 'index_parallel:druid'
  | 'index_parallel:inline'
  | 'index_parallel:s3'
  | 'index_parallel:azure'
  | 'index_parallel:google'
  | 'index_parallel:hdfs';

// Some extra values that can be selected in the initial screen
export type IngestionComboTypeWithExtra = IngestionComboType | 'hadoop' | 'example' | 'other';

export function adjustIngestionSpec(spec: IngestionSpec) {
  const tuningConfig = deepGet(spec, 'spec.tuningConfig');
  if (tuningConfig) {
    spec = deepSet(spec, 'spec.tuningConfig', adjustTuningConfig(tuningConfig));
  }
  return spec;
}

function ingestionTypeToIoAndTuningConfigType(ingestionType: IngestionType): string {
  switch (ingestionType) {
    case 'kafka':
    case 'kinesis':
    case 'index_parallel':
      return ingestionType;

    default:
      throw new Error(`unknown type '${ingestionType}'`);
  }
}

export function getIngestionComboType(spec: IngestionSpec): IngestionComboType | undefined {
  const ioConfig = deepGet(spec, 'spec.ioConfig') || EMPTY_OBJECT;

  switch (ioConfig.type) {
    case 'kafka':
    case 'kinesis':
      return ioConfig.type;

    case 'index_parallel':
      const inputSource = deepGet(spec, 'spec.ioConfig.inputSource') || EMPTY_OBJECT;
      switch (inputSource.type) {
        case 'local':
        case 'http':
        case 'druid':
        case 'inline':
        case 's3':
        case 'azure':
        case 'google':
        case 'hdfs':
          return `${ioConfig.type}:${inputSource.type}` as IngestionComboType;
      }
  }

  return;
}

export function getIngestionTitle(ingestionType: IngestionComboTypeWithExtra): string {
  switch (ingestionType) {
    case 'index_parallel:local':
      return 'Local disk';

    case 'index_parallel:http':
      return 'HTTP(s)';

    case 'index_parallel:druid':
      return 'Reindex from Druid';

    case 'index_parallel:inline':
      return 'Paste data';

    case 'index_parallel:s3':
      return 'Amazon S3';

    case 'index_parallel:azure':
      return 'Azure Data Lake';

    case 'index_parallel:google':
      return 'Google Cloud Storage';

    case 'index_parallel:hdfs':
      return 'HDFS';

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

export function getIngestionDocLink(spec: IngestionSpec): string {
  const type = getSpecType(spec);

  switch (type) {
    case 'kafka':
      return `${getLink('DOCS')}/development/extensions-core/kafka-ingestion.html`;

    case 'kinesis':
      return `${getLink('DOCS')}/development/extensions-core/kinesis-ingestion.html`;

    default:
      return `${getLink('DOCS')}/ingestion/native-batch.html#firehoses`;
  }
}

export function getRequiredModule(ingestionType: IngestionComboTypeWithExtra): string | undefined {
  switch (ingestionType) {
    case 'index_parallel:s3':
      return 'druid-s3-extensions';

    case 'index_parallel:azure':
      return 'druid-azure-extensions';

    case 'index_parallel:google':
      return 'druid-google-extensions';

    case 'index_parallel:hdfs':
      return 'druid-hdfs-storage';

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
  timestampSpec: TimestampSpec;
  transformSpec?: TransformSpec;
  granularitySpec?: GranularitySpec;
  dimensionsSpec: DimensionsSpec;
  metricsSpec?: MetricSpec[];
}

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

export type DimensionMode = 'specific' | 'auto-detect';

export function getDimensionMode(spec: IngestionSpec): DimensionMode {
  const dimensions = deepGet(spec, 'spec.dataSchema.dimensionsSpec.dimensions') || EMPTY_ARRAY;
  return Array.isArray(dimensions) && dimensions.length === 0 ? 'auto-detect' : 'specific';
}

export function getRollup(spec: IngestionSpec): boolean {
  const specRollup = deepGet(spec, 'spec.dataSchema.granularitySpec.rollup');
  return typeof specRollup === 'boolean' ? specRollup : true;
}

export function getSpecType(spec: Partial<IngestionSpec>): IngestionType {
  return (
    deepGet(spec, 'type') ||
    deepGet(spec, 'spec.ioConfig.type') ||
    deepGet(spec, 'spec.tuningConfig.type') ||
    'index_parallel'
  );
}

export function isTask(spec: IngestionSpec) {
  const type = String(getSpecType(spec));
  return (
    type.startsWith('index_') ||
    ['index', 'compact', 'kill', 'append', 'merge', 'same_interval_merge'].includes(type)
  );
}

export function isDruidSource(spec: IngestionSpec): boolean {
  return deepGet(spec, 'spec.ioConfig.inputSource.type') === 'druid';
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
  if (typeof spec.spec !== 'object' && typeof (spec as any).ioConfig === 'object') {
    spec = { spec: spec as any };
  }

  const specType =
    deepGet(spec, 'type') ||
    deepGet(spec, 'spec.ioConfig.type') ||
    deepGet(spec, 'spec.tuningConfig.type');

  if (!specType) return spec as IngestionSpec;
  if (!deepGet(spec, 'type')) spec = deepSet(spec, 'type', specType);
  if (!deepGet(spec, 'spec.ioConfig.type')) spec = deepSet(spec, 'spec.ioConfig.type', specType);
  if (!deepGet(spec, 'spec.tuningConfig.type')) {
    spec = deepSet(spec, 'spec.tuningConfig.type', specType);
  }
  return spec as IngestionSpec;
}

/**
 * Make sure that any extra junk in the spec other than 'type' and 'spec' is removed
 * @param spec
 */
export function cleanSpec(spec: IngestionSpec): IngestionSpec {
  return {
    type: spec.type,
    spec: spec.spec,
  };
}

const INPUT_FORMAT_FORM_FIELDS: Field<InputFormat>[] = [
  {
    name: 'type',
    label: 'Input format',
    type: 'string',
    suggestions: ['json', 'csv', 'tsv', 'regex', 'parquet', 'orc', 'avro_ocf'],
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
    name: 'findColumnsFromHeader',
    type: 'boolean',
    required: true,
    defined: (p: InputFormat) => p.type === 'csv' || p.type === 'tsv',
  },
  {
    name: 'skipHeaderRows',
    type: 'number',
    defaultValue: 0,
    defined: (p: InputFormat) => p.type === 'csv' || p.type === 'tsv',
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
    required: (p: InputFormat) =>
      ((p.type === 'csv' || p.type === 'tsv') && !p.findColumnsFromHeader) || p.type === 'regex',
    defined: (p: InputFormat) =>
      ((p.type === 'csv' || p.type === 'tsv') && !p.findColumnsFromHeader) || p.type === 'regex',
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
    defined: (p: InputFormat) => p.type === 'csv' || p.type === 'tsv' || p.type === 'regex',
    info: <>A custom delimiter for multi-value dimensions.</>,
  },
  {
    name: 'binaryAsString',
    type: 'boolean',
    defaultValue: false,
    defined: (p: InputFormat) => p.type === 'parquet' || p.type === 'orc' || p.type === 'avro_ocf',
    info: (
      <>
        Specifies if the bytes parquet column which is not logically marked as a string or enum type
        should be treated as a UTF-8 encoded string.
      </>
    ),
  },
];

export function getInputFormatFormFields() {
  return INPUT_FORMAT_FORM_FIELDS;
}

export function issueWithInputFormat(inputFormat: InputFormat | undefined): string | undefined {
  if (!inputFormat) return 'no input format';
  if (!inputFormat.type) return 'missing a type';
  switch (inputFormat.type) {
    case 'regex':
      if (!inputFormat.pattern) return "must have a 'pattern'";
      break;

    case 'javascript':
      if (!inputFormat['function']) return "must have a 'function'";
      break;
  }
  return;
}

export function inputFormatCanFlatten(inputFormat: InputFormat): boolean {
  const inputFormatType = inputFormat.type;
  return (
    inputFormatType === 'json' ||
    inputFormatType === 'parquet' ||
    inputFormatType === 'orc' ||
    inputFormatType === 'avro_ocf'
  );
}

export interface TimestampSpec {
  column?: string;
  format?: string;
  missingValue?: string;
}

export function getTimestampSpecColumn(timestampSpec: TimestampSpec) {
  // https://github.com/apache/druid/blob/master/core/src/main/java/org/apache/druid/data/input/impl/TimestampSpec.java#L44
  return timestampSpec.column || 'timestamp';
}

const NO_SUCH_COLUMN = '!!!_no_such_column_!!!';

const DUMMY_TIMESTAMP_SPEC: TimestampSpec = {
  column: NO_SUCH_COLUMN,
  missingValue: '1970-01-01T00:00:00Z',
};

export function getDummyTimestampSpec() {
  return DUMMY_TIMESTAMP_SPEC;
}

const CONSTANT_TIMESTAMP_SPEC: TimestampSpec = {
  column: NO_SUCH_COLUMN,
  missingValue: '2010-01-01T00:00:00Z',
};

export function getConstantTimestampSpec() {
  return CONSTANT_TIMESTAMP_SPEC;
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
    suggestions: ['string', 'long', 'float', 'double'],
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
        <ExternalLink href={`${getLink('DOCS')}/ingestion/flatten-json`}>expression</ExternalLink>.
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
        <ExternalLink href={`${getLink('DOCS')}/misc/math-expr.html`}>expression</ExternalLink>.
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
    info: <>The metric name as it will appear in Druid.</>,
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
      'thetaSketch',
      {
        group: 'HLLSketch',
        suggestions: ['HLLSketchBuild', 'HLLSketchMerge'],
      },
      'quantilesDoublesSketch',
      'momentSketch',
      'fixedBucketsHistogram',
      'hyperUnique',
      'filtered',
    ],
    info: <>The aggregation function to apply.</>,
  },
  {
    name: 'fieldName',
    type: 'string',
    defined: m => m.type !== 'filtered',
    info: <>The column name for the aggregator to operate on.</>,
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
  // filtered
  {
    name: 'filter',
    type: 'json',
    defined: m => m.type === 'filtered',
  },
  {
    name: 'aggregator',
    type: 'json',
    defined: m => m.type === 'filtered',
  },
  // thetaSketch
  {
    name: 'size',
    type: 'number',
    defined: m => m.type === 'thetaSketch',
    defaultValue: 16384,
    info: (
      <>
        <p>
          Must be a power of 2. Internally, size refers to the maximum number of entries sketch
          object will retain. Higher size means higher accuracy but more space to store sketches.
          Note that after you index with a particular size, druid will persist sketch in segments
          and you will use size greater or equal to that at query time.
        </p>
        <p>
          See the{' '}
          <ExternalLink href="https://datasketches.apache.org/docs/Theta/ThetaSize.html">
            DataSketches site
          </ExternalLink>{' '}
          for details.
        </p>
        <p>In general, We recommend just sticking to default size.</p>
      </>
    ),
  },
  {
    name: 'isInputThetaSketch',
    type: 'boolean',
    defined: m => m.type === 'thetaSketch',
    defaultValue: false,
    info: (
      <>
        This should only be used at indexing time if your input data contains theta sketch objects.
        This would be the case if you use datasketches library outside of Druid, say with Pig/Hive,
        to produce the data that you are ingesting into Druid
      </>
    ),
  },
  // HLLSketchBuild & HLLSketchMerge
  {
    name: 'lgK',
    type: 'number',
    defined: m => m.type === 'HLLSketchBuild' || m.type === 'HLLSketchMerge',
    defaultValue: 12,
    info: (
      <>
        <p>
          log2 of K that is the number of buckets in the sketch, parameter that controls the size
          and the accuracy.
        </p>
        <p>Must be between 4 to 21 inclusively.</p>
      </>
    ),
  },
  {
    name: 'tgtHllType',
    type: 'string',
    defined: m => m.type === 'HLLSketchBuild' || m.type === 'HLLSketchMerge',
    defaultValue: 'HLL_4',
    suggestions: ['HLL_4', 'HLL_6', 'HLL_8'],
    info: (
      <>
        The type of the target HLL sketch. Must be <Code>HLL_4</Code>, <Code>HLL_6</Code>, or{' '}
        <Code>HLL_8</Code>.
      </>
    ),
  },
  // quantilesDoublesSketch
  {
    name: 'k',
    type: 'number',
    defined: m => m.type === 'quantilesDoublesSketch',
    defaultValue: 128,
    info: (
      <>
        <p>
          Parameter that determines the accuracy and size of the sketch. Higher k means higher
          accuracy but more space to store sketches.
        </p>
        <p>
          Must be a power of 2 from 2 to 32768. See the{' '}
          <ExternalLink href="https://datasketches.apache.org/docs/Quantiles/QuantilesAccuracy.html">
            Quantiles Accuracy
          </ExternalLink>{' '}
          for details.
        </p>
      </>
    ),
  },
  // momentSketch
  {
    name: 'k',
    type: 'number',
    defined: m => m.type === 'momentSketch',
    required: true,
    info: (
      <>
        Parameter that determines the accuracy and size of the sketch. Higher k means higher
        accuracy but more space to store sketches. Usable range is generally [3,15]
      </>
    ),
  },
  {
    name: 'compress',
    type: 'boolean',
    defined: m => m.type === 'momentSketch',
    defaultValue: true,
    info: (
      <>
        Flag for whether the aggregator compresses numeric values using arcsinh. Can improve
        robustness to skewed and long-tailed distributions, but reduces accuracy slightly on more
        uniform distributions.
      </>
    ),
  },
  // fixedBucketsHistogram
  {
    name: 'lowerLimit',
    type: 'number',
    defined: m => m.type === 'fixedBucketsHistogram',
    required: true,
    info: <>Lower limit of the histogram.</>,
  },
  {
    name: 'upperLimit',
    type: 'number',
    defined: m => m.type === 'fixedBucketsHistogram',
    required: true,
    info: <>Upper limit of the histogram.</>,
  },
  {
    name: 'numBuckets',
    type: 'number',
    defined: m => m.type === 'fixedBucketsHistogram',
    defaultValue: 10,
    required: true,
    info: (
      <>
        Number of buckets for the histogram. The range <Code>[lowerLimit, upperLimit]</Code> will be
        divided into <Code>numBuckets</Code> intervals of equal size.
      </>
    ),
  },
  {
    name: 'outlierHandlingMode',
    type: 'string',
    defined: m => m.type === 'fixedBucketsHistogram',
    required: true,
    suggestions: ['ignore', 'overflow', 'clip'],
    info: (
      <>
        <p>
          Specifies how values outside of <Code>[lowerLimit, upperLimit]</Code> will be handled.
        </p>
        <p>
          Supported modes are <Code>ignore</Code>, <Code>overflow</Code>, and <Code>clip</Code>. See
          <ExternalLink
            href={`${getLink(
              'DOCS',
            )}/development/extensions-core/approximate-histograms.html#outlier-handling-modes`}
          >
            outlier handling modes
          </ExternalLink>{' '}
          for more details.
        </p>
      </>
    ),
  },
  // hyperUnique
  {
    name: 'isInputHyperUnique',
    type: 'boolean',
    defined: m => m.type === 'hyperUnique',
    defaultValue: false,
    info: (
      <>
        This can be set to true to index precomputed HLL (Base64 encoded output from druid-hll is
        expected).
      </>
    ),
  },
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
  inputSource?: InputSource;
  inputFormat?: InputFormat;
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

export interface InputSource {
  type: string;
  baseDir?: string;
  filter?: any;
  uris?: string[];
  prefixes?: string[];
  objects?: { bucket: string; path: string }[];
  fetchTimeout?: number;

  // druid
  dataSource?: string;
  interval?: string;
  dimensions?: string[];
  metrics?: string[];
  maxInputSegmentBytesPerTask?: number;

  // inline
  data?: string;

  // hdfs
  paths?: string;
}

export function getIoConfigFormFields(ingestionComboType: IngestionComboType): Field<IoConfig>[] {
  const inputSourceType: Field<IoConfig> = {
    name: 'inputSource.type',
    label: 'Source type',
    type: 'string',
    suggestions: ['local', 'http', 'inline', 's3', 'azure', 'google', 'hdfs'],
    info: (
      <p>
        Druid connects to raw data through{' '}
        <ExternalLink href={`${getLink('DOCS')}/ingestion/firehose.html`}>
          inputSources
        </ExternalLink>
        . You can change your selected inputSource here.
      </p>
    ),
  };

  switch (ingestionComboType) {
    case 'index_parallel:http':
      return [
        inputSourceType,
        {
          name: 'inputSource.uris',
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
          name: 'inputSource.httpAuthenticationUsername',
          label: 'HTTP auth username',
          type: 'string',
          placeholder: '(optional)',
          info: <p>Username to use for authentication with specified URIs</p>,
        },
        {
          name: 'inputSource.httpAuthenticationPassword',
          label: 'HTTP auth password',
          type: 'string',
          placeholder: '(optional)',
          info: <p>Password to use for authentication with specified URIs</p>,
        },
      ];

    case 'index_parallel:local':
      return [
        inputSourceType,
        {
          name: 'inputSource.baseDir',
          label: 'Base directory',
          type: 'string',
          placeholder: '/path/to/files/',
          required: true,
          info: (
            <>
              <ExternalLink href={`${getLink('DOCS')}/ingestion/firehose.html#localfirehose`}>
                inputSource.baseDir
              </ExternalLink>
              <p>Specifies the directory to search recursively for files to be ingested.</p>
            </>
          ),
        },
        {
          name: 'inputSource.filter',
          label: 'File filter',
          type: 'string',
          required: true,
          suggestions: [
            '*',
            '*.json',
            '*.json.gz',
            '*.csv',
            '*.tsv',
            '*.parquet',
            '*.orc',
            '*.avro',
          ],
          info: (
            <>
              <ExternalLink href={`${getLink('DOCS')}/ingestion/firehose.html#localfirehose`}>
                inputSource.filter
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

    case 'index_parallel:druid':
      return [
        inputSourceType,
        {
          name: 'inputSource.dataSource',
          label: 'Datasource',
          type: 'string',
          required: true,
          info: <p>The datasource to fetch rows from.</p>,
        },
        {
          name: 'inputSource.interval',
          label: 'Interval',
          type: 'interval',
          placeholder: `${CURRENT_YEAR}-01-01/${CURRENT_YEAR + 1}-01-01`,
          required: true,
          info: (
            <p>
              A String representing ISO-8601 Interval. This defines the time range to fetch the data
              over.
            </p>
          ),
        },
        {
          name: 'inputSource.dimensions',
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
          name: 'inputSource.metrics',
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
          name: 'inputSource.filter',
          label: 'Filter',
          type: 'json',
          placeholder: '(optional)',
          info: (
            <p>
              The{' '}
              <ExternalLink href={`${getLink('DOCS')}/querying/filters.html`}>filter</ExternalLink>{' '}
              to apply to the data as part of querying.
            </p>
          ),
        },
      ];

    case 'index_parallel:inline':
      return [
        inputSourceType,
        // do not add 'data' here as it has special handling in the load-data view
      ];

    case 'index_parallel:s3':
      return [
        inputSourceType,
        {
          name: 'inputSource.uris',
          label: 'S3 URIs',
          type: 'string-array',
          placeholder: 's3://your-bucket/some-file1.ext, s3://your-bucket/some-file2.ext',
          required: true,
          defined: ioConfig =>
            !deepGet(ioConfig, 'inputSource.prefixes') && !deepGet(ioConfig, 'inputSource.objects'),
          info: (
            <>
              <p>
                The full S3 URI of your file. To ingest from multiple URIs, use commas to separate
                each individual URI.
              </p>
              <p>Either S3 URIs or prefixes or objects must be set.</p>
            </>
          ),
        },
        {
          name: 'inputSource.prefixes',
          label: 'S3 prefixes',
          type: 'string-array',
          placeholder: 's3://your-bucket/some-path1, s3://your-bucket/some-path2',
          required: true,
          defined: ioConfig =>
            !deepGet(ioConfig, 'inputSource.uris') && !deepGet(ioConfig, 'inputSource.objects'),
          info: (
            <>
              <p>A list of paths (with bucket) where your files are stored.</p>
              <p>Either S3 URIs or prefixes or objects must be set.</p>
            </>
          ),
        },
        {
          name: 'inputSource.objects',
          label: 'S3 objects',
          type: 'json',
          placeholder: '{"bucket":"your-bucket", "path":"some-file.ext"}',
          required: true,
          defined: ioConfig => deepGet(ioConfig, 'inputSource.objects'),
          info: (
            <>
              <p>
                JSON array of{' '}
                <ExternalLink href={`${getLink('DOCS')}/development/extensions-core/s3.html`}>
                  S3 Objects
                </ExternalLink>
                .
              </p>
              <p>Either S3 URIs or prefixes or objects must be set.</p>
            </>
          ),
        },

        {
          name: 'inputSource.properties.accessKeyId.type',
          label: 'Access key ID type',
          type: 'string',
          suggestions: [undefined, 'environment', 'default'],
          placeholder: '(none)',
          info: (
            <>
              <p>S3 access key type.</p>
              <p>Setting this will override the default configuration provided in the config.</p>
              <p>
                The access key can be pulled from an environment variable or inlined in the
                ingestion spec (default).
              </p>
              <p>
                Note: Inlining the access key into the ingestion spec is dangerous as it might
                appear in server log files and can be seen by anyone accessing this console.
              </p>
            </>
          ),
          adjustment: (ioConfig: IoConfig) => {
            return deepSet(
              ioConfig,
              'inputSource.properties.secretAccessKey.type',
              deepGet(ioConfig, 'inputSource.properties.accessKeyId.type'),
            );
          },
        },
        {
          name: 'inputSource.properties.accessKeyId.variable',
          label: 'Access key ID environment variable',
          type: 'string',
          placeholder: '(environment variable name)',
          defined: (ioConfig: IoConfig) =>
            deepGet(ioConfig, 'inputSource.properties.accessKeyId.type') === 'environment',
          info: <p>The environment variable containing the S3 access key for this S3 bucket.</p>,
        },
        {
          name: 'inputSource.properties.accessKeyId.password',
          label: 'Access key ID value',
          type: 'string',
          placeholder: '(access key)',
          defined: (ioConfig: IoConfig) =>
            deepGet(ioConfig, 'inputSource.properties.accessKeyId.type') === 'default',
          info: (
            <>
              <p>S3 access key for this S3 bucket.</p>
              <p>
                Note: Inlining the access key into the ingestion spec is dangerous as it might
                appear in server log files and can be seen by anyone accessing this console.
              </p>
            </>
          ),
        },

        {
          name: 'inputSource.properties.secretAccessKey.type',
          label: 'Secret key type',
          type: 'string',
          suggestions: [undefined, 'environment', 'default'],
          placeholder: '(none)',
          info: (
            <>
              <p>S3 secret key type.</p>
              <p>Setting this will override the default configuration provided in the config.</p>
              <p>
                The secret key can be pulled from an environment variable or inlined in the
                ingestion spec (default).
              </p>
              <p>
                Note: Inlining the secret key into the ingestion spec is dangerous as it might
                appear in server log files and can be seen by anyone accessing this console.
              </p>
            </>
          ),
        },
        {
          name: 'inputSource.properties.secretAccessKey.variable',
          label: 'Secret key value',
          type: 'string',
          placeholder: '(environment variable name)',
          defined: (ioConfig: IoConfig) =>
            deepGet(ioConfig, 'inputSource.properties.secretAccessKey.type') === 'environment',
          info: <p>The environment variable containing the S3 secret key for this S3 bucket.</p>,
        },
        {
          name: 'inputSource.properties.secretAccessKey.password',
          label: 'Secret key value',
          type: 'string',
          placeholder: '(secret key)',
          defined: (ioConfig: IoConfig) =>
            deepGet(ioConfig, 'inputSource.properties.secretAccessKey.type') === 'default',
          info: (
            <>
              <p>S3 secret key for this S3 bucket.</p>
              <p>
                Note: Inlining the access key into the ingestion spec is dangerous as it might
                appear in server log files and can be seen by anyone accessing this console.
              </p>
            </>
          ),
        },
      ];

    case 'index_parallel:azure':
      return [
        inputSourceType,
        {
          name: 'inputSource.uris',
          label: 'Azure URIs',
          type: 'string-array',
          placeholder:
            'azure://your-container/some-file1.ext, azure://your-container/some-file2.ext',
          required: true,
          defined: ioConfig =>
            !deepGet(ioConfig, 'inputSource.prefixes') && !deepGet(ioConfig, 'inputSource.objects'),
          info: (
            <>
              <p>
                The full Azure URI of your file. To ingest from multiple URIs, use commas to
                separate each individual URI.
              </p>
              <p>Either Azure URIs or prefixes or objects must be set.</p>
            </>
          ),
        },
        {
          name: 'inputSource.prefixes',
          label: 'Azure prefixes',
          type: 'string-array',
          placeholder: 'azure://your-container/some-path1, azure://your-container/some-path2',
          required: true,
          defined: ioConfig =>
            !deepGet(ioConfig, 'inputSource.uris') && !deepGet(ioConfig, 'inputSource.objects'),
          info: (
            <>
              <p>A list of paths (with bucket) where your files are stored.</p>
              <p>Either Azure URIs or prefixes or objects must be set.</p>
            </>
          ),
        },
        {
          name: 'inputSource.objects',
          label: 'Azure objects',
          type: 'json',
          placeholder: '{"bucket":"your-container", "path":"some-file.ext"}',
          required: true,
          defined: ioConfig => deepGet(ioConfig, 'inputSource.objects'),
          info: (
            <>
              <p>
                JSON array of{' '}
                <ExternalLink href={`${getLink('DOCS')}/development/extensions-core/azure.html`}>
                  S3 Objects
                </ExternalLink>
                .
              </p>
              <p>Either Azure URIs or prefixes or objects must be set.</p>
            </>
          ),
        },
      ];

    case 'index_parallel:google':
      return [
        inputSourceType,
        {
          name: 'inputSource.uris',
          label: 'Google Cloud Storage URIs',
          type: 'string-array',
          placeholder: 'gs://your-bucket/some-file1.ext, gs://your-bucket/some-file2.ext',
          required: true,
          defined: ioConfig =>
            !deepGet(ioConfig, 'inputSource.prefixes') && !deepGet(ioConfig, 'inputSource.objects'),
          info: (
            <>
              <p>
                The full Google Cloud Storage URI of your file. To ingest from multiple URIs, use
                commas to separate each individual URI.
              </p>
              <p>Either Google Cloud Storage URIs or prefixes or objects must be set.</p>
            </>
          ),
        },
        {
          name: 'inputSource.prefixes',
          label: 'Google Cloud Storage prefixes',
          type: 'string-array',
          placeholder: 'gs://your-bucket/some-path1, gs://your-bucket/some-path2',
          required: true,
          defined: ioConfig =>
            !deepGet(ioConfig, 'inputSource.uris') && !deepGet(ioConfig, 'inputSource.objects'),
          info: (
            <>
              <p>A list of paths (with bucket) where your files are stored.</p>
              <p>Either Google Cloud Storage URIs or prefixes or objects must be set.</p>
            </>
          ),
        },
        {
          name: 'inputSource.objects',
          label: 'Google Cloud Storage objects',
          type: 'json',
          placeholder: '{"bucket":"your-bucket", "path":"some-file.ext"}',
          required: true,
          defined: ioConfig => deepGet(ioConfig, 'inputSource.objects'),
          info: (
            <>
              <p>
                JSON array of{' '}
                <ExternalLink href={`${getLink('DOCS')}/development/extensions-core/google.html`}>
                  Google Cloud Storage Objects
                </ExternalLink>
                .
              </p>
              <p>Either Google Cloud Storage URIs or prefixes or objects must be set.</p>
            </>
          ),
        },
      ];

    case 'index_parallel:hdfs':
      return [
        inputSourceType,
        {
          name: 'inputSource.paths',
          label: 'Paths',
          type: 'string',
          placeholder: '/path/to/file.ext',
          required: true,
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
              <ExternalLink
                href={`${getLink(
                  'DOCS',
                )}/development/extensions-core/kafka-ingestion#kafkasupervisorioconfig`}
              >
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
              <ExternalLink
                href={`${getLink(
                  'DOCS',
                )}/development/extensions-core/kafka-ingestion#kafkasupervisorioconfig`}
              >
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
              <ExternalLink href={`https://docs.aws.amazon.com/general/latest/gr/ak.html`}>
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

function issueWithInputSource(inputSource: InputSource | undefined): string | undefined {
  if (!inputSource) return 'does not exist';
  if (!inputSource.type) return 'missing a type';
  switch (inputSource.type) {
    case 'local':
      if (!inputSource.baseDir) return `must have a 'baseDir'`;
      if (!inputSource.filter) return `must have a 'filter'`;
      break;

    case 'http':
      if (!nonEmptyArray(inputSource.uris)) {
        return 'must have at least one uri';
      }
      break;

    case 'druid':
      if (!inputSource.dataSource) return `must have a 'dataSource'`;
      if (!inputSource.interval) return `must have an 'interval'`;
      break;

    case 'inline':
      if (!inputSource.data) return `must have 'data'`;
      break;

    case 's3':
    case 'azure':
    case 'google':
      if (
        !nonEmptyArray(inputSource.uris) &&
        !nonEmptyArray(inputSource.prefixes) &&
        !nonEmptyArray(inputSource.objects)
      ) {
        return 'must have at least one uri or prefix or object';
      }
      break;

    case 'hdfs':
      if (!inputSource.paths) {
        return 'must have paths';
      }
      break;
  }
  return;
}

export function issueWithIoConfig(
  ioConfig: IoConfig | undefined,
  ignoreInputFormat = false,
): string | undefined {
  if (!ioConfig) return 'does not exist';
  if (!ioConfig.type) return 'missing a type';
  switch (ioConfig.type) {
    case 'index':
    case 'index_parallel':
      if (issueWithInputSource(ioConfig.inputSource)) {
        return `inputSource: '${issueWithInputSource(ioConfig.inputSource)}'`;
      }
      break;

    case 'kafka':
      if (!ioConfig.topic) return 'must have a topic';
      break;

    case 'kinesis':
      if (!ioConfig.stream) return 'must have a stream';
      break;
  }

  if (!ignoreInputFormat && issueWithInputFormat(ioConfig.inputFormat)) {
    return `inputFormat: '${issueWithInputFormat(ioConfig.inputFormat)}'`;
  }

  return;
}

export function getIoConfigTuningFormFields(
  ingestionComboType: IngestionComboType,
): Field<IoConfig>[] {
  switch (ingestionComboType) {
    case 'index_parallel:http':
    case 'index_parallel:s3':
    case 'index_parallel:azure':
    case 'index_parallel:google':
    case 'index_parallel:hdfs':
      return [
        {
          name: 'inputSource.fetchTimeout',
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
          name: 'inputSource.maxFetchRetry',
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
          name: 'inputSource.maxCacheCapacityBytes',
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
          name: 'inputSource.maxFetchCapacityBytes',
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
          name: 'inputSource.prefetchTriggerBytes',
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

    case 'index_parallel:local':
    case 'index_parallel:inline':
      return [];

    case 'index_parallel:druid':
      return [
        {
          name: 'inputSource.maxFetchCapacityBytes',
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
  return deepSet(spec, 'spec.dataSchema.dataSource', possibleName);
}

export function guessDataSourceName(spec: IngestionSpec): string | undefined {
  const ioConfig = deepGet(spec, 'spec.ioConfig');
  if (!ioConfig) return;

  switch (ioConfig.type) {
    case 'index':
    case 'index_parallel':
      const inputSource = ioConfig.inputSource;
      if (!inputSource) return;

      switch (inputSource.type) {
        case 'local':
          if (inputSource.filter && filterIsFilename(inputSource.filter)) {
            return basenameFromFilename(inputSource.filter);
          } else if (inputSource.baseDir) {
            return filenameFromPath(inputSource.baseDir);
          } else {
            return;
          }

        case 's3':
        case 'azure':
        case 'google':
          const actualPath = (inputSource.objects || EMPTY_ARRAY)[0];
          const uriPath =
            (inputSource.uris || EMPTY_ARRAY)[0] || (inputSource.prefixes || EMPTY_ARRAY)[0];
          return actualPath ? actualPath.path : uriPath ? filenameFromPath(uriPath) : undefined;

        case 'http':
          return Array.isArray(inputSource.uris)
            ? filenameFromPath(inputSource.uris[0])
            : undefined;

        case 'druid':
          return inputSource.dataSource;

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
  partitionsSpec?: PartitionsSpec;
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

export interface PartitionsSpec {
  type: 'string';

  // For type: dynamic
  maxTotalRows?: number;

  // For type: hashed
  numShards?: number;
  partitionDimensions?: string[];

  // For type: single_dim
  targetRowsPerSegment?: number;
  maxRowsPerSegment?: number;
  partitionDimension?: string;
  assumeGrouped?: boolean;
}

export function adjustTuningConfig(tuningConfig: TuningConfig) {
  const tuningConfigType = deepGet(tuningConfig, 'type');
  if (tuningConfigType !== 'index_parallel') return tuningConfig;

  const partitionsSpecType = deepGet(tuningConfig, 'partitionsSpec.type');
  if (tuningConfig.forceGuaranteedRollup) {
    if (partitionsSpecType !== 'hashed' && partitionsSpecType !== 'single_dim') {
      tuningConfig = deepSet(tuningConfig, 'partitionsSpec', { type: 'hashed' });
    }
  } else {
    if (partitionsSpecType !== 'dynamic') {
      tuningConfig = deepSet(tuningConfig, 'partitionsSpec', { type: 'dynamic' });
    }
  }
  return tuningConfig;
}

export function invalidTuningConfig(tuningConfig: TuningConfig, intervals: any): boolean {
  if (tuningConfig.type !== 'index_parallel' || !tuningConfig.forceGuaranteedRollup) return false;

  if (!intervals) return true;
  switch (deepGet(tuningConfig, 'partitionsSpec.type')) {
    case 'hashed':
      if (!deepGet(tuningConfig, 'partitionsSpec.numShards')) return true;
      break;

    case 'single_dim':
      if (!deepGet(tuningConfig, 'partitionsSpec.partitionDimension')) return true;
      if (
        !deepGet(tuningConfig, 'partitionsSpec.targetRowsPerSegment') &&
        !deepGet(tuningConfig, 'partitionsSpec.maxRowsPerSegment')
      ) {
        return true;
      }
  }

  return false;
}

export function getPartitionRelatedTuningSpecFormFields(
  specType: IngestionType,
): Field<TuningConfig>[] {
  switch (specType) {
    case 'index_parallel':
      return [
        {
          name: 'forceGuaranteedRollup',
          type: 'boolean',
          defaultValue: false,
          info: (
            <p>
              Forces guaranteeing the perfect rollup. The perfect rollup optimizes the total size of
              generated segments and querying time while indexing time will be increased. If this is
              set to true, the index task will read the entire input data twice: one for finding the
              optimal number of partitions per time chunk and one for generating segments.
            </p>
          ),
        },
        {
          name: 'partitionsSpec.type',
          label: 'Partitioning type',
          type: 'string',
          suggestions: (t: TuningConfig) =>
            t.forceGuaranteedRollup ? ['hashed', 'single_dim'] : ['dynamic'],
          info: (
            <p>
              For perfect rollup, you should use either <Code>hashed</Code> (partitioning based on
              the hash of dimensions in each row) or <Code>single_dim</Code> (based on ranges of a
              single dimension. For best-effort rollup, you should use dynamic.
            </p>
          ),
        },
        // partitionsSpec type: dynamic
        {
          name: 'partitionsSpec.maxRowsPerSegment',
          label: 'Max rows per segment',
          type: 'number',
          defaultValue: 5000000,
          defined: (t: TuningConfig) => deepGet(t, 'partitionsSpec.type') === 'dynamic',
          info: <>Determines how many rows are in each segment.</>,
        },
        {
          name: 'partitionsSpec.maxTotalRows',
          label: 'Max total rows',
          type: 'number',
          defaultValue: 20000000,
          defined: (t: TuningConfig) => deepGet(t, 'partitionsSpec.type') === 'dynamic',
          info: <>Total number of rows in segments waiting for being pushed.</>,
        },
        // partitionsSpec type: hashed
        {
          name: 'partitionsSpec.numShards',
          label: 'Num shards',
          type: 'number',
          defined: (t: TuningConfig) => deepGet(t, 'partitionsSpec.type') === 'hashed',
          required: true,
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
          name: 'partitionsSpec.partitionDimensions',
          label: 'Partition dimensions',
          type: 'string-array',
          defined: (t: TuningConfig) => deepGet(t, 'partitionsSpec.type') === 'hashed',
          info: <p>The dimensions to partition on. Leave blank to select all dimensions.</p>,
        },
        // partitionsSpec type: single_dim
        {
          name: 'partitionsSpec.partitionDimension',
          label: 'Partition dimension',
          type: 'string',
          defined: (t: TuningConfig) => deepGet(t, 'partitionsSpec.type') === 'single_dim',
          required: true,
          info: <p>The dimension to partition on.</p>,
        },
        {
          name: 'partitionsSpec.targetRowsPerSegment',
          label: 'Target rows per segment',
          type: 'number',
          zeroMeansUndefined: true,
          defined: (t: TuningConfig) => deepGet(t, 'partitionsSpec.type') === 'single_dim',
          required: (t: TuningConfig) =>
            !deepGet(t, 'partitionsSpec.targetRowsPerSegment') &&
            !deepGet(t, 'partitionsSpec.maxRowsPerSegment'),
          info: (
            <p>
              Target number of rows to include in a partition, should be a number that targets
              segments of 500MB~1GB.
            </p>
          ),
        },
        {
          name: 'partitionsSpec.maxRowsPerSegment',
          label: 'Max rows per segment',
          type: 'number',
          zeroMeansUndefined: true,
          defined: (t: TuningConfig) => deepGet(t, 'partitionsSpec.type') === 'single_dim',
          required: (t: TuningConfig) =>
            !deepGet(t, 'partitionsSpec.targetRowsPerSegment') &&
            !deepGet(t, 'partitionsSpec.maxRowsPerSegment'),
          info: <p>Maximum number of rows to include in a partition.</p>,
        },
        {
          name: 'partitionsSpec.assumeGrouped',
          label: 'Assume grouped',
          type: 'boolean',
          defaultValue: false,
          defined: (t: TuningConfig) => deepGet(t, 'partitionsSpec.type') === 'single_dim',
          info: (
            <p>
              Assume that input data has already been grouped on time and dimensions. Ingestion will
              run faster, but may choose sub-optimal partitions if this assumption is violated.
            </p>
          ),
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
    min: 1,
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
    defaultValue: 'roaring',
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
    info: <>Compression format for primitive type metric columns.</>,
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
  const [ingestionType, inputSourceType] = comboType.split(':');
  const ioAndTuningConfigType = ingestionTypeToIoAndTuningConfigType(
    ingestionType as IngestionType,
  );

  let newSpec = spec;
  newSpec = deepSet(newSpec, 'type', ingestionType);
  newSpec = deepSet(newSpec, 'spec.ioConfig.type', ioAndTuningConfigType);
  newSpec = deepSet(newSpec, 'spec.tuningConfig.type', ioAndTuningConfigType);

  if (inputSourceType) {
    newSpec = deepSet(newSpec, 'spec.ioConfig.inputSource', { type: inputSourceType });

    if (inputSourceType === 'local') {
      newSpec = deepSet(newSpec, 'spec.ioConfig.inputSource.filter', '*');
    }
  }

  if (!deepGet(spec, 'spec.dataSchema.dataSource')) {
    newSpec = deepSet(newSpec, 'spec.dataSchema.dataSource', 'new-data-source');
  }

  if (!deepGet(spec, 'spec.dataSchema.granularitySpec')) {
    const granularitySpec: GranularitySpec = {
      type: 'uniform',
      queryGranularity: 'HOUR',
    };
    if (ingestionType !== 'index_parallel') {
      granularitySpec.segmentGranularity = 'HOUR';
    }

    newSpec = deepSet(newSpec, 'spec.dataSchema.granularitySpec', granularitySpec);
  }

  if (!deepGet(spec, 'spec.dataSchema.timestampSpec')) {
    newSpec = deepSet(newSpec, 'spec.dataSchema.timestampSpec', getDummyTimestampSpec());
  }

  if (!deepGet(spec, 'spec.dataSchema.dimensionsSpec')) {
    newSpec = deepSet(newSpec, 'spec.dataSchema.dimensionsSpec', {});
  }

  return newSpec;
}

export function fillInputFormat(spec: IngestionSpec, sampleData: string[]): IngestionSpec {
  return deepSet(spec, 'spec.ioConfig.inputFormat', guessInputFormat(sampleData));
}

function guessInputFormat(sampleData: string[]): InputFormat {
  let sampleDatum = sampleData[0];
  if (sampleDatum) {
    sampleDatum = String(sampleDatum); // Really ensure it is a string

    // First check for magic byte sequences as they rarely yield false positives

    // Parquet 4 byte magic header: https://github.com/apache/parquet-format#file-format
    if (sampleDatum.startsWith('PAR1')) {
      return inputFormatFromType('parquet');
    }
    // ORC 3 byte magic header: https://orc.apache.org/specification/ORCv1/
    if (sampleDatum.startsWith('ORC')) {
      return inputFormatFromType('orc');
    }
    // Avro OCF 4 byte magic header: https://avro.apache.org/docs/current/spec.html#Object+Container+Files
    if (sampleDatum.startsWith('Obj1')) {
      return inputFormatFromType('avro_ocf');
    }

    // After checking for magic byte sequences perform heuristics to deduce string formats

    // If the string starts and ends with curly braces assume JSON
    if (sampleDatum.startsWith('{') && sampleDatum.endsWith('}')) {
      return inputFormatFromType('json');
    }
    // Contains more than 3 tabs assume TSV
    if (sampleDatum.split('\t').length > 3) {
      return inputFormatFromType('tsv', !/\t\d+\t/.test(sampleDatum));
    }
    // Contains more than 3 commas assume CSV
    if (sampleDatum.split(',').length > 3) {
      return inputFormatFromType('csv', !/,\d+,/.test(sampleDatum));
    }
  }

  return inputFormatFromType('regex');
}

function inputFormatFromType(type: string, findColumnsFromHeader?: boolean): InputFormat {
  const inputFormat: InputFormat = { type };

  if (type === 'regex') {
    inputFormat.pattern = '(.*)';
    inputFormat.columns = ['column1'];
  }

  if (typeof findColumnsFromHeader === 'boolean') {
    inputFormat.findColumnsFromHeader = findColumnsFromHeader;
  }

  return inputFormat;
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

export function upgradeSpec(spec: any): any {
  if (deepGet(spec, 'spec.ioConfig.firehose')) {
    switch (deepGet(spec, 'spec.ioConfig.firehose.type')) {
      case 'static-s3':
        deepSet(spec, 'spec.ioConfig.firehose.type', 's3');
        break;

      case 'static-google-blobstore':
        deepSet(spec, 'spec.ioConfig.firehose.type', 'google');
        deepMove(spec, 'spec.ioConfig.firehose.blobs', 'spec.ioConfig.firehose.objects');
        break;
    }

    spec = deepMove(spec, 'spec.ioConfig.firehose', 'spec.ioConfig.inputSource');
    spec = deepMove(
      spec,
      'spec.dataSchema.parser.parseSpec.timestampSpec',
      'spec.dataSchema.timestampSpec',
    );
    spec = deepMove(
      spec,
      'spec.dataSchema.parser.parseSpec.dimensionsSpec',
      'spec.dataSchema.dimensionsSpec',
    );
    spec = deepMove(spec, 'spec.dataSchema.parser.parseSpec', 'spec.ioConfig.inputFormat');
    spec = deepDelete(spec, 'spec.dataSchema.parser');
    spec = deepMove(spec, 'spec.ioConfig.inputFormat.format', 'spec.ioConfig.inputFormat.type');
  }
  return spec;
}

export function downgradeSpec(spec: any): any {
  if (deepGet(spec, 'spec.ioConfig.inputSource')) {
    spec = deepMove(spec, 'spec.ioConfig.inputFormat.type', 'spec.ioConfig.inputFormat.format');
    spec = deepSet(spec, 'spec.dataSchema.parser', { type: 'string' });
    spec = deepMove(spec, 'spec.ioConfig.inputFormat', 'spec.dataSchema.parser.parseSpec');
    spec = deepMove(
      spec,
      'spec.dataSchema.dimensionsSpec',
      'spec.dataSchema.parser.parseSpec.dimensionsSpec',
    );
    spec = deepMove(
      spec,
      'spec.dataSchema.timestampSpec',
      'spec.dataSchema.parser.parseSpec.timestampSpec',
    );
    spec = deepMove(spec, 'spec.ioConfig.inputSource', 'spec.ioConfig.firehose');

    switch (deepGet(spec, 'spec.ioConfig.firehose.type')) {
      case 's3':
        deepSet(spec, 'spec.ioConfig.firehose.type', 'static-s3');
        break;

      case 'google':
        deepSet(spec, 'spec.ioConfig.firehose.type', 'static-google-blobstore');
        deepMove(spec, 'spec.ioConfig.firehose.objects', 'spec.ioConfig.firehose.blobs');
        break;
    }
  }
  return spec;
}
