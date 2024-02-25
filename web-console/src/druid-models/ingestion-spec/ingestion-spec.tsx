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
import { range } from 'd3-array';
import { csvParseRows, tsvParseRows } from 'd3-dsv';
import type { JSX } from 'react';
import React from 'react';

import type { Field } from '../../components';
import { AutoForm, ExternalLink } from '../../components';
import { IndexSpecDialog } from '../../dialogs/index-spec-dialog/index-spec-dialog';
import { getLink } from '../../links';
import {
  allowKeys,
  deepDelete,
  deepGet,
  deepMove,
  deepSet,
  deepSetIfUnset,
  EMPTY_ARRAY,
  EMPTY_OBJECT,
  filterMap,
  findMap,
  isSimpleArray,
  oneOf,
  oneOfKnown,
  parseCsvLine,
  typeIsKnown,
} from '../../utils';
import type { SampleResponse } from '../../utils/sampler';
import type { DimensionSpec, DimensionsSpec } from '../dimension-spec/dimension-spec';
import {
  getDimensionSpecColumnType,
  getDimensionSpecName,
  getDimensionSpecs,
} from '../dimension-spec/dimension-spec';
import type { FlattenSpec } from '../flatten-spec/flatten-spec';
import type { IndexSpec } from '../index-spec/index-spec';
import { summarizeIndexSpec } from '../index-spec/index-spec';
import type { InputFormat } from '../input-format/input-format';
import { issueWithInputFormat } from '../input-format/input-format';
import type { InputSource } from '../input-source/input-source';
import { FILTER_SUGGESTIONS, issueWithInputSource } from '../input-source/input-source';
import type { MetricSpec } from '../metric-spec/metric-spec';
import {
  getMetricSpecOutputType,
  getMetricSpecs,
  getMetricSpecSingleFieldName,
} from '../metric-spec/metric-spec';
import type { TimestampSpec } from '../timestamp-spec/timestamp-spec';
import type { TransformSpec } from '../transform-spec/transform-spec';

export const MAX_INLINE_DATA_LENGTH = 65536;

const CURRENT_YEAR = new Date().getUTCFullYear();

export interface IngestionSpec {
  readonly type: IngestionType;
  readonly spec: IngestionSpecInner;
  readonly context?: { useConcurrentLocks?: boolean };
  readonly suspended?: boolean;
}

export interface IngestionSpecInner {
  readonly ioConfig: IoConfig;
  readonly dataSchema: DataSchema;
  readonly tuningConfig?: TuningConfig;
}

export function isEmptyIngestionSpec(spec: Partial<IngestionSpec>) {
  return Object.keys(spec).length === 0;
}

export type IngestionType = 'kafka' | 'kinesis' | 'index_parallel';
const KNOWN_TYPES = ['kafka', 'kinesis', 'index_parallel'];

// A combination of IngestionType and inputSourceType
export type IngestionComboType =
  | 'kafka'
  | 'kinesis'
  | 'index_parallel:http'
  | 'index_parallel:local'
  | 'index_parallel:druid'
  | 'index_parallel:inline'
  | 'index_parallel:s3'
  | 'index_parallel:azureStorage'
  | 'index_parallel:delta'
  | 'index_parallel:google'
  | 'index_parallel:hdfs';

// Some extra values that can be selected in the initial screen
export type IngestionComboTypeWithExtra =
  | IngestionComboType
  | 'azure-event-hubs'
  | 'hadoop'
  | 'example'
  | 'other';

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

export function getIngestionComboType(
  spec: Partial<IngestionSpec>,
): IngestionComboType | undefined {
  const ioConfig = deepGet(spec, 'spec.ioConfig') || EMPTY_OBJECT;

  switch (ioConfig.type) {
    case 'kafka':
    case 'kinesis':
      return ioConfig.type;

    case 'index_parallel': {
      const inputSource = deepGet(spec, 'spec.ioConfig.inputSource') || EMPTY_OBJECT;
      switch (inputSource.type) {
        case 'local':
        case 'http':
        case 'delta':
        case 'druid':
        case 'inline':
        case 's3':
        case 'azureStorage':
        case 'google':
        case 'hdfs':
          return `${ioConfig.type}:${inputSource.type}` as IngestionComboType;
      }
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

    case 'index_parallel:azureStorage':
      return 'Azure Data Lake';

    case 'index_parallel:delta':
      return 'Delta Lake';

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

    case 'azure-event-hubs':
      return 'Azure Event Hub';

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

export function getIngestionDocLink(spec: Partial<IngestionSpec>): string {
  const type = getSpecType(spec);

  switch (type) {
    case 'kafka':
      return `${getLink('DOCS')}/development/extensions-core/kafka-ingestion.html`;

    case 'kinesis':
      return `${getLink('DOCS')}/development/extensions-core/kinesis-ingestion.html`;

    default:
      return `${getLink('DOCS')}/ingestion/native-batch.html#input-sources`;
  }
}

export function getRequiredModule(ingestionType: IngestionComboTypeWithExtra): string | undefined {
  switch (ingestionType) {
    case 'index_parallel:s3':
      return 'druid-s3-extensions';

    case 'index_parallel:azureStorage':
      return 'druid-azure-extensions';

    case 'index_parallel:delta':
      return 'druid-deltalake-extensions';

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

export function getIssueWithSpec(spec: Partial<IngestionSpec>): string | undefined {
  if (!deepGet(spec, 'spec.dataSchema.dataSource')) {
    return 'missing spec.dataSchema.dataSource';
  }

  if (!deepGet(spec, 'spec.dataSchema.timestampSpec')) {
    return 'missing spec.dataSchema.timestampSpec';
  }

  if (!deepGet(spec, 'spec.dataSchema.dimensionsSpec')) {
    return 'missing spec.dataSchema.dimensionsSpec';
  }

  return;
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

export type SchemaMode = 'fixed' | 'string-only-discovery' | 'type-aware-discovery';

export type ArrayMode = 'arrays' | 'multi-values';

export function getSchemaMode(spec: Partial<IngestionSpec>): SchemaMode {
  if (deepGet(spec, 'spec.dataSchema.dimensionsSpec.useSchemaDiscovery') === true) {
    return 'type-aware-discovery';
  }
  if (deepGet(spec, 'spec.dataSchema.dimensionsSpec.includeAllDimensions') === true) {
    return 'string-only-discovery';
  }
  const dimensions = deepGet(spec, 'spec.dataSchema.dimensionsSpec.dimensions') || EMPTY_ARRAY;
  return Array.isArray(dimensions) && dimensions.length === 0 ? 'string-only-discovery' : 'fixed';
}

export function getArrayMode(spec: Partial<IngestionSpec>): ArrayMode {
  const schemaMode = getSchemaMode(spec);
  switch (schemaMode) {
    case 'type-aware-discovery':
      return 'arrays';

    case 'string-only-discovery':
      return 'multi-values';

    default: {
      const dimensions: (DimensionSpec | string)[] = deepGet(
        spec,
        'spec.dataSchema.dimensionsSpec.dimensions',
      );

      if (
        dimensions.some(
          d =>
            typeof d === 'object' && d.type === 'auto' && String(d.castToType).startsWith('ARRAY'),
        )
      ) {
        return 'arrays';
      }

      if (
        dimensions.some(
          d =>
            typeof d === 'object' &&
            d.type === 'string' &&
            typeof d.multiValueHandling === 'string',
        )
      ) {
        return 'multi-values';
      }

      return 'arrays';
    }
  }
}

export function showArrayModeToggle(spec: Partial<IngestionSpec>): boolean {
  const schemaMode = getSchemaMode(spec);
  if (schemaMode !== 'fixed') return false;

  const dimensions: (DimensionSpec | string)[] = deepGet(
    spec,
    'spec.dataSchema.dimensionsSpec.dimensions',
  );

  return dimensions.some(
    d =>
      typeof d === 'object' &&
      ((d.type === 'auto' && String(d.castToType).startsWith('ARRAY')) ||
        (d.type === 'string' && typeof d.multiValueHandling === 'string')),
  );
}

export function getRollup(spec: Partial<IngestionSpec>, valueIfUnset = true): boolean {
  const specRollup = deepGet(spec, 'spec.dataSchema.granularitySpec.rollup');
  return typeof specRollup === 'boolean' ? specRollup : valueIfUnset;
}

export function getSpecType(spec: Partial<IngestionSpec>): IngestionType {
  return (
    deepGet(spec, 'type') ||
    deepGet(spec, 'spec.ioConfig.type') ||
    deepGet(spec, 'spec.tuningConfig.type') ||
    'index_parallel'
  );
}

export function isStreamingSpec(spec: Partial<IngestionSpec>): boolean {
  return oneOf(getSpecType(spec), 'kafka', 'kinesis');
}

export function isDruidSource(spec: Partial<IngestionSpec>): boolean {
  return deepGet(spec, 'spec.ioConfig.inputSource.type') === 'druid';
}

export function getPossibleSystemFieldsForSpec(spec: Partial<IngestionSpec>): string[] {
  const inputSource = deepGet(spec, 'spec.ioConfig.inputSource');
  if (!inputSource) return [];
  return getPossibleSystemFieldsForInputSource(inputSource);
}

export function getFlattenSpec(spec: Partial<IngestionSpec>): FlattenSpec | undefined {
  const inputFormat: InputFormat | undefined = deepGet(spec, 'spec.ioConfig.inputFormat');
  if (!inputFormat) return;
  return (
    (inputFormat.type === 'kafka'
      ? inputFormat.valueFormat?.flattenSpec
      : inputFormat.flattenSpec) || undefined
  );
}

export function changeFlattenSpec(
  spec: Partial<IngestionSpec>,
  flattenSpec: FlattenSpec | undefined,
): Partial<IngestionSpec> {
  if (deepGet(spec, 'spec.ioConfig.inputFormat.type') === 'kafka') {
    return deepSet(spec, 'spec.ioConfig.inputFormat.valueFormat.flattenSpec', flattenSpec);
  } else {
    return deepSet(spec, 'spec.ioConfig.inputFormat.flattenSpec', flattenSpec);
  }
}

export function getPossibleSystemFieldsForInputSource(inputSource: InputSource): string[] {
  switch (inputSource.type) {
    case 's3':
    case 'google':
    case 'azureStorage':
      return ['__file_uri', '__file_bucket', '__file_path'];

    case 'hdfs':
    case 'local':
      return ['__file_uri', '__file_path'];

    default:
      return [];
  }
}

export const ALL_POSSIBLE_SYSTEM_FIELDS: string[] = ['__file_uri', '__file_bucket', '__file_path'];

// ---------------------------------
// Spec cleanup and normalization

/**
 * Make sure that the ioConfig, dataSchema, e.t.c. are nested inside of spec and not just hanging out at the top level
 * @param spec
 */
function nestSpecIfNeeded(spec: any): Partial<IngestionSpec> {
  if (spec?.type && typeof spec.spec !== 'object' && (spec.ioConfig || spec.dataSchema)) {
    return {
      type: spec.type,
      spec: deepDelete(spec, 'type'),
    };
  }
  return spec;
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

  spec = nestSpecIfNeeded(spec);

  const specType =
    deepGet(spec, 'type') ||
    deepGet(spec, 'spec.ioConfig.type') ||
    deepGet(spec, 'spec.tuningConfig.type');

  if (!specType) return spec as IngestionSpec;
  spec = deepSetIfUnset(spec, 'type', specType);
  spec = deepSetIfUnset(spec, 'spec.ioConfig.type', specType);
  spec = deepSetIfUnset(spec, 'spec.tuningConfig.type', specType);

  return spec as IngestionSpec;
}

/**
 * Make sure that any extra junk in the spec other than 'type', 'spec', and 'context' is removed
 * @param spec - the spec to clean
 * @param allowSuspended - allow keeping 'suspended' also
 */
export function cleanSpec(
  spec: Partial<IngestionSpec>,
  allowSuspended?: boolean,
): Partial<IngestionSpec> {
  return allowKeys(
    spec,
    ['type', 'spec', 'context'].concat(allowSuspended ? ['suspended'] : []) as any,
  ) as IngestionSpec;
}

export function upgradeSpec(spec: any, yolo = false): Partial<IngestionSpec> {
  spec = nestSpecIfNeeded(spec);

  // Upgrade firehose if exists
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
  }

  // Decompose parser if exists
  if (deepGet(spec, 'spec.dataSchema.parser')) {
    if (!yolo) {
      const parserType = deepGet(spec, 'spec.dataSchema.parser.type');
      if (parserType !== 'string') {
        throw new Error(
          `Can not rewrite parser of type '${parserType}', only 'string' is supported`,
        );
      }
    }

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

// ------------------------------------

export interface GranularitySpec {
  type?: string;
  queryGranularity?: string;
  segmentGranularity?: string;
  rollup?: boolean;
  intervals?: string | string[];
}

// --------------

export interface IoConfig {
  type: string;
  inputSource?: InputSource;
  inputFormat?: InputFormat;
  appendToExisting?: boolean;
  topic?: string;
  topicPattern?: string;
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

export function getIoConfigFormFields(ingestionComboType: IngestionComboType): Field<IoConfig>[] {
  const inputSourceType: Field<IoConfig> = {
    name: 'inputSource.type',
    label: 'Source type',
    type: 'string',
    suggestions: ['local', 'http', 'inline', 'delta', 's3', 'azureStorage', 'google', 'hdfs'],
    info: (
      <p>
        Druid connects to raw data through{' '}
        <ExternalLink href={`${getLink('DOCS')}/ingestion/native-batch.html#input-sources`}>
          inputSources
        </ExternalLink>
        . You can change your selected inputSource here.
      </p>
    ),
  };

  const inputSourceFilter: Field<IoConfig> = {
    name: 'inputSource.filter',
    label: 'File filter',
    type: 'string',
    suggestions: FILTER_SUGGESTIONS,
    placeholder: '*',
    info: (
      <p>
        A wildcard filter for files. See{' '}
        <ExternalLink href="https://commons.apache.org/proper/commons-io/apidocs/org/apache/commons/io/filefilter/WildcardFileFilter.html">
          here
        </ExternalLink>{' '}
        for format information. Files matching the filter criteria are considered for ingestion.
        Files not matching the filter criteria are ignored.
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
              <ExternalLink href={`${getLink('DOCS')}/ingestion/native-batch.html#input-sources`}>
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
          suggestions: FILTER_SUGGESTIONS,
          info: (
            <>
              <ExternalLink
                href={`${getLink('DOCS')}/ingestion/native-batch.html#local-input-source`}
              >
                inputSource.filter
              </ExternalLink>
              <p>
                A wildcard filter for files. See{' '}
                <ExternalLink href="https://commons.apache.org/proper/commons-io/apidocs/org/apache/commons/io/filefilter/WildcardFileFilter.html">
                  here
                </ExternalLink>{' '}
                for format information. Files matching the filter criteria are considered for
                ingestion. Files not matching the filter criteria are ignored.
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
          name: 'inputSource.filter',
          label: 'Filter',
          type: 'json',
          placeholder: '(optional)',
          hideInMore: true,
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
        inputSourceFilter,
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
          adjustment: ioConfig => {
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
          defined: ioConfig =>
            deepGet(ioConfig, 'inputSource.properties.accessKeyId.type') === 'environment',
          info: <p>The environment variable containing the S3 access key for this S3 bucket.</p>,
        },
        {
          name: 'inputSource.properties.accessKeyId.password',
          label: 'Access key ID value',
          type: 'string',
          placeholder: '(access key)',
          defined: ioConfig =>
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
          label: 'Secret access key type',
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
          label: 'Secret access key environment variable',
          type: 'string',
          placeholder: '(environment variable name)',
          defined: ioConfig =>
            deepGet(ioConfig, 'inputSource.properties.secretAccessKey.type') === 'environment',
          info: <p>The environment variable containing the S3 secret key for this S3 bucket.</p>,
        },
        {
          name: 'inputSource.properties.secretAccessKey.password',
          label: 'Secret access key value',
          type: 'string',
          placeholder: '(secret key)',
          defined: ioConfig =>
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

    case 'index_parallel:azureStorage':
      return [
        inputSourceType,
        {
          name: 'inputSource.uris',
          label: 'Azure URIs',
          type: 'string-array',
          placeholder:
            'azureStorage://your-storage-account/your-container/some-file1.ext, azureStorage://your-storage-account/your-container/some-file2.ext',
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
          placeholder:
            'azureStorage://your-storage-account/your-container/some-path1, azureStorage://your-storage-account/your-container/some-path2',
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
          placeholder: '{"bucket":"your-storage-account", "path":"your-container/some-file.ext"}',
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
        inputSourceFilter,
        {
          name: 'inputSource.properties.sharedAccessStorageToken',
          label: 'Shared Access Storage Token',
          type: 'string',
          placeholder: '(sas token)',
          info: (
            <>
              <p>Shared Access Storage Token for this storage account.</p>
              <p>
                Note: Inlining the sas token into the ingestion spec can be dangerous as it might
                appear in server log files and can be seen by anyone accessing this console.
              </p>
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
        inputSourceFilter,
      ];

    case 'index_parallel:delta':
      return [
        inputSourceType,
        {
          name: 'inputSource.tablePath',
          label: 'Delta table path',
          type: 'string',
          placeholder: '/path/to/deltaTable',
          required: true,
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
          placeholder: 'kafka_broker_host:9092',
          info: (
            <>
              <ExternalLink
                href={`${getLink(
                  'DOCS',
                )}/development/extensions-core/kafka-ingestion#supervisor-io-configuration`}
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
          defined: ioConfig =>
            oneOfKnown(ioConfig.type, KNOWN_TYPES, 'kafka') && !ioConfig.topicPattern,
          placeholder: 'your_kafka_topic',
          info: 'The name of the Kafka topic to ingest from.',
        },
        {
          name: 'topicPattern',
          type: 'string',
          required: true,
          defined: ioConfig => oneOfKnown(ioConfig.type, KNOWN_TYPES, 'kafka') && !ioConfig.topic,
          placeholder: 'topic1|topic2',
          info: (
            <>
              <p>
                A regular expression that represents all topics to be ingested from. For example, to
                ingest data from <Code>clicks</Code> and <Code>impressions</Code>, you can set this
                to <Code>clicks|impressions</Code>. Alternatively, to ingest from all topics
                starting with <Code>metrics-</Code> set this to <Code>metrics-.*</Code>.
              </p>
              <p>
                If new topics are added to the cluster that match the regex, Druid will
                automatically start ingesting from those new topics.
              </p>
            </>
          ),
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
                )}/development/extensions-core/kafka-ingestion#supervisor-io-configuration`}
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
          info: (
            <>
              The Amazon Kinesis stream endpoint for a region. You can find a list of endpoints{' '}
              <ExternalLink href="https://docs.aws.amazon.com/general/latest/gr/ak.html">
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
      if (!ioConfig.topic && !ioConfig.topicPattern) return 'must have a topic or topicPattern';
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
    case 'index_parallel:azureStorage':
    case 'index_parallel:google':
    case 'index_parallel:delta':
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
          defined: typeIsKnown(KNOWN_TYPES, 'kafka'),
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
          defined: typeIsKnown(KNOWN_TYPES, 'kinesis'),
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
                number of tasks (reading + publishing) will be higher than this. See &apos;Capacity
                Planning&apos; below for more details.
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
          name: 'pollTimeout',
          type: 'number',
          defaultValue: 100,
          defined: typeIsKnown(KNOWN_TYPES, 'kafka'),
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
          defaultValue: 0,
          defined: typeIsKnown(KNOWN_TYPES, 'kinesis'),
          info: <>Time in milliseconds to wait between subsequent GetRecords calls to Kinesis.</>,
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
          defined: typeIsKnown(KNOWN_TYPES, 'kafka'),
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
  const parts = path.split('/');
  while (parts.length && !parts[parts.length - 1]) parts.pop();
  return parts.length ? basenameFromFilename(parts[parts.length - 1]) : undefined;
}

function basenameFromFilename(filename: string): string | undefined {
  return filename.split('.')[0];
}

export function fillDataSourceNameIfNeeded(spec: Partial<IngestionSpec>): Partial<IngestionSpec> {
  const possibleName = guessDataSourceName(spec);
  if (!possibleName) return spec;
  return deepSetIfUnset(spec, 'spec.dataSchema.dataSource', possibleName);
}

export function guessDataSourceNameFromInputSource(inputSource: InputSource): string | undefined {
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
    case 'azureStorage':
    case 'google': {
      const actualPath = (inputSource.objects || EMPTY_ARRAY)[0];
      const uriPath =
        (inputSource.uris || EMPTY_ARRAY)[0] || (inputSource.prefixes || EMPTY_ARRAY)[0];
      return actualPath ? actualPath.path : uriPath ? filenameFromPath(uriPath) : undefined;
    }

    case 'http':
      return Array.isArray(inputSource.uris) ? filenameFromPath(inputSource.uris[0]) : undefined;

    case 'druid':
      return inputSource.dataSource;

    case 'inline':
      return 'inline_data';

    default:
      return;
  }
}

export function guessDataSourceName(spec: Partial<IngestionSpec>): string | undefined {
  const ioConfig = deepGet(spec, 'spec.ioConfig');
  if (!ioConfig) return;

  switch (ioConfig.type) {
    case 'index':
    case 'index_parallel': {
      const inputSource = ioConfig.inputSource;
      if (!inputSource) return;
      return guessDataSourceNameFromInputSource(inputSource);
    }

    case 'kafka':
      return ioConfig.topic || ioConfig.topicPattern;

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
  indexSpecForIntermediatePersists?: IndexSpec;
  forceExtendableShardSpecs?: boolean;
  forceGuaranteedRollup?: boolean;
  reportParseExceptions?: boolean;
  pushTimeout?: number;
  segmentWriteOutMediumFactory?: any;
  intermediateHandoffPeriod?: string;
  handoffConditionTimeout?: number;
  resetOffsetAutomatically?: boolean;
  workerThreads?: number;
  chatRetries?: number;
  httpTimeout?: string;
  shutdownTimeout?: string;
  offsetFetchPeriod?: string;
  maxParseExceptions?: number;
  maxSavedParseExceptions?: number;
  recordBufferSizeBytes?: number;
  recordBufferOfferTimeout?: number;
  recordBufferFullWait?: number;
  fetchThreads?: number;
}

export interface PartitionsSpec {
  type: string;

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

export function adjustForceGuaranteedRollup(spec: Partial<IngestionSpec>) {
  if (getSpecType(spec) !== 'index_parallel') return spec;

  const partitionsSpecType = deepGet(spec, 'spec.tuningConfig.partitionsSpec.type') || 'dynamic';
  if (partitionsSpecType === 'dynamic') {
    spec = deepDelete(spec, 'spec.tuningConfig.forceGuaranteedRollup');
  } else if (oneOf(partitionsSpecType, 'hashed', 'single_dim', 'range')) {
    spec = deepSet(spec, 'spec.tuningConfig.forceGuaranteedRollup', true);
  }

  return spec;
}

export function invalidPartitionConfig(spec: Partial<IngestionSpec>): boolean {
  return (
    // Bad primary partitioning, or...
    !deepGet(spec, 'spec.dataSchema.granularitySpec.segmentGranularity') ||
    // Bad secondary partitioning
    !AutoForm.isValidModel(spec, getSecondaryPartitionRelatedFormFields(spec, undefined))
  );
}

export const PRIMARY_PARTITION_RELATED_FORM_FIELDS: Field<IngestionSpec>[] = [
  {
    name: 'spec.dataSchema.granularitySpec.segmentGranularity',
    label: 'Segment granularity',
    type: 'string',
    suggestions: ['hour', 'day', 'month', 'year', 'all'],
    required: true,
    info: (
      <>
        The granularity to create time chunks at. Multiple segments can be created per time chunk.
        For example, with &apos;DAY&apos; segmentGranularity, the events of the same day fall into
        the same time chunk which can be optionally further partitioned into multiple segments based
        on other configurations and input size.
      </>
    ),
  },
  {
    name: 'spec.dataSchema.granularitySpec.intervals',
    label: 'Time intervals',
    type: 'string-array',
    placeholder: '(auto determine)',
    defined: s => getSpecType(s) === 'index_parallel',
    info: (
      <>
        <p>
          A list of intervals describing what time chunks of segments should be created. This list
          will be broken up and rounded-off based on the segmentGranularity.
        </p>
        <p>
          If not provided, batch ingestion tasks will generally determine which time chunks to
          output based on what timestamps are found in the input data.
        </p>
        <p>
          If specified, batch ingestion tasks may be able to skip a determining-partitions phase,
          which can result in faster ingestion. Batch ingestion tasks may also be able to request
          all their locks up-front instead of one by one. Batch ingestion tasks will throw away any
          records with timestamps outside of the specified intervals.
        </p>
      </>
    ),
  },
];

export function getSecondaryPartitionRelatedFormFields(
  spec: Partial<IngestionSpec>,
  dimensionSuggestions: string[] | undefined,
): Field<IngestionSpec>[] {
  const specType = getSpecType(spec);
  switch (specType) {
    case 'index_parallel':
      return [
        {
          name: 'spec.tuningConfig.partitionsSpec.type',
          label: 'Partitioning type',
          type: 'string',
          required: true,
          suggestions: ['dynamic', 'hashed', 'range'],
          info: (
            <p>
              For perfect rollup, you should use either <Code>hashed</Code> (partitioning based on
              the hash of dimensions in each row) or <Code>range</Code> (based on several
              dimensions). For best-effort rollup, you should use <Code>dynamic</Code>.
            </p>
          ),
          adjustment: s => {
            if (Array.isArray(dimensionSuggestions) && dimensionSuggestions.length) {
              const partitionsSpecType = deepGet(s, 'spec.tuningConfig.partitionsSpec.type');
              if (partitionsSpecType === 'range') {
                return deepSet(s, 'spec.tuningConfig.partitionsSpec.partitionDimensions', [
                  dimensionSuggestions[0],
                ]);
              }

              if (partitionsSpecType === 'single_dim') {
                return deepSet(
                  s,
                  'spec.tuningConfig.partitionsSpec.partitionDimension',
                  dimensionSuggestions[0],
                );
              }
            }

            return s;
          },
        },
        // partitionsSpec type: dynamic
        {
          name: 'spec.tuningConfig.partitionsSpec.maxRowsPerSegment',
          type: 'number',
          defaultValue: 5000000,
          defined: s => deepGet(s, 'spec.tuningConfig.partitionsSpec.type') === 'dynamic',
          info: <>Determines how many rows are in each segment.</>,
        },
        {
          name: 'spec.tuningConfig.partitionsSpec.maxTotalRows',
          type: 'number',
          defaultValue: 20000000,
          defined: s => deepGet(s, 'spec.tuningConfig.partitionsSpec.type') === 'dynamic',
          info: <>Total number of rows in segments waiting for being pushed.</>,
        },
        // partitionsSpec type: hashed
        {
          name: 'spec.tuningConfig.partitionsSpec.targetRowsPerSegment',
          type: 'number',
          zeroMeansUndefined: true,
          defaultValue: 5000000,
          defined: s =>
            deepGet(s, 'spec.tuningConfig.partitionsSpec.type') === 'hashed' &&
            !deepGet(s, 'spec.tuningConfig.partitionsSpec.numShards'),
          info: (
            <>
              <p>
                If the segments generated are a sub-optimal size for the requested partition
                dimensions, consider setting this field.
              </p>
              <p>
                A target row count for each partition. Each partition will have a row count close to
                the target assuming evenly distributed keys. Defaults to 5 million if numShards is
                null.
              </p>
            </>
          ),
        },
        {
          name: 'spec.tuningConfig.partitionsSpec.numShards',
          type: 'number',
          zeroMeansUndefined: true,
          hideInMore: true,
          defined: s =>
            deepGet(s, 'spec.tuningConfig.partitionsSpec.type') === 'hashed' &&
            !deepGet(s, 'spec.tuningConfig.partitionsSpec.targetRowsPerSegment'),
          info: (
            <>
              <p>
                If you know the optimal number of shards and want to speed up the time it takes for
                compaction to run, set this field.
              </p>
              <p>
                Directly specify the number of shards to create. If this is specified and
                &apos;intervals&apos; is specified in the granularitySpec, the index task can skip
                the determine intervals/partitions pass through the data.
              </p>
            </>
          ),
        },
        {
          name: 'spec.tuningConfig.partitionsSpec.partitionDimensions',
          type: 'string-array',
          placeholder: '(all dimensions)',
          defined: s => deepGet(s, 'spec.tuningConfig.partitionsSpec.type') === 'hashed',
          info: (
            <>
              <p>The dimensions to partition on.</p>
              <p>Leave blank to select all dimensions.</p>
              <p>
                If you want to partition on specific dimensions then you would likely be better off
                using <Code>range</Code> partitioning instead.
              </p>
            </>
          ),
          hideInMore: true,
        },
        // partitionsSpec type: single_dim, range
        {
          name: 'spec.tuningConfig.partitionsSpec.partitionDimension',
          type: 'string',
          defined: s => deepGet(s, 'spec.tuningConfig.partitionsSpec.type') === 'single_dim',
          required: true,
          suggestions: dimensionSuggestions,
          info: (
            <>
              <p>The dimension to partition on.</p>
              <p>
                This should be the first dimension in your schema which would make it first in the
                sort order. As{' '}
                <ExternalLink href={`${getLink('DOCS')}/ingestion/index.html#why-partition`}>
                  Partitioning and sorting are best friends!
                </ExternalLink>
              </p>
            </>
          ),
        },
        {
          name: 'spec.tuningConfig.partitionsSpec.partitionDimensions',
          type: 'string-array',
          defined: s => deepGet(s, 'spec.tuningConfig.partitionsSpec.type') === 'range',
          required: true,
          suggestions: dimensionSuggestions
            ? s => {
                const existingDimensions =
                  deepGet(s, 'spec.tuningConfig.partitionsSpec.partitionDimensions') || [];
                return dimensionSuggestions.filter(
                  dimensionSuggestion => !existingDimensions.includes(dimensionSuggestion),
                );
              }
            : undefined,
          info: <p>The dimensions to partition on.</p>,
        },
        {
          name: 'spec.tuningConfig.partitionsSpec.targetRowsPerSegment',
          type: 'number',
          zeroMeansUndefined: true,
          defined: s =>
            oneOf(deepGet(s, 'spec.tuningConfig.partitionsSpec.type'), 'single_dim', 'range') &&
            !deepGet(s, 'spec.tuningConfig.partitionsSpec.maxRowsPerSegment'),
          required: s =>
            !deepGet(s, 'spec.tuningConfig.partitionsSpec.targetRowsPerSegment') &&
            !deepGet(s, 'spec.tuningConfig.partitionsSpec.maxRowsPerSegment'),
          info: (
            <p>
              Target number of rows to include in a partition, should be a number that targets
              segments of 500MB~1GB.
            </p>
          ),
        },
        {
          name: 'spec.tuningConfig.partitionsSpec.maxRowsPerSegment',
          type: 'number',
          zeroMeansUndefined: true,
          defined: s =>
            oneOf(deepGet(s, 'spec.tuningConfig.partitionsSpec.type'), 'single_dim', 'range') &&
            !deepGet(s, 'spec.tuningConfig.partitionsSpec.targetRowsPerSegment'),
          required: s =>
            !deepGet(s, 'spec.tuningConfig.partitionsSpec.targetRowsPerSegment') &&
            !deepGet(s, 'spec.tuningConfig.partitionsSpec.maxRowsPerSegment'),
          info: <p>Maximum number of rows to include in a partition.</p>,
        },
        {
          name: 'spec.tuningConfig.partitionsSpec.assumeGrouped',
          type: 'boolean',
          defaultValue: false,
          hideInMore: true,
          defined: s =>
            oneOf(deepGet(s, 'spec.tuningConfig.partitionsSpec.type'), 'single_dim', 'range'),
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
          name: 'spec.tuningConfig.maxRowsPerSegment',
          type: 'number',
          defaultValue: 5000000,
          info: <>Determines how many rows are in each segment.</>,
        },
        {
          name: 'spec.tuningConfig.maxTotalRows',
          type: 'number',
          defaultValue: 20000000,
          info: <>Total number of rows in segments waiting for being pushed.</>,
        },
      ];
  }

  throw new Error(`unknown spec type ${specType}`);
}

const TUNING_FORM_FIELDS: Field<IngestionSpec>[] = [
  {
    name: 'spec.tuningConfig.maxNumConcurrentSubTasks',
    type: 'number',
    defaultValue: 1,
    min: 1,
    defined: typeIsKnown(KNOWN_TYPES, 'index_parallel'),
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
    name: 'spec.tuningConfig.maxRetry',
    type: 'number',
    defaultValue: 3,
    defined: typeIsKnown(KNOWN_TYPES, 'index_parallel'),
    hideInMore: true,
    info: <>Maximum number of retries on task failures.</>,
  },
  {
    name: 'spec.tuningConfig.taskStatusCheckPeriodMs',
    type: 'number',
    defaultValue: 1000,
    defined: typeIsKnown(KNOWN_TYPES, 'index_parallel'),
    hideInMore: true,
    info: <>Polling period in milliseconds to check running task statuses.</>,
  },
  {
    name: 'spec.tuningConfig.totalNumMergeTasks',
    type: 'number',
    defaultValue: 10,
    min: 1,
    defined: s =>
      s.type === 'index_parallel' &&
      oneOf(deepGet(s, 'spec.tuningConfig.partitionsSpec.type'), 'hashed', 'single_dim', 'range'),
    info: <>Number of tasks to merge partial segments after shuffle.</>,
  },
  {
    name: 'spec.tuningConfig.maxNumSegmentsToMerge',
    type: 'number',
    defaultValue: 100,
    defined: s =>
      s.type === 'index_parallel' &&
      oneOf(deepGet(s, 'spec.tuningConfig.partitionsSpec.type'), 'hashed', 'single_dim', 'range'),
    info: (
      <>
        Max limit for the number of segments a single task can merge at the same time after shuffle.
      </>
    ),
  },
  {
    name: 'spec.tuningConfig.maxRowsInMemory',
    type: 'number',
    defaultValue: 1000000,
    info: <>Used in determining when intermediate persists to disk should occur.</>,
  },
  {
    name: 'spec.tuningConfig.maxBytesInMemory',
    type: 'number',
    placeholder: 'Default: 1/6 of max JVM memory',
    info: <>Used in determining when intermediate persists to disk should occur.</>,
  },
  {
    name: 'spec.tuningConfig.maxColumnsToMerge',
    type: 'number',
    defaultValue: -1,
    min: -1,
    hideInMore: true,
    info: (
      <>
        <p>
          Limit of the number of segments to merge in a single phase when merging segments for
          publishing. This limit affects the total number of columns present in a set of segments to
          merge. If the limit is exceeded, segment merging occurs in multiple phases. Druid merges
          at least 2 segments per phase, regardless of this setting.
        </p>
        <p>Default: -1 (unlimited)</p>
      </>
    ),
  },
  {
    name: 'spec.tuningConfig.resetOffsetAutomatically',
    type: 'boolean',
    defaultValue: false,
    defined: typeIsKnown(KNOWN_TYPES, 'kafka', 'kinesis'),
    info: (
      <>
        Whether to reset the consumer offset if the next offset that it is trying to fetch is less
        than the earliest available offset for that particular partition.
      </>
    ),
  },
  {
    name: 'spec.tuningConfig.skipSequenceNumberAvailabilityCheck',
    type: 'boolean',
    defaultValue: false,
    defined: typeIsKnown(KNOWN_TYPES, 'kinesis'),
    info: (
      <>
        Whether to enable checking if the current sequence number is still available in a particular
        Kinesis shard. If set to false, the indexing task will attempt to reset the current sequence
        number (or not), depending on the value of <Code>resetOffsetAutomatically</Code>.
      </>
    ),
  },
  {
    name: 'spec.tuningConfig.intermediatePersistPeriod',
    type: 'duration',
    defaultValue: 'PT10M',
    defined: typeIsKnown(KNOWN_TYPES, 'kafka', 'kinesis'),
    info: <>The period that determines the rate at which intermediate persists occur.</>,
  },
  {
    name: 'spec.tuningConfig.intermediateHandoffPeriod',
    type: 'duration',
    defaultValue: 'P2147483647D',
    defined: typeIsKnown(KNOWN_TYPES, 'kafka', 'kinesis'),
    info: (
      <>
        How often the tasks should hand off segments. Handoff will happen either if
        maxRowsPerSegment or maxTotalRows is hit or every intermediateHandoffPeriod, whichever
        happens earlier.
      </>
    ),
  },
  {
    name: 'spec.tuningConfig.maxPendingPersists',
    type: 'number',
    hideInMore: true,
    info: (
      <>
        Maximum number of persists that can be pending but not started. If this limit would be
        exceeded by a new intermediate persist, ingestion will block until the currently-running
        persist finishes.
      </>
    ),
  },
  {
    name: 'spec.tuningConfig.pushTimeout',
    type: 'number',
    defaultValue: 0,
    hideInMore: true,
    info: (
      <>
        Milliseconds to wait for pushing segments. It must be &gt;= 0, where 0 means to wait
        forever.
      </>
    ),
  },
  {
    name: 'spec.tuningConfig.handoffConditionTimeout',
    type: 'number',
    defaultValue: 0,
    defined: typeIsKnown(KNOWN_TYPES, 'kafka', 'kinesis'),
    hideInMore: true,
    info: <>Milliseconds to wait for segment handoff. 0 means to wait forever.</>,
  },

  {
    name: 'spec.tuningConfig.indexSpec',
    type: 'custom',
    hideInMore: true,
    info: <>Defines segment storage format options to use at indexing time.</>,
    placeholder: 'Default index spec',
    customSummary: summarizeIndexSpec,
    customDialog: ({ value, onValueChange, onClose }) => (
      <IndexSpecDialog onClose={onClose} onSave={onValueChange} indexSpec={value} />
    ),
  },
  {
    name: 'spec.tuningConfig.indexSpecForIntermediatePersists',
    type: 'custom',
    hideInMore: true,
    info: (
      <>
        Defines segment storage format options to use at indexing time for intermediate persisted
        temporary segments.
      </>
    ),
    placeholder: 'Default index spec',
    customSummary: summarizeIndexSpec,
    customDialog: ({ value, onValueChange, onClose }) => (
      <IndexSpecDialog
        title="Index spec for intermediate persists"
        onClose={onClose}
        onSave={onValueChange}
        indexSpec={value}
      />
    ),
  },

  {
    name: 'spec.tuningConfig.splitHintSpec.maxSplitSize',
    type: 'number',
    defaultValue: 1073741824,
    min: 1000000,
    defined: s =>
      s.type === 'index_parallel' && deepGet(s, 'spec.ioConfig.inputSource.type') !== 'http',
    hideInMore: true,
    adjustment: s => deepSet(s, 'spec.tuningConfig.splitHintSpec.type', 'maxSize'),
    info: (
      <>
        Maximum number of bytes of input files to process in a single subtask. If a single file is
        larger than this number, it will be processed by itself in a single subtask (Files are never
        split across tasks yet).
      </>
    ),
  },
  {
    name: 'spec.tuningConfig.splitHintSpec.maxNumFiles',
    type: 'number',
    defaultValue: 1000,
    min: 1,
    defined: typeIsKnown(KNOWN_TYPES, 'index_parallel'),
    hideInMore: true,
    adjustment: s => deepSet(s, 'spec.tuningConfig.splitHintSpec.type', 'maxSize'),
    info: (
      <>
        Maximum number of input files to process in a single subtask. This limit is to avoid task
        failures when the ingestion spec is too long. There are two known limits on the max size of
        serialized ingestion spec, i.e., the max ZNode size in ZooKeeper (
        <Code>jute.maxbuffer</Code>) and the max packet size in MySQL (
        <Code>max_allowed_packet</Code>). These can make ingestion tasks fail if the serialized
        ingestion spec size hits one of them.
      </>
    ),
  },
  {
    name: 'spec.tuningConfig.chatHandlerTimeout',
    type: 'duration',
    defaultValue: 'PT10S',
    defined: typeIsKnown(KNOWN_TYPES, 'index_parallel'),
    hideInMore: true,
    info: <>Timeout for reporting the pushed segments in worker tasks.</>,
  },
  {
    name: 'spec.tuningConfig.chatHandlerNumRetries',
    type: 'number',
    defaultValue: 5,
    defined: typeIsKnown(KNOWN_TYPES, 'index_parallel'),
    hideInMore: true,
    info: <>Retries for reporting the pushed segments in worker tasks.</>,
  },
  {
    name: 'spec.tuningConfig.workerThreads',
    type: 'number',
    placeholder: 'min(10, taskCount)',
    defined: typeIsKnown(KNOWN_TYPES, 'kafka', 'kinesis'),
    info: (
      <>The number of threads that will be used by the supervisor for asynchronous operations.</>
    ),
  },
  {
    name: 'spec.tuningConfig.chatRetries',
    type: 'number',
    defaultValue: 8,
    defined: typeIsKnown(KNOWN_TYPES, 'kafka', 'kinesis'),
    hideInMore: true,
    info: (
      <>
        The number of times HTTP requests to indexing tasks will be retried before considering tasks
        unresponsive.
      </>
    ),
  },
  {
    name: 'spec.tuningConfig.httpTimeout',
    type: 'duration',
    defaultValue: 'PT10S',
    defined: typeIsKnown(KNOWN_TYPES, 'kafka', 'kinesis'),
    info: <>How long to wait for a HTTP response from an indexing task.</>,
  },
  {
    name: 'spec.tuningConfig.shutdownTimeout',
    type: 'duration',
    defaultValue: 'PT80S',
    defined: typeIsKnown(KNOWN_TYPES, 'kafka', 'kinesis'),
    hideInMore: true,
    info: (
      <>
        How long to wait for the supervisor to attempt a graceful shutdown of tasks before exiting.
      </>
    ),
  },
  {
    name: 'spec.tuningConfig.offsetFetchPeriod',
    type: 'duration',
    defaultValue: 'PT30S',
    defined: typeIsKnown(KNOWN_TYPES, 'kafka'),
    info: (
      <>
        How often the supervisor queries Kafka and the indexing tasks to fetch current offsets and
        calculate lag.
      </>
    ),
  },
  {
    name: 'spec.tuningConfig.recordBufferSizeBytes',
    type: 'number',
    defaultValue: 100000000,
    defined: typeIsKnown(KNOWN_TYPES, 'kinesis'),
    info: (
      <>
        Size of the buffer (heap memory bytes) used between the Kinesis fetch threads and the main
        ingestion thread.
      </>
    ),
  },
  {
    name: 'spec.tuningConfig.recordBufferOfferTimeout',
    type: 'number',
    defaultValue: 5000,
    defined: typeIsKnown(KNOWN_TYPES, 'kinesis'),
    hideInMore: true,
    info: (
      <>
        Length of time in milliseconds to wait for space to become available in the buffer before
        timing out.
      </>
    ),
  },
  {
    name: 'spec.tuningConfig.recordBufferFullWait',
    hideInMore: true,
    type: 'number',
    defaultValue: 5000,
    defined: typeIsKnown(KNOWN_TYPES, 'kinesis'),
    info: (
      <>
        Length of time in milliseconds to wait for the buffer to drain before attempting to fetch
        records from Kinesis again.
      </>
    ),
  },
  {
    name: 'spec.tuningConfig.fetchThreads',
    type: 'number',
    placeholder: 'max(1, {numProcessors} - 1)',
    defined: typeIsKnown(KNOWN_TYPES, 'kinesis'),
    hideInMore: true,
    info: (
      <>
        Size of the pool of threads fetching data from Kinesis. There is no benefit in having more
        threads than Kinesis shards.
      </>
    ),
  },
  {
    name: 'spec.tuningConfig.maxBytesPerPoll',
    type: 'number',
    defaultValue: 1000000,
    defined: typeIsKnown(KNOWN_TYPES, 'kinesis'),
    hideInMore: true,
    info: (
      <>
        The maximum number of bytes to be fetched from buffer per poll. At least one record will be
        fetched regardless of config.
      </>
    ),
  },
  {
    name: 'spec.tuningConfig.repartitionTransitionDuration',
    type: 'duration',
    defaultValue: 'PT2M',
    defined: typeIsKnown(KNOWN_TYPES, 'kinesis'),
    hideInMore: true,
    info: (
      <>
        <p>
          When shards are split or merged, the supervisor will recompute shard, task group mappings,
          and signal any running tasks created under the old mappings to stop early at{' '}
          <Code>(current time + repartitionTransitionDuration)</Code>. Stopping the tasks early
          allows Druid to begin reading from the new shards more quickly.
        </p>
        <p>
          The repartition transition wait time controlled by this property gives the stream
          additional time to write records to the new shards after the split/merge, which helps
          avoid the issues with empty shard handling described at
          <ExternalLink href="https://github.com/apache/druid/issues/7600">#7600</ExternalLink>.
        </p>
      </>
    ),
  },
];

export function getTuningFormFields() {
  return TUNING_FORM_FIELDS;
}

// --------------

export function updateIngestionType(
  spec: Partial<IngestionSpec>,
  comboType: IngestionComboType,
): Partial<IngestionSpec> {
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
  }

  return newSpec;
}

function findValueWithNewline(rows: string[][]): string | undefined {
  return findMap(rows, row => findMap(row, value => (value.includes('\n') ? value : undefined)));
}

export function issueWithSampleData(
  sampleLines: string[],
  isStreaming: boolean,
): JSX.Element | undefined {
  if (!sampleLines.length) return;

  const firstLine = sampleLines[0];
  if (!isStreaming) {
    if (firstLine === '{') {
      return (
        <>
          This data looks like a multi-line formatted JSON object. For Druid to parse a text file,
          it must have one row per event. Consider reformatting your data as{' '}
          <ExternalLink href="https://jsonlines.org">JSON Lines</ExternalLink>.
        </>
      );
    }

    if (oneOf(firstLine, '[', '[]')) {
      return (
        <>
          This data looks like a multi-line JSON array. For Druid to parse a text file, it must have
          one row per event. Consider reformatting your data as{' '}
          <ExternalLink href="https://jsonlines.org">JSON Lines</ExternalLink>.
        </>
      );
    }
  }

  const format = guessSimpleInputFormat(sampleLines, isStreaming);
  const text = sampleLines.join('\n');
  if (oneOf(format.type, 'csv', 'tsv')) {
    const valueWithNewline = findValueWithNewline(
      format.type === 'csv' ? csvParseRows(text) : tsvParseRows(text),
    );
    if (valueWithNewline) {
      const formatLabel = format.type.toUpperCase();
      return (
        <>
          {`This ${formatLabel} data has values that contain new lines. Druid requires ${formatLabel} files to have one event per line, so ${formatLabel} values can not contain new lines. Consider encoding new lines in the values of your ${formatLabel} with some special delimiter.`}
        </>
      );
    }
  }

  return;
}

export function fillInputFormatIfNeeded(
  spec: Partial<IngestionSpec>,
  sampleResponse: SampleResponse,
): Partial<IngestionSpec> {
  if (deepGet(spec, 'spec.ioConfig.inputFormat.type')) return spec;

  return deepSet(
    spec,
    'spec.ioConfig.inputFormat',
    getSpecType(spec) === 'kafka'
      ? guessKafkaInputFormat(filterMap(sampleResponse.data, l => l.input))
      : guessSimpleInputFormat(
          filterMap(sampleResponse.data, l => l.input?.raw),
          isStreamingSpec(spec),
        ),
  );
}

function noNumbers(xs: string[]): boolean {
  return xs.every(x => isNaN(Number(x)));
}

export function guessKafkaInputFormat(sampleRaw: Record<string, any>[]): InputFormat {
  const hasHeader = sampleRaw.some(x => Object.keys(x).some(k => k.startsWith('kafka.header.')));
  const keys = filterMap(sampleRaw, x => x['kafka.key']);
  const payloads = filterMap(sampleRaw, x => x.raw);
  return {
    type: 'kafka',
    headerFormat: hasHeader ? { type: 'string' } : undefined,
    keyFormat: keys.length ? guessSimpleInputFormat(keys, true) : undefined,
    valueFormat: guessSimpleInputFormat(payloads, true),
  };
}

export function guessSimpleInputFormat(
  sampleRaw: string[],
  canBeMultiLineJson = false,
): InputFormat {
  let sampleDatum = sampleRaw[0];
  if (sampleDatum) {
    sampleDatum = String(sampleDatum); // Really ensure it is a string

    // First check for magic byte sequences as they rarely yield false positives

    // Parquet 4 byte magic header: https://github.com/apache/parquet-format#file-format
    if (sampleDatum.startsWith('PAR1')) {
      return inputFormatFromType({ type: 'parquet' });
    }
    // ORC 3 byte magic header: https://orc.apache.org/specification/ORCv1/
    if (sampleDatum.startsWith('ORC')) {
      return inputFormatFromType({ type: 'orc' });
    }
    // Avro OCF 4 byte magic header: https://avro.apache.org/docs/current/spec.html#Object+Container+Files
    if (sampleDatum.startsWith('Obj\x01')) {
      return inputFormatFromType({ type: 'avro_ocf' });
    }

    // After checking for magic byte sequences perform heuristics to deduce string formats

    // If the string starts and ends with curly braces assume JSON
    if (sampleDatum.startsWith('{') && sampleDatum.endsWith('}')) {
      try {
        JSON.parse(sampleDatum);
        return { type: 'json' };
      } catch {
        // If the standard JSON parse does not parse then try setting a very lax parsing style
        return {
          type: 'json',
          featureSpec: {
            ALLOW_COMMENTS: true,
            ALLOW_YAML_COMMENTS: true,
            ALLOW_UNQUOTED_FIELD_NAMES: true,
            ALLOW_SINGLE_QUOTES: true,
            ALLOW_UNQUOTED_CONTROL_CHARS: true,
            ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER: true,
            ALLOW_NUMERIC_LEADING_ZEROS: true,
            ALLOW_NON_NUMERIC_NUMBERS: true,
            ALLOW_MISSING_VALUES: true,
            ALLOW_TRAILING_COMMA: true,
          },
        };
      }
    }

    // Contains more than 3 tabs assume TSV
    const lineAsTsv = sampleDatum.split('\t');
    if (lineAsTsv.length > 3) {
      return inputFormatFromType({
        type: 'tsv',
        findColumnsFromHeader: noNumbers(lineAsTsv),
        numColumns: lineAsTsv.length,
      });
    }

    // Contains more than fields if parsed as CSV line
    const lineAsCsv = parseCsvLine(sampleDatum);
    if (lineAsCsv.length > 3) {
      return inputFormatFromType({
        type: 'csv',
        findColumnsFromHeader: noNumbers(lineAsCsv),
        numColumns: lineAsCsv.length,
      });
    }

    // Contains more than 3 semicolons assume semicolon separated
    const lineAsTsvSemicolon = sampleDatum.split(';');
    if (lineAsTsvSemicolon.length > 3) {
      return inputFormatFromType({
        type: 'tsv',
        delimiter: ';',
        findColumnsFromHeader: noNumbers(lineAsTsvSemicolon),
        numColumns: lineAsTsvSemicolon.length,
      });
    }

    // Contains more than 3 pipes assume pipe separated
    const lineAsTsvPipe = sampleDatum.split('|');
    if (lineAsTsvPipe.length > 3) {
      return inputFormatFromType({
        type: 'tsv',
        delimiter: '|',
        findColumnsFromHeader: noNumbers(lineAsTsvPipe),
        numColumns: lineAsTsvPipe.length,
      });
    }

    // If the object is a single json object spanning multiple lines than the first one will just start with `{`
    if (canBeMultiLineJson && sampleDatum.startsWith('{')) {
      return { type: 'json', useJsonNodeReader: true };
    }
  }

  return inputFormatFromType({ type: 'regex' });
}

interface InputFormatFromTypeOptions {
  type: string;
  delimiter?: string;
  findColumnsFromHeader?: boolean;
  numColumns?: number;
}

function inputFormatFromType(options: InputFormatFromTypeOptions): InputFormat {
  const { type, delimiter, findColumnsFromHeader, numColumns } = options;

  let inputFormat: InputFormat = { type };

  if (type === 'regex') {
    inputFormat = deepSet(inputFormat, 'pattern', '([\\s\\S]*)');
    inputFormat = deepSet(inputFormat, 'columns', ['line']);
  } else {
    if (typeof findColumnsFromHeader === 'boolean') {
      inputFormat = deepSet(inputFormat, 'findColumnsFromHeader', findColumnsFromHeader);

      if (!findColumnsFromHeader && numColumns) {
        const padLength = String(numColumns).length;
        inputFormat = deepSet(
          inputFormat,
          'columns',
          range(0, numColumns).map(c => `column${String(c + 1).padStart(padLength, '0')}`),
        );
      }
    }

    if (delimiter) {
      inputFormat = deepSet(inputFormat, 'delimiter', delimiter);
    }
  }

  return inputFormat;
}

// ------------------------

function checkArray(array: any[], checkFn: (x: any) => boolean): boolean {
  return array.every(as => checkFn(as) || (Array.isArray(as) && as.every(checkFn)));
}

function isIntegerOrNull(x: any): boolean {
  return x == null || (typeof x === 'number' && Number.isInteger(x));
}

function isIntegerOrNullAcceptString(x: any): boolean {
  return (
    x == null || ((typeof x === 'number' || typeof x === 'string') && Number.isInteger(Number(x)))
  );
}

function isNumberOrNull(x: any): boolean {
  return x == null || (typeof x === 'number' && !isNaN(x));
}

function isNumberOrNullAcceptString(x: any): boolean {
  return x == null || ((typeof x === 'number' || typeof x === 'string') && !isNaN(Number(x)));
}

export function guessColumnTypeFromInput(
  sampleValues: any[],
  guessNumericStringsAsNumbers: boolean,
): string {
  const definedValues = sampleValues.filter(v => v != null);

  // If we have no usable sample, assume string
  if (!definedValues.length) return 'string';

  // If we see any arrays in the input this is a multi-value dimension that must be a string
  if (definedValues.some(v => isSimpleArray(v))) {
    if (guessNumericStringsAsNumbers) {
      if (checkArray(definedValues, isIntegerOrNullAcceptString)) {
        return 'ARRAY<long>';
      }

      if (checkArray(definedValues, isNumberOrNullAcceptString)) {
        return 'ARRAY<double>';
      }
    } else {
      if (checkArray(definedValues, isIntegerOrNull)) {
        return 'ARRAY<long>';
      }

      if (checkArray(definedValues, isNumberOrNull)) {
        return 'ARRAY<double>';
      }
    }

    return 'ARRAY<string>';
  }

  // If we see any JSON objects in the input assume COMPLEX<json>
  if (definedValues.some(v => v && typeof v === 'object')) return 'COMPLEX<json>';

  if (guessNumericStringsAsNumbers) {
    if (definedValues.every(isIntegerOrNullAcceptString)) {
      return 'long';
    }

    if (definedValues.every(isNumberOrNullAcceptString)) {
      return 'double';
    }
  } else {
    if (definedValues.every(isIntegerOrNull)) {
      return 'long';
    }

    if (definedValues.every(isNumberOrNull)) {
      return 'double';
    }
  }

  return 'string';
}

export function guessColumnTypeFromSampleResponse(
  sampleResponse: SampleResponse,
  column: string,
  guessNumericStringsAsNumbers: boolean,
): string {
  return guessColumnTypeFromInput(
    filterMap(sampleResponse.data, r => r.input?.[column]),
    guessNumericStringsAsNumbers,
  );
}

export function inputFormatOutputsNumericStrings(inputFormat: InputFormat | undefined): boolean {
  return oneOf(inputFormat?.type, 'csv', 'tsv', 'regex');
}

function getColumnTypeHintsFromSpec(spec: Partial<IngestionSpec>): Record<string, string> {
  const columnTypeHints: Record<string, string> = {};
  const currentDimensions = deepGet(spec, 'spec.dataSchema.dimensionsSpec.dimensions') || [];
  for (const currentDimension of currentDimensions) {
    columnTypeHints[getDimensionSpecName(currentDimension)] =
      getDimensionSpecColumnType(currentDimension);
  }

  const currentMetrics = deepGet(spec, 'spec.dataSchema.metricsSpec') || [];
  for (const currentMetric of currentMetrics) {
    const singleFieldName = getMetricSpecSingleFieldName(currentMetric);
    const metricOutputType = getMetricSpecOutputType(currentMetric);
    if (singleFieldName && metricOutputType) {
      columnTypeHints[singleFieldName] = metricOutputType;
    }
  }

  return columnTypeHints;
}

export function updateSchemaWithSample(
  spec: Partial<IngestionSpec>,
  sampleResponse: SampleResponse,
  schemaMode: SchemaMode,
  arrayMode: ArrayMode,
  rollup: boolean,
  forcePartitionInitialization = false,
): Partial<IngestionSpec> {
  const columnTypeHints = getColumnTypeHintsFromSpec(spec);
  const guessNumericStringsAsNumbers = inputFormatOutputsNumericStrings(
    deepGet(spec, 'spec.ioConfig.inputFormat'),
  );

  let newSpec = spec;

  switch (schemaMode) {
    case 'type-aware-discovery':
      newSpec = deepSet(newSpec, 'spec.dataSchema.dimensionsSpec.useSchemaDiscovery', true);
      newSpec = deepDelete(newSpec, 'spec.dataSchema.dimensionsSpec.includeAllDimensions');
      newSpec = deepSet(newSpec, 'spec.dataSchema.dimensionsSpec.dimensionExclusions', []);
      newSpec = deepDelete(newSpec, 'spec.dataSchema.dimensionsSpec.dimensions');
      break;

    case 'string-only-discovery':
      newSpec = deepDelete(newSpec, 'spec.dataSchema.dimensionsSpec.useSchemaDiscovery');
      newSpec = deepDelete(newSpec, 'spec.dataSchema.dimensionsSpec.includeAllDimensions');
      newSpec = deepSet(newSpec, 'spec.dataSchema.dimensionsSpec.dimensionExclusions', []);
      newSpec = deepDelete(newSpec, 'spec.dataSchema.dimensionsSpec.dimensions');
      break;

    case 'fixed':
      newSpec = deepDelete(newSpec, 'spec.dataSchema.dimensionsSpec.useSchemaDiscovery');
      newSpec = deepDelete(newSpec, 'spec.dataSchema.dimensionsSpec.includeAllDimensions');
      newSpec = deepDelete(newSpec, 'spec.dataSchema.dimensionsSpec.dimensionExclusions');
      newSpec = deepSet(
        newSpec,
        'spec.dataSchema.dimensionsSpec.dimensions',
        getDimensionSpecs(
          sampleResponse,
          columnTypeHints,
          guessNumericStringsAsNumbers,
          arrayMode === 'multi-values',
          rollup,
        ),
      );
      break;
  }

  if (rollup) {
    newSpec = deepSet(newSpec, 'spec.dataSchema.granularitySpec.queryGranularity', 'hour');

    const metrics = getMetricSpecs(sampleResponse, columnTypeHints, guessNumericStringsAsNumbers);
    if (metrics) {
      newSpec = deepSet(newSpec, 'spec.dataSchema.metricsSpec', metrics);
    }
  } else {
    newSpec = deepSet(newSpec, 'spec.dataSchema.granularitySpec.queryGranularity', 'none');
    newSpec = deepDelete(newSpec, 'spec.dataSchema.metricsSpec');
  }

  if (
    getSpecType(newSpec) === 'index_parallel' &&
    (!deepGet(newSpec, 'spec.tuningConfig.partitionsSpec') || forcePartitionInitialization)
  ) {
    newSpec = adjustForceGuaranteedRollup(
      deepSet(
        newSpec,
        'spec.tuningConfig.partitionsSpec',
        rollup ? { type: 'hashed' } : { type: 'dynamic' },
      ),
    );
  }

  newSpec = deepSet(newSpec, 'spec.dataSchema.granularitySpec.rollup', rollup);
  return newSpec;
}

export function adjustId(id: string): string {
  return id
    .replace(/\//g, '') // Can not have /
    .replace(/^\./, '') // Can not have leading .
    .replace(/\s+/gm, ' '); // Can not have whitespaces other than space
}
