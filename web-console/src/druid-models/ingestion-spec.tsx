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

import { ExternalLink, Field } from '../components';
import { getLink } from '../links';
import {
  deepDelete,
  deepGet,
  deepMove,
  deepSet,
  EMPTY_ARRAY,
  EMPTY_OBJECT,
  filterMap,
  oneOf,
} from '../utils';
import { HeaderAndRows } from '../utils/sampler';

import {
  DimensionsSpec,
  getDimensionSpecName,
  getDimensionSpecs,
  getDimensionSpecType,
} from './dimension-spec';
import { InputFormat, issueWithInputFormat } from './input-format';
import { InputSource, issueWithInputSource } from './input-source';
import {
  getMetricSpecOutputType,
  getMetricSpecs,
  getMetricSpecSingleFieldName,
  MetricSpec,
} from './metric-spec';
import { TimestampSpec } from './timestamp-spec';
import { TransformSpec } from './transform-spec';

export const MAX_INLINE_DATA_LENGTH = 65536;

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

export function getIngestionDocLink(spec: IngestionSpec): string {
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
    oneOf(type, 'index', 'compact', 'kill', 'append', 'merge', 'same_interval_merge')
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
    suggestions: ['local', 'http', 'inline', 's3', 'azure', 'google', 'hdfs'],
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
          hideInMore: true,
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
          hideInMore: true,
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

  const partitionsSpecType = deepGet(tuningConfig, 'partitionsSpec.type') || 'dynamic';
  if (partitionsSpecType === 'dynamic') {
    tuningConfig = deepDelete(tuningConfig, 'forceGuaranteedRollup');
  } else if (oneOf(partitionsSpecType, 'hashed', 'single_dim')) {
    tuningConfig = deepSet(tuningConfig, 'forceGuaranteedRollup', true);
  }

  return tuningConfig;
}

export function invalidTuningConfig(tuningConfig: TuningConfig, intervals: any): boolean {
  if (tuningConfig.type !== 'index_parallel') return false;

  switch (deepGet(tuningConfig, 'partitionsSpec.type')) {
    case 'hashed':
      if (!intervals) return true;
      return (
        Boolean(deepGet(tuningConfig, 'partitionsSpec.targetRowsPerSegment')) &&
        Boolean(deepGet(tuningConfig, 'partitionsSpec.numShards'))
      );

    case 'single_dim':
      if (!intervals) return true;
      if (!deepGet(tuningConfig, 'partitionsSpec.partitionDimension')) return true;
      const hasTargetRowsPerSegment = Boolean(
        deepGet(tuningConfig, 'partitionsSpec.targetRowsPerSegment'),
      );
      const hasMaxRowsPerSegment = Boolean(
        deepGet(tuningConfig, 'partitionsSpec.maxRowsPerSegment'),
      );
      if (hasTargetRowsPerSegment === hasMaxRowsPerSegment) {
        return true;
      }
  }

  return false;
}

export function getPartitionRelatedTuningSpecFormFields(
  specType: IngestionType,
  dimensionSuggestions: string[] | undefined,
): Field<TuningConfig>[] {
  switch (specType) {
    case 'index_parallel':
      return [
        {
          name: 'partitionsSpec.type',
          label: 'Partitioning type',
          type: 'string',
          required: true,
          suggestions: ['dynamic', 'hashed', 'single_dim'],
          info: (
            <p>
              For perfect rollup, you should use either <Code>hashed</Code> (partitioning based on
              the hash of dimensions in each row) or <Code>single_dim</Code> (based on ranges of a
              single dimension). For best-effort rollup, you should use <Code>dynamic</Code>.
            </p>
          ),
          adjustment: (t: TuningConfig) => {
            if (!Array.isArray(dimensionSuggestions) || !dimensionSuggestions.length) return t;
            return deepSet(t, 'partitionsSpec.partitionDimension', dimensionSuggestions[0]);
          },
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
          name: 'partitionsSpec.targetRowsPerSegment',
          label: 'Target rows per segment',
          type: 'number',
          zeroMeansUndefined: true,
          defaultValue: 5000000,
          defined: (t: TuningConfig) =>
            deepGet(t, 'partitionsSpec.type') === 'hashed' &&
            !deepGet(t, 'partitionsSpec.numShards'),
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
          name: 'partitionsSpec.numShards',
          label: 'Num shards',
          type: 'number',
          zeroMeansUndefined: true,
          hideInMore: true,
          defined: (t: TuningConfig) =>
            deepGet(t, 'partitionsSpec.type') === 'hashed' &&
            !deepGet(t, 'partitionsSpec.targetRowsPerSegment'),
          info: (
            <>
              <p>
                If you know the optimal number of shards and want to speed up the time it takes for
                compaction to run, set this field.
              </p>
              <p>
                Directly specify the number of shards to create. If this is specified and
                'intervals' is specified in the granularitySpec, the index task can skip the
                determine intervals/partitions pass through the data.
              </p>
            </>
          ),
        },
        {
          name: 'partitionsSpec.partitionDimensions',
          label: 'Partition dimensions',
          type: 'string-array',
          placeholder: '(all dimensions)',
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
          name: 'partitionsSpec.targetRowsPerSegment',
          label: 'Target rows per segment',
          type: 'number',
          zeroMeansUndefined: true,
          defined: (t: TuningConfig) =>
            deepGet(t, 'partitionsSpec.type') === 'single_dim' &&
            !deepGet(t, 'partitionsSpec.maxRowsPerSegment'),
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
          defined: (t: TuningConfig) =>
            deepGet(t, 'partitionsSpec.type') === 'single_dim' &&
            !deepGet(t, 'partitionsSpec.targetRowsPerSegment'),
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
    hideInMore: true,
    info: <>Maximum number of retries on task failures.</>,
  },
  {
    name: 'taskStatusCheckPeriodMs',
    type: 'number',
    defaultValue: 1000,
    defined: (t: TuningConfig) => t.type === 'index_parallel',
    hideInMore: true,
    info: <>Polling period in milliseconds to check running task statuses.</>,
  },
  {
    name: 'totalNumMergeTasks',
    type: 'number',
    defaultValue: 10,
    min: 1,
    defined: (t: TuningConfig) =>
      Boolean(
        t.type === 'index_parallel' &&
          oneOf(deepGet(t, 'partitionsSpec.type'), 'hashed', 'single_dim'),
      ),
    info: <>Number of tasks to merge partial segments after shuffle.</>,
  },
  {
    name: 'maxNumSegmentsToMerge',
    type: 'number',
    defaultValue: 100,
    defined: (t: TuningConfig) =>
      Boolean(
        t.type === 'index_parallel' &&
          oneOf(deepGet(t, 'partitionsSpec.type'), 'hashed', 'single_dim'),
      ),
    info: (
      <>
        Max limit for the number of segments a single task can merge at the same time after shuffle.
      </>
    ),
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
    name: 'resetOffsetAutomatically',
    type: 'boolean',
    defaultValue: false,
    defined: (t: TuningConfig) => oneOf(t.type, 'kafka', 'kinesis'),
    info: (
      <>
        Whether to reset the consumer offset if the next offset that it is trying to fetch is less
        than the earliest available offset for that particular partition.
      </>
    ),
  },
  {
    name: 'skipSequenceNumberAvailabilityCheck',
    type: 'boolean',
    defaultValue: false,
    defined: (t: TuningConfig) => t.type === 'kinesis',
    info: (
      <>
        Whether to enable checking if the current sequence number is still available in a particular
        Kinesis shard. If set to false, the indexing task will attempt to reset the current sequence
        number (or not), depending on the value of <Code>resetOffsetAutomatically</Code>.
      </>
    ),
  },
  {
    name: 'intermediatePersistPeriod',
    type: 'duration',
    defaultValue: 'PT10M',
    defined: (t: TuningConfig) => oneOf(t.type, 'kafka', 'kinesis'),
    info: <>The period that determines the rate at which intermediate persists occur.</>,
  },
  {
    name: 'intermediateHandoffPeriod',
    type: 'duration',
    defaultValue: 'P2147483647D',
    defined: (t: TuningConfig) => oneOf(t.type, 'kafka', 'kinesis'),
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
    name: 'pushTimeout',
    type: 'number',
    defaultValue: 0,
    hideInMore: true,
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
    defined: (t: TuningConfig) => oneOf(t.type, 'kafka', 'kinesis'),
    hideInMore: true,
    info: <>Milliseconds to wait for segment handoff. 0 means to wait forever.</>,
  },
  {
    name: 'indexSpec.bitmap.type',
    label: 'Index bitmap type',
    type: 'string',
    defaultValue: 'roaring',
    suggestions: ['concise', 'roaring'],
    hideInMore: true,
    info: <>Compression format for bitmap indexes.</>,
  },
  {
    name: 'indexSpec.dimensionCompression',
    label: 'Index dimension compression',
    type: 'string',
    defaultValue: 'lz4',
    suggestions: ['lz4', 'lzf', 'uncompressed'],
    hideInMore: true,
    info: <>Compression format for dimension columns.</>,
  },
  {
    name: 'indexSpec.metricCompression',
    label: 'Index metric compression',
    type: 'string',
    defaultValue: 'lz4',
    suggestions: ['lz4', 'lzf', 'uncompressed'],
    hideInMore: true,
    info: <>Compression format for primitive type metric columns.</>,
  },
  {
    name: 'indexSpec.longEncoding',
    label: 'Index long encoding',
    type: 'string',
    defaultValue: 'longs',
    suggestions: ['longs', 'auto'],
    hideInMore: true,
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
    hideInMore: true,
    info: <>Timeout for reporting the pushed segments in worker tasks.</>,
  },
  {
    name: 'chatHandlerNumRetries',
    type: 'number',
    defaultValue: 5,
    defined: (t: TuningConfig) => t.type === 'index_parallel',
    hideInMore: true,
    info: <>Retries for reporting the pushed segments in worker tasks.</>,
  },
  {
    name: 'workerThreads',
    type: 'number',
    placeholder: 'min(10, taskCount)',
    defined: (t: TuningConfig) => oneOf(t.type, 'kafka', 'kinesis'),
    info: (
      <>The number of threads that will be used by the supervisor for asynchronous operations.</>
    ),
  },
  {
    name: 'chatThreads',
    type: 'number',
    placeholder: 'min(10, taskCount * replicas)',
    defined: (t: TuningConfig) => oneOf(t.type, 'kafka', 'kinesis'),
    hideInMore: true,
    info: <>The number of threads that will be used for communicating with indexing tasks.</>,
  },
  {
    name: 'chatRetries',
    type: 'number',
    defaultValue: 8,
    defined: (t: TuningConfig) => oneOf(t.type, 'kafka', 'kinesis'),
    hideInMore: true,
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
    defined: (t: TuningConfig) => oneOf(t.type, 'kafka', 'kinesis'),
    info: <>How long to wait for a HTTP response from an indexing task.</>,
  },
  {
    name: 'shutdownTimeout',
    type: 'duration',
    defaultValue: 'PT80S',
    defined: (t: TuningConfig) => oneOf(t.type, 'kafka', 'kinesis'),
    hideInMore: true,
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
    hideInMore: true,
    info: (
      <>
        Length of time in milliseconds to wait for space to become available in the buffer before
        timing out.
      </>
    ),
  },
  {
    name: 'recordBufferFullWait',
    hideInMore: true,
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
    hideInMore: true,
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
    hideInMore: true,
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
    hideInMore: true,
    info: (
      <>
        The maximum number of records/events to be fetched from buffer per poll. The actual maximum
        will be <Code>max(maxRecordsPerPoll, max(bufferSize, 1))</Code>.
      </>
    ),
  },
  {
    name: 'repartitionTransitionDuration',
    type: 'duration',
    defaultValue: 'PT2M',
    defined: (t: TuningConfig) => t.type === 'kinesis',
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
  }

  return newSpec;
}

export function issueWithSampleData(sampleData: string[]): JSX.Element | undefined {
  if (sampleData.length) {
    const firstData = sampleData[0];

    if (firstData === '{') {
      return (
        <>
          This data looks like regular JSON object. For Druid to parse a text file it must have one
          row per event. Maybe look at{' '}
          <ExternalLink href="http://ndjson.org/">newline delimited JSON</ExternalLink> instead.
        </>
      );
    }

    if (oneOf(firstData, '[', '[]')) {
      return (
        <>
          This data looks like a multi-line JSON array. For Druid to parse a text file it must have
          one row per event. Maybe look at{' '}
          <ExternalLink href="http://ndjson.org/">newline delimited JSON</ExternalLink> instead.
        </>
      );
    }
  }

  return;
}

export function fillInputFormat(spec: IngestionSpec, sampleData: string[]): IngestionSpec {
  return deepSet(spec, 'spec.ioConfig.inputFormat', guessInputFormat(sampleData));
}

export function guessInputFormat(sampleData: string[]): InputFormat {
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
    if (sampleDatum.startsWith('Obj') && sampleDatum.charCodeAt(3) === 1) {
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

// ------------------------

export function guessTypeFromSample(sample: any[]): string {
  const definedValues = sample.filter(v => v != null);
  if (
    definedValues.length &&
    definedValues.every(v => !isNaN(v) && oneOf(typeof v, 'number', 'string'))
  ) {
    if (definedValues.every(v => v % 1 === 0)) {
      return 'long';
    } else {
      return 'double';
    }
  } else {
    return 'string';
  }
}

export function getColumnTypeFromHeaderAndRows(
  headerAndRows: HeaderAndRows,
  column: string,
): string {
  return guessTypeFromSample(
    filterMap(headerAndRows.rows, (r: any) => (r.parsed ? r.parsed[column] : undefined)),
  );
}

function getTypeHintsFromSpec(spec: IngestionSpec): Record<string, string> {
  const typeHints: Record<string, string> = {};
  const currentDimensions = deepGet(spec, 'spec.dataSchema.dimensionsSpec.dimensions') || [];
  for (const currentDimension of currentDimensions) {
    typeHints[getDimensionSpecName(currentDimension)] = getDimensionSpecType(currentDimension);
  }

  const currentMetrics = deepGet(spec, 'spec.dataSchema.metricsSpec') || [];
  for (const currentMetric of currentMetrics) {
    const singleFieldName = getMetricSpecSingleFieldName(currentMetric);
    const metricOutputType = getMetricSpecOutputType(currentMetric);
    if (singleFieldName && metricOutputType) {
      typeHints[singleFieldName] = metricOutputType;
    }
  }

  return typeHints;
}

export function updateSchemaWithSample(
  spec: IngestionSpec,
  headerAndRows: HeaderAndRows,
  dimensionMode: DimensionMode,
  rollup: boolean,
): IngestionSpec {
  const typeHints = getTypeHintsFromSpec(spec);

  let newSpec = spec;

  if (dimensionMode === 'auto-detect') {
    newSpec = deepDelete(newSpec, 'spec.dataSchema.dimensionsSpec.dimensions');
    newSpec = deepSet(newSpec, 'spec.dataSchema.dimensionsSpec.dimensionExclusions', []);
  } else {
    newSpec = deepDelete(newSpec, 'spec.dataSchema.dimensionsSpec.dimensionExclusions');

    const dimensions = getDimensionSpecs(headerAndRows, typeHints, rollup);
    if (dimensions) {
      newSpec = deepSet(newSpec, 'spec.dataSchema.dimensionsSpec.dimensions', dimensions);
    }
  }

  if (rollup) {
    newSpec = deepSet(newSpec, 'spec.dataSchema.granularitySpec.queryGranularity', 'hour');

    const metrics = getMetricSpecs(headerAndRows, typeHints);
    if (metrics) {
      newSpec = deepSet(newSpec, 'spec.dataSchema.metricsSpec', metrics);
    }
  } else {
    newSpec = deepSet(newSpec, 'spec.dataSchema.granularitySpec.queryGranularity', 'none');
    newSpec = deepDelete(newSpec, 'spec.dataSchema.metricsSpec');
  }

  newSpec = deepSet(newSpec, 'spec.dataSchema.granularitySpec.rollup', rollup);
  return newSpec;
}

// ------------------------

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
