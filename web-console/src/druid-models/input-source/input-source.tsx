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

import React from 'react';

import type { Field } from '../../components';
import { ExternalLink } from '../../components';
import { getLink } from '../../links';
import { deepGet, deepSet, nonEmptyArray, typeIsKnown } from '../../utils';

export const FILTER_SUGGESTIONS: string[] = [
  '*',
  '*.jsonl',
  '*.jsonl.gz',
  '*.json',
  '*.json.gz',
  '*.csv',
  '*.tsv',
  '*.parquet',
  '*.orc',
  '*.avro',
];

export interface InputSource {
  type: string;
  baseDir?: string;
  filter?: any;
  uris?: string[];
  prefixes?: string[];
  objects?: { bucket: string; path: string }[];
  fetchTimeout?: number;
  systemFields?: string[];

  // druid
  dataSource?: string;
  interval?: string;
  dimensions?: string[];
  metrics?: string[];
  maxInputSegmentBytesPerTask?: number;

  // inline
  data?: string;

  // delta
  tablePath?: string;

  // hdfs
  paths?: string | string[];

  // http
  httpAuthenticationUsername?: any;
  httpAuthenticationPassword?: any;
}

export type InputSourceDesc =
  | {
      type: 'inline';
      data: string;
    }
  | {
      type: 'local';
      filter?: any;
      baseDir?: string;
      files?: string[];
    }
  | {
      type: 'druid';
      dataSource: string;
      interval: string;
      filter?: any;
      dimensions?: string[]; // ToDo: these are not in the docs https://druid.apache.org/docs/latest/ingestion/input-sources.html
      metrics?: string[];
      maxInputSegmentBytesPerTask?: number;
    }
  | {
      type: 'http';
      uris: string[];
      httpAuthenticationUsername?: any;
      httpAuthenticationPassword?: any;
    }
  | {
      type: 's3';
      uris?: string[];
      prefixes?: string[];
      objects?: { bucket: string; path: string }[];
      properties?: {
        accessKeyId?: any;
        secretAccessKey?: any;
        assumeRoleArn?: any;
        assumeRoleExternalId?: any;
      };
    }
  | {
      type: 'google' | 'azureStorage';
      uris?: string[];
      prefixes?: string[];
      objects?: { bucket: string; path: string }[];
    }
  | {
      type: 'hdfs';
      paths?: string | string[];
    }
  | {
      type: 'delta';
      tablePath?: string;
    }
  | {
      type: 'sql';
      database: any;
      foldCase?: boolean;
      sqls: string[];
    }
  | {
      type: 'combining';
      delegates: InputSource[];
    };

export function issueWithInputSource(inputSource: InputSource | undefined): string | undefined {
  if (!inputSource) return 'does not exist';
  if (!inputSource.type) return 'missing a type';
  switch (inputSource.type) {
    case 'local':
      if (!inputSource.baseDir) return `must have a 'baseDir'`;
      if (!inputSource.filter) return `must have a 'filter'`;
      return;

    case 'http':
      if (!nonEmptyArray(inputSource.uris)) {
        return 'must have at least one uri';
      }
      return;

    case 'druid':
      if (!inputSource.dataSource) return `must have a 'dataSource'`;
      if (!inputSource.interval) return `must have an 'interval'`;
      return;

    case 'inline':
      if (!inputSource.data) return `must have 'data'`;
      return;

    case 's3':
    case 'azureStorage':
    case 'google':
      if (
        !nonEmptyArray(inputSource.uris) &&
        !nonEmptyArray(inputSource.prefixes) &&
        !nonEmptyArray(inputSource.objects)
      ) {
        return 'must have at least one uri or prefix or object';
      }
      return;

    case 'delta':
      if (!inputSource.tablePath) {
        return 'must have tablePath';
      }
      return;

    case 'hdfs':
      if (!inputSource.paths) {
        return 'must have paths';
      }
      return;

    default:
      return;
  }
}

const KNOWN_TYPES = [
  'inline',
  'druid',
  'http',
  'local',
  's3',
  'azureStorage',
  'delta',
  'google',
  'hdfs',
  'sql',
];
export const INPUT_SOURCE_FIELDS: Field<InputSource>[] = [
  // inline

  {
    name: 'data',
    label: 'Inline data',
    type: 'string',
    defined: typeIsKnown(KNOWN_TYPES, 'inline'),
    required: true,
    placeholder: 'Paste your data here',
    multiline: true,
    height: '400px',
    info: <p>Put you inline data here</p>,
  },

  // http

  {
    name: 'uris',
    label: 'URIs',
    type: 'string-array',
    placeholder: 'https://example.com/path/to/file1.ext, https://example.com/path/to/file2.ext',
    defined: typeIsKnown(KNOWN_TYPES, 'http'),
    required: true,
    info: (
      <p>
        The full URI of your file. To ingest from multiple URIs, use commas to separate each
        individual URI.
      </p>
    ),
  },
  {
    name: 'httpAuthenticationUsername',
    label: 'HTTP auth username',
    type: 'string',
    defined: typeIsKnown(KNOWN_TYPES, 'http'),
    placeholder: '(optional)',
    info: <p>Username to use for authentication with specified URIs</p>,
  },
  {
    name: 'httpAuthenticationPassword',
    label: 'HTTP auth password',
    type: 'string',
    defined: typeIsKnown(KNOWN_TYPES, 'http'),
    placeholder: '(optional)',
    info: <p>Password to use for authentication with specified URIs</p>,
  },

  // local

  {
    name: 'baseDir',
    label: 'Base directory',
    type: 'string',
    placeholder: '/path/to/files/',
    defined: typeIsKnown(KNOWN_TYPES, 'local'),
    required: true,
    info: (
      <>
        <ExternalLink href={`${getLink('DOCS')}/ingestion/native-batch.html#input-sources`}>
          baseDir
        </ExternalLink>
        <p>Specifies the directory to search recursively for files to be ingested.</p>
      </>
    ),
  },
  {
    name: 'filter',
    label: 'File filter',
    type: 'string',
    defined: typeIsKnown(KNOWN_TYPES, 'local'),
    required: true,
    suggestions: FILTER_SUGGESTIONS,
    info: (
      <>
        <ExternalLink href={`${getLink('DOCS')}/ingestion/native-batch.html#local-input-source`}>
          filter
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

  // s3

  {
    name: 'uris',
    label: 'S3 URIs',
    type: 'string-array',
    placeholder: 's3://your-bucket/some-file1.ext, s3://your-bucket/some-file2.ext',
    defined: inputSource =>
      inputSource.type === 's3' &&
      !deepGet(inputSource, 'prefixes') &&
      !deepGet(inputSource, 'objects'),
    required: true,
    info: (
      <>
        <p>
          The full S3 URI of your file. To ingest from multiple URIs, use commas to separate each
          individual URI.
        </p>
        <p>Either S3 URIs or prefixes or objects must be set.</p>
      </>
    ),
  },
  {
    name: 'prefixes',
    label: 'S3 prefixes',
    type: 'string-array',
    placeholder: 's3://your-bucket/some-path1, s3://your-bucket/some-path2',
    defined: inputSource =>
      inputSource.type === 's3' &&
      !deepGet(inputSource, 'uris') &&
      !deepGet(inputSource, 'objects'),
    required: true,
    info: (
      <>
        <p>A list of paths (with bucket) where your files are stored.</p>
        <p>Either S3 URIs or prefixes or objects must be set.</p>
      </>
    ),
  },
  {
    name: 'objects',
    label: 'S3 objects',
    type: 'json',
    placeholder: '{"bucket":"your-bucket", "path":"some-file.ext"}',
    defined: inputSource => inputSource.type === 's3' && deepGet(inputSource, 'objects'),
    required: true,
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

  // azure

  {
    name: 'uris',
    label: 'Azure URIs',
    type: 'string-array',
    placeholder:
      'azureStorage://your-storage-account/your-container/some-file1.ext, azureStorage://your-storage-account/your-container/some-file2.ext',
    defined: inputSource =>
      inputSource.type === 'azureStorage' &&
      !deepGet(inputSource, 'prefixes') &&
      !deepGet(inputSource, 'objects'),
    required: true,
    info: (
      <>
        <p>
          The full Azure URI of your file. To ingest from multiple URIs, use commas to separate each
          individual URI.
        </p>
        <p>Either Azure URIs or prefixes or objects must be set.</p>
      </>
    ),
  },
  {
    name: 'prefixes',
    label: 'Azure prefixes',
    type: 'string-array',
    placeholder:
      'azureStorage://your-storage-account/your-container/some-path1, azureStorage://your-storage-account/your-container/some-path2',
    defined: inputSource =>
      inputSource.type === 'azureStorage' &&
      !deepGet(inputSource, 'uris') &&
      !deepGet(inputSource, 'objects'),
    required: true,
    info: (
      <>
        <p>A list of paths (with bucket) where your files are stored.</p>
        <p>Either Azure URIs or prefixes or objects must be set.</p>
      </>
    ),
  },
  {
    name: 'objects',
    label: 'Azure objects',
    type: 'json',
    placeholder: '{"bucket":"your-storage-account", "path":"your-container/some-file.ext"}',
    defined: inputSource => inputSource.type === 'azureStorage' && deepGet(inputSource, 'objects'),
    required: true,
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
  {
    name: 'properties.sharedAccessStorageToken',
    label: 'Shared Access Storage Token',
    type: 'string',
    placeholder: '(sas token)',
    defined: inputSource => inputSource.type === 'azureStorage',
    info: (
      <>
        <p>Shared Access Storage Token for this storage account.</p>
        <p>
          Note: Inlining the sas token into the ingestion spec can be dangerous as it might appear
          in server log files and can be seen by anyone accessing this console.
        </p>
      </>
    ),
  },
  // google
  {
    name: 'uris',
    label: 'Google Cloud Storage URIs',
    type: 'string-array',
    placeholder: 'gs://your-bucket/some-file1.ext, gs://your-bucket/some-file2.ext',
    defined: inputSource =>
      inputSource.type === 'google' &&
      !deepGet(inputSource, 'prefixes') &&
      !deepGet(inputSource, 'objects'),
    required: true,
    info: (
      <>
        <p>
          The full Google Cloud Storage URI of your file. To ingest from multiple URIs, use commas
          to separate each individual URI.
        </p>
        <p>Either Google Cloud Storage URIs or prefixes or objects must be set.</p>
      </>
    ),
  },
  {
    name: 'prefixes',
    label: 'Google Cloud Storage prefixes',
    type: 'string-array',
    placeholder: 'gs://your-bucket/some-path1, gs://your-bucket/some-path2',
    defined: inputSource =>
      inputSource.type === 'google' &&
      !deepGet(inputSource, 'uris') &&
      !deepGet(inputSource, 'objects'),
    required: true,
    info: (
      <>
        <p>A list of paths (with bucket) where your files are stored.</p>
        <p>Either Google Cloud Storage URIs or prefixes or objects must be set.</p>
      </>
    ),
  },
  {
    name: 'objects',
    label: 'Google Cloud Storage objects',
    type: 'json',
    placeholder: '{"bucket":"your-bucket", "path":"some-file.ext"}',
    defined: inputSource => inputSource.type === 'google' && deepGet(inputSource, 'objects'),
    required: true,
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

  // Cloud common
  {
    name: 'filter',
    label: 'File filter',
    type: 'string',
    suggestions: FILTER_SUGGESTIONS,
    placeholder: '*',
    defined: typeIsKnown(KNOWN_TYPES, 's3', 'azureStorage', 'google'),
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
  },

  // S3 auth extra
  {
    name: 'properties.accessKeyId.type',
    label: 'Access key ID type',
    type: 'string',
    suggestions: [undefined, 'environment', 'default'],
    placeholder: '(none)',
    defined: typeIsKnown(KNOWN_TYPES, 's3'),
    info: (
      <>
        <p>S3 access key type.</p>
        <p>Setting this will override the default configuration provided in the config.</p>
        <p>
          The access key can be pulled from an environment variable or inlined in the ingestion spec
          (default).
        </p>
        <p>
          Note: Inlining the access key into the ingestion spec is dangerous as it might appear in
          server log files and can be seen by anyone accessing this console.
        </p>
      </>
    ),
    adjustment: inputSource => {
      return deepSet(
        inputSource,
        'properties.secretAccessKey.type',
        deepGet(inputSource, 'properties.accessKeyId.type'),
      );
    },
  },
  {
    name: 'properties.accessKeyId.variable',
    label: 'Access key ID environment variable',
    type: 'string',
    placeholder: '(environment variable name)',
    defined: inputSource =>
      inputSource.type === 's3' &&
      deepGet(inputSource, 'properties.accessKeyId.type') === 'environment',
    info: <p>The environment variable containing the S3 access key for this S3 bucket.</p>,
  },
  {
    name: 'properties.accessKeyId.password',
    label: 'Access key ID value',
    type: 'string',
    placeholder: '(access key)',
    defined: inputSource =>
      inputSource.type === 's3' &&
      deepGet(inputSource, 'properties.accessKeyId.type') === 'default',
    info: (
      <>
        <p>S3 access key for this S3 bucket.</p>
        <p>
          Note: Inlining the access key into the ingestion spec is dangerous as it might appear in
          server log files and can be seen by anyone accessing this console.
        </p>
      </>
    ),
  },
  {
    name: 'properties.secretAccessKey.type',
    label: 'Secret access key type',
    type: 'string',
    suggestions: [undefined, 'environment', 'default'],
    placeholder: '(none)',
    defined: typeIsKnown(KNOWN_TYPES, 's3'),
    info: (
      <>
        <p>S3 secret key type.</p>
        <p>Setting this will override the default configuration provided in the config.</p>
        <p>
          The secret key can be pulled from an environment variable or inlined in the ingestion spec
          (default).
        </p>
        <p>
          Note: Inlining the secret key into the ingestion spec is dangerous as it might appear in
          server log files and can be seen by anyone accessing this console.
        </p>
      </>
    ),
  },
  {
    name: 'properties.secretAccessKey.variable',
    label: 'Secret access key environment variable',
    type: 'string',
    placeholder: '(environment variable name)',
    defined: inputSource =>
      deepGet(inputSource, 'properties.secretAccessKey.type') === 'environment',
    info: <p>The environment variable containing the S3 secret key for this S3 bucket.</p>,
  },
  {
    name: 'properties.secretAccessKey.password',
    label: 'Secret access key value',
    type: 'string',
    placeholder: '(secret key)',
    defined: inputSource => deepGet(inputSource, 'properties.secretAccessKey.type') === 'default',
    info: (
      <>
        <p>S3 secret key for this S3 bucket.</p>
        <p>
          Note: Inlining the access key into the ingestion spec is dangerous as it might appear in
          server log files and can be seen by anyone accessing this console.
        </p>
      </>
    ),
  },

  // hdfs
  {
    name: 'paths',
    label: 'Paths',
    type: 'string',
    placeholder: '/path/to/file.ext',
    defined: typeIsKnown(KNOWN_TYPES, 'hdfs'),
    required: true,
  },

  // delta lake
  {
    name: 'tablePath',
    label: 'Delta table path',
    type: 'string',
    placeholder: '/path/to/deltaTable',
    defined: typeIsKnown(KNOWN_TYPES, 'delta'),
    required: true,
  },

  // sql
  {
    name: 'database.type',
    label: 'Database type',
    type: 'string',
    suggestions: ['mysql', 'postgresql'],
    defined: typeIsKnown(KNOWN_TYPES, 'sql'),
    required: true,
    info: (
      <>
        <p>
          The full Google Cloud Storage URI of your file. To ingest from multiple URIs, use commas
          to separate each individual URI.
        </p>
        <p>Either Google Cloud Storage URIs or prefixes or objects must be set.</p>
      </>
    ),
  },
];
