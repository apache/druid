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

import * as JSONBig from 'json-bigint-native';

import {
  DimensionsSpec,
  getSpecType,
  getTimestampSchema,
  IngestionSpec,
  IngestionType,
  InputFormat,
  IoConfig,
  isDruidSource,
  MetricSpec,
  PLACEHOLDER_TIMESTAMP_SPEC,
  TimestampSpec,
  Transform,
  TransformSpec,
  upgradeSpec,
} from '../druid-models';
import { Api } from '../singletons';

import { getDruidErrorMessage, queryDruidRune } from './druid-query';
import {
  alphanumericCompare,
  EMPTY_ARRAY,
  filterMap,
  oneOf,
  sortWithPrefixSuffix,
} from './general';
import { deepGet, deepSet } from './object-change';

const SAMPLER_URL = `/druid/indexer/v1/sampler`;
const BASE_SAMPLER_CONFIG: SamplerConfig = {
  numRows: 500,
  timeoutMs: 15000,
};

export type SampleSpec = IngestionSpec & {
  samplerConfig: SamplerConfig;
};

export interface SamplerConfig {
  numRows?: number;
  timeoutMs?: number;
}

export interface SampleResponse {
  data: SampleEntry[];
}

export type CacheRows = Record<string, any>[];

export interface SampleResponseWithExtraInfo extends SampleResponse {
  queryGranularity?: any;
  rollup?: boolean;
  columns?: Record<string, any>;
  aggregators?: Record<string, any>;
}

export interface SampleEntry {
  input: Record<string, any>;
  parsed?: Record<string, any>;
  unparseable?: boolean;
  error?: string;
}

export interface HeaderAndRows {
  header: string[];
  rows: SampleEntry[];
}

export interface ExampleManifest {
  name: string;
  description: string;
  spec: any;
}

function dedupe(xs: string[]): string[] {
  const seen: Record<string, boolean> = {};
  return xs.filter(x => {
    if (seen[x]) {
      return false;
    } else {
      seen[x] = true;
      return true;
    }
  });
}

export function getCacheRowsFromSampleResponse(
  sampleResponse: SampleResponse,
  useParsed = false,
): CacheRows {
  const key = useParsed ? 'parsed' : 'input';
  return filterMap(sampleResponse.data, d => d[key]).slice(0, 20);
}

export function applyCache(sampleSpec: SampleSpec, cacheRows: CacheRows) {
  if (!cacheRows) return sampleSpec;

  // In order to prevent potential data loss null columns should be kept by the sampler and shown in the ingestion flow
  if (deepGet(sampleSpec, 'spec.ioConfig.inputFormat')) {
    sampleSpec = deepSet(sampleSpec, 'spec.ioConfig.inputFormat.keepNullColumns', true);
  }

  // If this is already an inline spec there is nothing to do
  if (deepGet(sampleSpec, 'spec.ioConfig.inputSource.type') === 'inline') return sampleSpec;

  // Make the spec into an inline json spec
  sampleSpec = deepSet(sampleSpec, 'type', 'index');
  sampleSpec = deepSet(sampleSpec, 'spec.type', 'index');
  sampleSpec = deepSet(sampleSpec, 'spec.ioConfig.type', 'index');
  sampleSpec = deepSet(sampleSpec, 'spec.ioConfig.inputSource', {
    type: 'inline',
    data: cacheRows.map(r => JSONBig.stringify(r)).join('\n'),
  });

  const flattenSpec = deepGet(sampleSpec, 'spec.ioConfig.inputFormat.flattenSpec');
  const inputFormat: InputFormat = { type: 'json', keepNullColumns: true };
  if (flattenSpec) inputFormat.flattenSpec = flattenSpec;
  sampleSpec = deepSet(sampleSpec, 'spec.ioConfig.inputFormat', inputFormat);

  return sampleSpec;
}

export interface HeaderFromSampleResponseOptions {
  sampleResponse: SampleResponse;
  ignoreTimeColumn?: boolean;
  columnOrder?: string[];
  suffixColumnOrder?: string[];
}

export function headerFromSampleResponse(options: HeaderFromSampleResponseOptions): string[] {
  const { sampleResponse, ignoreTimeColumn, columnOrder, suffixColumnOrder } = options;

  let columns = sortWithPrefixSuffix(
    dedupe(sampleResponse.data.flatMap(s => (s.parsed ? Object.keys(s.parsed) : []))).sort(),
    columnOrder || ['__time'],
    suffixColumnOrder || [],
    alphanumericCompare,
  );

  if (ignoreTimeColumn) {
    columns = columns.filter(c => c !== '__time');
  }

  return columns;
}

export interface HeaderAndRowsFromSampleResponseOptions extends HeaderFromSampleResponseOptions {
  parsedOnly?: boolean;
}

export function headerAndRowsFromSampleResponse(
  options: HeaderAndRowsFromSampleResponseOptions,
): HeaderAndRows {
  const { sampleResponse, parsedOnly } = options;

  return {
    header: headerFromSampleResponse(options),
    rows: parsedOnly ? sampleResponse.data.filter((d: any) => d.parsed) : sampleResponse.data,
  };
}

export async function getProxyOverlordModules(): Promise<string[]> {
  let statusResp: any;
  try {
    statusResp = await Api.instance.get(`/proxy/overlord/status`);
  } catch (e) {
    throw new Error(getDruidErrorMessage(e));
  }

  return statusResp.data.modules.map((m: any) => m.artifact);
}

export async function postToSampler(
  sampleSpec: SampleSpec,
  forStr: string,
): Promise<SampleResponse> {
  sampleSpec = fixSamplerTypes(sampleSpec);

  let sampleResp: any;
  try {
    sampleResp = await Api.instance.post(`${SAMPLER_URL}?for=${forStr}`, sampleSpec);
  } catch (e) {
    throw new Error(getDruidErrorMessage(e));
  }

  return sampleResp.data;
}

export type SampleStrategy = 'start' | 'end';

function makeSamplerIoConfig(
  ioConfig: IoConfig,
  specType: IngestionType,
  sampleStrategy: SampleStrategy,
): IoConfig {
  ioConfig = deepSet(ioConfig || {}, 'type', specType);
  if (specType === 'kafka') {
    ioConfig = deepSet(ioConfig, 'useEarliestOffset', sampleStrategy === 'start');
  } else if (specType === 'kinesis') {
    ioConfig = deepSet(ioConfig, 'useEarliestSequenceNumber', sampleStrategy === 'start');
  }
  // In order to prevent potential data loss null columns should be kept by the sampler and shown in the ingestion flow
  if (ioConfig.inputFormat) {
    ioConfig = deepSet(ioConfig, 'inputFormat.keepNullColumns', true);
  }
  return ioConfig;
}

/**
  This is a hack to deal with the fact that the sampler can not deal with the index_parallel type
 */
function fixSamplerTypes(sampleSpec: SampleSpec): SampleSpec {
  let samplerType: string = getSpecType(sampleSpec);
  if (samplerType === 'index_parallel') {
    samplerType = 'index';
  }

  sampleSpec = deepSet(sampleSpec, 'type', samplerType);
  sampleSpec = deepSet(sampleSpec, 'spec.type', samplerType);
  sampleSpec = deepSet(sampleSpec, 'spec.ioConfig.type', samplerType);
  sampleSpec = deepSet(sampleSpec, 'spec.tuningConfig.type', samplerType);
  return sampleSpec;
}

function cleanupQueryGranularity(queryGranularity: any): any {
  let queryGranularityType = deepGet(queryGranularity, 'type');
  if (typeof queryGranularityType !== 'string') return queryGranularity;
  queryGranularityType = queryGranularityType.toUpperCase();

  const knownGranularity = oneOf(
    queryGranularityType,
    'NONE',
    'SECOND',
    'MINUTE',
    'HOUR',
    'DAY',
    'WEEK',
    'MONTH',
    'YEAR',
  );

  return knownGranularity ? queryGranularityType : queryGranularity;
}

export async function sampleForConnect(
  spec: IngestionSpec,
  sampleStrategy: SampleStrategy,
): Promise<SampleResponseWithExtraInfo> {
  const samplerType = getSpecType(spec);
  let ioConfig: IoConfig = makeSamplerIoConfig(
    deepGet(spec, 'spec.ioConfig'),
    samplerType,
    sampleStrategy,
  );

  const reingestMode = isDruidSource(spec);
  if (!reingestMode) {
    ioConfig = deepSet(ioConfig, 'inputFormat', {
      type: 'regex',
      pattern: '(.*)',
      listDelimiter: '56616469-6de2-9da4-efb8-8f416e6e6965', // Just a UUID to disable the list delimiter, let's hope we do not see this UUID in the data
      columns: ['raw'],
    });
  }

  const sampleSpec: SampleSpec = {
    type: samplerType,
    spec: {
      type: samplerType,
      ioConfig,
      dataSchema: {
        dataSource: 'sample',
        timestampSpec: PLACEHOLDER_TIMESTAMP_SPEC,
        dimensionsSpec: {},
      },
    } as any,
    samplerConfig: BASE_SAMPLER_CONFIG,
  };

  const samplerResponse: SampleResponseWithExtraInfo = await postToSampler(sampleSpec, 'connect');

  if (!samplerResponse.data.length) return samplerResponse;

  if (reingestMode) {
    const segmentMetadataResponse = await queryDruidRune({
      queryType: 'segmentMetadata',
      dataSource: deepGet(ioConfig, 'inputSource.dataSource'),
      intervals: [deepGet(ioConfig, 'inputSource.interval')],
      merge: true,
      lenientAggregatorMerge: true,
      analysisTypes: ['timestampSpec', 'queryGranularity', 'aggregators', 'rollup'],
    });

    if (Array.isArray(segmentMetadataResponse) && segmentMetadataResponse.length === 1) {
      const segmentMetadataResponse0 = segmentMetadataResponse[0];
      samplerResponse.queryGranularity = cleanupQueryGranularity(
        segmentMetadataResponse0.queryGranularity,
      );
      samplerResponse.rollup = segmentMetadataResponse0.rollup;
      samplerResponse.columns = segmentMetadataResponse0.columns;
      samplerResponse.aggregators = segmentMetadataResponse0.aggregators;
    } else {
      throw new Error(`unexpected response from segmentMetadata query`);
    }
  }

  return samplerResponse;
}

export async function sampleForParser(
  spec: IngestionSpec,
  sampleStrategy: SampleStrategy,
): Promise<SampleResponse> {
  const samplerType = getSpecType(spec);
  const ioConfig: IoConfig = makeSamplerIoConfig(
    deepGet(spec, 'spec.ioConfig'),
    samplerType,
    sampleStrategy,
  );

  const sampleSpec: SampleSpec = {
    type: samplerType,
    spec: {
      ioConfig,
      dataSchema: {
        dataSource: 'sample',
        timestampSpec: PLACEHOLDER_TIMESTAMP_SPEC,
        dimensionsSpec: {},
      },
    },
    samplerConfig: BASE_SAMPLER_CONFIG,
  };

  return postToSampler(sampleSpec, 'parser');
}

export async function sampleForTimestamp(
  spec: IngestionSpec,
  cacheRows: CacheRows,
): Promise<SampleResponse> {
  const samplerType = getSpecType(spec);
  const timestampSpec: TimestampSpec = deepGet(spec, 'spec.dataSchema.timestampSpec');
  const timestampSchema = getTimestampSchema(spec);

  // First do a query with a static timestamp spec
  const sampleSpecColumns: SampleSpec = {
    type: samplerType,
    spec: {
      ioConfig: deepGet(spec, 'spec.ioConfig'),
      dataSchema: {
        dataSource: 'sample',
        dimensionsSpec: {},
        timestampSpec: timestampSchema === 'column' ? PLACEHOLDER_TIMESTAMP_SPEC : timestampSpec,
      },
    },
    samplerConfig: BASE_SAMPLER_CONFIG,
  };

  const sampleColumns = await postToSampler(
    applyCache(sampleSpecColumns, cacheRows),
    'timestamp-columns',
  );

  // If we are not parsing a column then there is nothing left to do
  if (timestampSchema === 'none') return sampleColumns;

  const transforms: Transform[] =
    deepGet(spec, 'spec.dataSchema.transformSpec.transforms') || EMPTY_ARRAY;

  // If we are trying to parts a column then get a bit fancy:
  // Query the same sample again (same cache key)
  const sampleSpec: SampleSpec = {
    type: samplerType,
    spec: {
      ioConfig: deepGet(spec, 'spec.ioConfig'),
      dataSchema: {
        dataSource: 'sample',
        dimensionsSpec: {},
        timestampSpec,
        transformSpec: {
          transforms: transforms.filter(transform => transform.name === '__time'),
        },
      },
    },
    samplerConfig: BASE_SAMPLER_CONFIG,
  };

  const sampleTime = await postToSampler(applyCache(sampleSpec, cacheRows), 'timestamp-time');

  if (sampleTime.data.length !== sampleColumns.data.length) {
    // If the two responses did not come from the same cache (or for some reason have different lengths) then
    // just return the one with the parsed time column.
    return sampleTime;
  }

  const sampleTimeData = sampleTime.data;
  return Object.assign({}, sampleColumns, {
    data: sampleColumns.data.map((d, i) => {
      // Merge the column sample with the time column sample
      if (!d.parsed) return d;
      const timeDatumParsed = sampleTimeData[i].parsed;
      d.parsed.__time = timeDatumParsed ? timeDatumParsed.__time : null;
      return d;
    }),
  });
}

export async function sampleForTransform(
  spec: IngestionSpec,
  cacheRows: CacheRows,
): Promise<SampleResponse> {
  const samplerType = getSpecType(spec);
  const inputFormatColumns: string[] = deepGet(spec, 'spec.ioConfig.inputFormat.columns') || [];
  const timestampSpec: TimestampSpec = deepGet(spec, 'spec.dataSchema.timestampSpec');
  const transforms: Transform[] = deepGet(spec, 'spec.dataSchema.transformSpec.transforms') || [];

  // Extra step to simulate auto detecting dimension with transforms
  const specialDimensionSpec: DimensionsSpec = {};
  if (transforms && transforms.length) {
    const sampleSpecHack: SampleSpec = {
      type: samplerType,
      spec: {
        ioConfig: deepGet(spec, 'spec.ioConfig'),
        dataSchema: {
          dataSource: 'sample',
          timestampSpec,
          dimensionsSpec: {},
        },
      },
      samplerConfig: BASE_SAMPLER_CONFIG,
    };

    const sampleResponseHack = await postToSampler(
      applyCache(sampleSpecHack, cacheRows),
      'transform-pre',
    );

    specialDimensionSpec.dimensions = dedupe(
      headerFromSampleResponse({
        sampleResponse: sampleResponseHack,
        ignoreTimeColumn: true,
        columnOrder: ['__time'].concat(inputFormatColumns),
      }).concat(transforms.map(t => t.name)),
    );
  }

  const sampleSpec: SampleSpec = {
    type: samplerType,
    spec: {
      ioConfig: deepGet(spec, 'spec.ioConfig'),
      dataSchema: {
        dataSource: 'sample',
        timestampSpec,
        dimensionsSpec: specialDimensionSpec, // Hack Hack Hack
        transformSpec: {
          transforms,
        },
      },
    },
    samplerConfig: BASE_SAMPLER_CONFIG,
  };

  return postToSampler(applyCache(sampleSpec, cacheRows), 'transform');
}

export async function sampleForFilter(
  spec: IngestionSpec,
  cacheRows: CacheRows,
): Promise<SampleResponse> {
  const samplerType = getSpecType(spec);
  const inputFormatColumns: string[] = deepGet(spec, 'spec.ioConfig.inputFormat.columns') || [];
  const timestampSpec: TimestampSpec = deepGet(spec, 'spec.dataSchema.timestampSpec');
  const transforms: Transform[] = deepGet(spec, 'spec.dataSchema.transformSpec.transforms') || [];
  const filter: any = deepGet(spec, 'spec.dataSchema.transformSpec.filter');

  // Extra step to simulate auto detecting dimension with transforms
  const specialDimensionSpec: DimensionsSpec = {};
  if (transforms && transforms.length) {
    const sampleSpecHack: SampleSpec = {
      type: samplerType,
      spec: {
        ioConfig: deepGet(spec, 'spec.ioConfig'),
        dataSchema: {
          dataSource: 'sample',
          timestampSpec,
          dimensionsSpec: {},
        },
      },
      samplerConfig: BASE_SAMPLER_CONFIG,
    };

    const sampleResponseHack = await postToSampler(
      applyCache(sampleSpecHack, cacheRows),
      'filter-pre',
    );

    specialDimensionSpec.dimensions = dedupe(
      headerFromSampleResponse({
        sampleResponse: sampleResponseHack,
        ignoreTimeColumn: true,
        columnOrder: ['__time'].concat(inputFormatColumns),
      }).concat(transforms.map(t => t.name)),
    );
  }

  const sampleSpec: SampleSpec = {
    type: samplerType,
    spec: {
      ioConfig: deepGet(spec, 'spec.ioConfig'),
      dataSchema: {
        dataSource: 'sample',
        timestampSpec,
        dimensionsSpec: specialDimensionSpec, // Hack Hack Hack
        transformSpec: {
          transforms,
          filter,
        },
      },
    },
    samplerConfig: BASE_SAMPLER_CONFIG,
  };

  return postToSampler(applyCache(sampleSpec, cacheRows), 'filter');
}

export async function sampleForSchema(
  spec: IngestionSpec,
  cacheRows: CacheRows,
): Promise<SampleResponse> {
  const samplerType = getSpecType(spec);
  const timestampSpec: TimestampSpec = deepGet(spec, 'spec.dataSchema.timestampSpec');
  const transformSpec: TransformSpec =
    deepGet(spec, 'spec.dataSchema.transformSpec') || ({} as TransformSpec);
  const dimensionsSpec: DimensionsSpec = deepGet(spec, 'spec.dataSchema.dimensionsSpec');
  const metricsSpec: MetricSpec[] = deepGet(spec, 'spec.dataSchema.metricsSpec') || [];
  const queryGranularity: string =
    deepGet(spec, 'spec.dataSchema.granularitySpec.queryGranularity') || 'NONE';

  const sampleSpec: SampleSpec = {
    type: samplerType,
    spec: {
      ioConfig: deepGet(spec, 'spec.ioConfig'),
      dataSchema: {
        dataSource: 'sample',
        timestampSpec,
        transformSpec,
        granularitySpec: {
          queryGranularity,
        },
        dimensionsSpec,
        metricsSpec,
      },
    },
    samplerConfig: BASE_SAMPLER_CONFIG,
  };

  return postToSampler(applyCache(sampleSpec, cacheRows), 'schema');
}

export async function sampleForExampleManifests(
  exampleManifestUrl: string,
): Promise<ExampleManifest[]> {
  const exampleSpec: SampleSpec = {
    type: 'index_parallel',
    spec: {
      ioConfig: {
        type: 'index_parallel',
        inputSource: { type: 'http', uris: [exampleManifestUrl] },
        inputFormat: { type: 'tsv', findColumnsFromHeader: true },
      },
      dataSchema: {
        dataSource: 'sample',
        timestampSpec: {
          column: 'timestamp',
          missingValue: '2010-01-01T00:00:00Z',
        },
        dimensionsSpec: {},
      },
    },
    samplerConfig: { numRows: 50, timeoutMs: 10000 },
  };

  const exampleData = await postToSampler(exampleSpec, 'example-manifest');

  return filterMap(exampleData.data, datum => {
    const parsed = datum.parsed;
    if (!parsed) return;
    let { name, description, spec } = parsed;
    try {
      spec = JSON.parse(spec);
    } catch {
      return;
    }

    if (
      typeof name === 'string' &&
      typeof description === 'string' &&
      spec &&
      typeof spec === 'object'
    ) {
      return {
        name: parsed.name,
        description: parsed.description,
        spec: upgradeSpec(spec),
      };
    } else {
      return;
    }
  });
}
