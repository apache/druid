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
  getDimensionNamesFromTransforms,
  getSpecType,
  getTimestampSchema,
  IngestionSpec,
  IngestionType,
  InputFormat,
  IoConfig,
  isDruidSource,
  MetricSpec,
  PLACEHOLDER_TIMESTAMP_SPEC,
  REINDEX_TIMESTAMP_SPEC,
  TIME_COLUMN,
  TimestampSpec,
  Transform,
  TransformSpec,
  upgradeSpec,
} from '../druid-models';
import { Api } from '../singletons';

import { getDruidErrorMessage, queryDruidRune } from './druid-query';
import { arrangeWithPrefixSuffix, EMPTY_ARRAY, filterMap } from './general';
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
  columns?: string[];
  columnInfo?: Record<string, any>;
  aggregators?: Record<string, any>;
  rollup?: boolean;
}

export interface SampleEntry {
  input: Record<string, any>;
  parsed?: Record<string, any>;
  unparseable?: boolean;
  error?: string;
}

export interface SampleHeaderAndRows {
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

export function getCacheRowsFromSampleResponse(sampleResponse: SampleResponse): CacheRows {
  return filterMap(sampleResponse.data, d => d.input).slice(0, 20);
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
  let inputFormat: InputFormat = { type: 'json', keepNullColumns: true };
  if (flattenSpec) {
    inputFormat = deepSet(inputFormat, 'flattenSpec', flattenSpec);
  }
  sampleSpec = deepSet(sampleSpec, 'spec.ioConfig.inputFormat', inputFormat);

  return sampleSpec;
}

export interface HeaderFromSampleResponseOptions {
  sampleResponse: SampleResponse;
  ignoreTimeColumn?: boolean;
  columnOrder?: string[];
  suffixColumnOrder?: string[];
  useInput?: boolean;
}

export function headerFromSampleResponse(options: HeaderFromSampleResponseOptions): string[] {
  const { sampleResponse, ignoreTimeColumn, columnOrder, suffixColumnOrder, useInput } = options;

  const key = useInput ? 'input' : 'parsed';
  let columns = arrangeWithPrefixSuffix(
    dedupe(sampleResponse.data.flatMap(s => (s[key] ? Object.keys(s[key]!) : []))),
    columnOrder || [TIME_COLUMN],
    suffixColumnOrder || [],
  );

  if (ignoreTimeColumn) {
    columns = columns.filter(c => c !== TIME_COLUMN);
  }

  return columns;
}

export interface HeaderAndRowsFromSampleResponseOptions extends HeaderFromSampleResponseOptions {
  parsedOnly?: boolean;
}

export function headerAndRowsFromSampleResponse(
  options: HeaderAndRowsFromSampleResponseOptions,
): SampleHeaderAndRows {
  const { sampleResponse, parsedOnly } = options;

  return {
    header: headerFromSampleResponse(options),
    rows: parsedOnly ? sampleResponse.data.filter(d => d.parsed) : sampleResponse.data,
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

export async function sampleForConnect(
  spec: Partial<IngestionSpec>,
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
      pattern: '([\\s\\S]*)', // Match the entire line, every single character
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
        timestampSpec: reingestMode ? REINDEX_TIMESTAMP_SPEC : PLACEHOLDER_TIMESTAMP_SPEC,
        dimensionsSpec: {},
        granularitySpec: {
          rollup: false,
        },
      },
    } as any,
    samplerConfig: BASE_SAMPLER_CONFIG,
  };

  const samplerResponse: SampleResponseWithExtraInfo = await postToSampler(sampleSpec, 'connect');

  if (!samplerResponse.data.length) return samplerResponse;

  if (reingestMode) {
    const dataSource = deepGet(ioConfig, 'inputSource.dataSource');
    const intervals = deepGet(ioConfig, 'inputSource.interval');

    const scanResponse = await queryDruidRune({
      queryType: 'scan',
      dataSource,
      intervals,
      resultFormat: 'compactedList',
      limit: 1,
      columns: [],
      granularity: 'all',
    });

    const columns = deepGet(scanResponse, '0.columns');
    if (!Array.isArray(columns)) {
      throw new Error(`unexpected response from scan query`);
    }
    samplerResponse.columns = columns;

    const segmentMetadataResponse = await queryDruidRune({
      queryType: 'segmentMetadata',
      dataSource,
      intervals,
      merge: true,
      lenientAggregatorMerge: true,
      analysisTypes: ['aggregators', 'rollup'],
    });

    if (!Array.isArray(segmentMetadataResponse) || segmentMetadataResponse.length !== 1) {
      throw new Error(`unexpected response from segmentMetadata query`);
    }
    const segmentMetadataResponse0 = segmentMetadataResponse[0];
    samplerResponse.rollup = segmentMetadataResponse0.rollup;
    samplerResponse.columnInfo = segmentMetadataResponse0.columns;
    samplerResponse.aggregators = segmentMetadataResponse0.aggregators;
  }

  return samplerResponse;
}

export async function sampleForParser(
  spec: Partial<IngestionSpec>,
  sampleStrategy: SampleStrategy,
): Promise<SampleResponse> {
  const samplerType = getSpecType(spec);
  const ioConfig: IoConfig = makeSamplerIoConfig(
    deepGet(spec, 'spec.ioConfig'),
    samplerType,
    sampleStrategy,
  );

  const reingestMode = isDruidSource(spec);

  const sampleSpec: SampleSpec = {
    type: samplerType,
    spec: {
      ioConfig,
      dataSchema: {
        dataSource: 'sample',
        timestampSpec: reingestMode ? REINDEX_TIMESTAMP_SPEC : PLACEHOLDER_TIMESTAMP_SPEC,
        dimensionsSpec: {},
        granularitySpec: {
          rollup: false,
        },
      },
    },
    samplerConfig: BASE_SAMPLER_CONFIG,
  };

  return postToSampler(sampleSpec, 'parser');
}

export async function sampleForTimestamp(
  spec: Partial<IngestionSpec>,
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
        granularitySpec: {
          rollup: false,
        },
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
          transforms: transforms.filter(transform => transform.name === TIME_COLUMN),
        },
        granularitySpec: {
          rollup: false,
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
  return {
    ...sampleColumns,
    data: sampleColumns.data.map((d, i) => {
      // Merge the column sample with the time column sample
      if (!d.parsed) return d;
      const timeDatumParsed = sampleTimeData[i].parsed;
      d.parsed.__time = timeDatumParsed ? timeDatumParsed.__time : null;
      return d;
    }),
  };
}

export async function sampleForTransform(
  spec: Partial<IngestionSpec>,
  cacheRows: CacheRows,
): Promise<SampleResponse> {
  const samplerType = getSpecType(spec);
  const timestampSpec: TimestampSpec = deepGet(spec, 'spec.dataSchema.timestampSpec');
  const transforms: Transform[] = deepGet(spec, 'spec.dataSchema.transformSpec.transforms') || [];

  // Extra step to simulate auto detecting dimension with transforms
  let specialDimensionSpec: DimensionsSpec = {};
  if (transforms && transforms.length) {
    const sampleSpecHack: SampleSpec = {
      type: samplerType,
      spec: {
        ioConfig: deepGet(spec, 'spec.ioConfig'),
        dataSchema: {
          dataSource: 'sample',
          timestampSpec,
          dimensionsSpec: {},
          granularitySpec: {
            rollup: false,
          },
        },
      },
      samplerConfig: BASE_SAMPLER_CONFIG,
    };

    const sampleResponseHack = await postToSampler(
      applyCache(sampleSpecHack, cacheRows),
      'transform-pre',
    );

    specialDimensionSpec = deepSet(
      specialDimensionSpec,
      'dimensions',
      dedupe(
        headerFromSampleResponse({
          sampleResponse: sampleResponseHack,
          ignoreTimeColumn: true,
        }).concat(getDimensionNamesFromTransforms(transforms)),
      ),
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
        granularitySpec: {
          rollup: false,
        },
      },
    },
    samplerConfig: BASE_SAMPLER_CONFIG,
  };

  return postToSampler(applyCache(sampleSpec, cacheRows), 'transform');
}

export async function sampleForFilter(
  spec: Partial<IngestionSpec>,
  cacheRows: CacheRows,
): Promise<SampleResponse> {
  const samplerType = getSpecType(spec);
  const timestampSpec: TimestampSpec = deepGet(spec, 'spec.dataSchema.timestampSpec');
  const transforms: Transform[] = deepGet(spec, 'spec.dataSchema.transformSpec.transforms') || [];
  const filter: any = deepGet(spec, 'spec.dataSchema.transformSpec.filter');

  // Extra step to simulate auto detecting dimension with transforms
  let specialDimensionSpec: DimensionsSpec = {};
  if (transforms && transforms.length) {
    const sampleSpecHack: SampleSpec = {
      type: samplerType,
      spec: {
        ioConfig: deepGet(spec, 'spec.ioConfig'),
        dataSchema: {
          dataSource: 'sample',
          timestampSpec,
          dimensionsSpec: {},
          granularitySpec: {
            rollup: false,
          },
        },
      },
      samplerConfig: BASE_SAMPLER_CONFIG,
    };

    const sampleResponseHack = await postToSampler(
      applyCache(sampleSpecHack, cacheRows),
      'filter-pre',
    );

    specialDimensionSpec = deepSet(
      specialDimensionSpec,
      'dimensions',
      dedupe(
        headerFromSampleResponse({
          sampleResponse: sampleResponseHack,
          ignoreTimeColumn: true,
        }).concat(getDimensionNamesFromTransforms(transforms)),
      ),
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
        granularitySpec: {
          rollup: false,
        },
      },
    },
    samplerConfig: BASE_SAMPLER_CONFIG,
  };

  return postToSampler(applyCache(sampleSpec, cacheRows), 'filter');
}

export async function sampleForSchema(
  spec: Partial<IngestionSpec>,
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
  const rollup = deepGet(spec, 'spec.dataSchema.granularitySpec.rollup') ?? true;

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
          rollup,
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
