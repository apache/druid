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

import axios from 'axios';

import { getDruidErrorMessage } from './druid-query';
import { filterMap, sortWithPrefixSuffix } from './general';
import {
  DimensionsSpec,
  getEmptyTimestampSpec, getSpecType,
  IngestionSpec,
  IoConfig, MetricSpec,
  Parser,
  ParseSpec,
  Transform, TransformSpec
} from './ingestion-spec';
import { deepGet, deepSet, shallowCopy, whitelistKeys } from './object-change';
import { QueryState } from './query-state';

const SAMPLER_URL = `/druid/indexer/v1/sampler`;
const BASE_SAMPLER_CONFIG: SamplerConfig = {
  // skipCache: true,
  numRows: 500,
  timeoutMs: 15000
};

export interface SampleSpec {
  type: string;
  spec: IngestionSpec;
  samplerConfig: SamplerConfig;
}

export interface SamplerConfig {
  numRows?: number;
  timeoutMs?: number;
  cacheKey?: string;
  skipCache?: boolean;
}

export interface SampleResponse {
  cacheKey?: string;
  data: SampleEntry[];
}

export interface SampleEntry {
  raw: string;
  parsed?: Record<string, any>;
  unparseable?: boolean;
  error?: string;
}

export interface HeaderAndRows {
  header: string[];
  rows: SampleEntry[];
}

function dedupe(xs: string[]): string[] {
  const seen: Record<string, boolean> = {};
  return xs.filter((x) => {
    if (seen[x]) {
      return false;
    } else {
      seen[x] = true;
      return true;
    }
  });
}

type SamplerType = 'index' | 'kafka' | 'kinesis';

export function getSamplerType(spec: IngestionSpec): SamplerType {
  const specType = getSpecType(spec);
  if (specType === 'kafka' || specType === 'kinesis') return specType;
  return 'index';
}

export function headerFromSampleResponse(sampleResponse: SampleResponse, ignoreColumn?: string): string[] {
  let columns = sortWithPrefixSuffix(dedupe(
    [].concat(...(filterMap(sampleResponse.data, s => s.parsed ? Object.keys(s.parsed) : null) as any))
  ).sort(), ['__time'], []);

  if (ignoreColumn) {
    columns = columns.filter(c => c !== ignoreColumn);
  }

  return columns;
}

export function headerAndRowsFromSampleResponse(sampleResponse: SampleResponse, ignoreColumn?: string, parsedOnly = false): HeaderAndRows {
  return {
    header: headerFromSampleResponse(sampleResponse, ignoreColumn),
    rows: parsedOnly ? sampleResponse.data.filter((d: any) => d.parsed) : sampleResponse.data
  };
}

export async function getOverlordModules(): Promise<string[]> {
  let statusResp: any;
  try {
    statusResp = await axios.get(`/proxy/overlord/status`);
  } catch (e) {
    throw new Error(getDruidErrorMessage(e));
  }

  return statusResp.data.modules.map((m: any) => m.artifact);
}

async function postToSampler(sampleSpec: SampleSpec, forStr: string): Promise<SampleResponse> {
  let sampleResp: any;
  try {
    sampleResp = await axios.post(`${SAMPLER_URL}?for=${forStr}`, sampleSpec);
  } catch (e) {
    throw new Error(getDruidErrorMessage(e));
  }

  return sampleResp.data;
}

export type SampleStrategy = 'start' | 'end';

function makeSamplerIoConfig(ioConfig: IoConfig, samplerType: SamplerType, sampleStrategy: SampleStrategy): IoConfig {
  ioConfig = deepSet(ioConfig || {}, 'type', samplerType);
  if (samplerType === 'kafka') {
    ioConfig = deepSet(ioConfig, 'useEarliestOffset', sampleStrategy === 'start');
  } else if (samplerType === 'kinesis') {
    ioConfig = deepSet(ioConfig, 'useEarliestSequenceNumber', sampleStrategy === 'start');
  }
  return ioConfig;
}

export async function sampleForConnect(spec: IngestionSpec, sampleStrategy: SampleStrategy): Promise<SampleResponse> {
  const samplerType = getSamplerType(spec);
  const ioConfig: IoConfig = makeSamplerIoConfig(deepGet(spec, 'ioConfig'), samplerType, sampleStrategy);

  const sampleSpec: SampleSpec = {
    type: samplerType,
    spec: {
      type: samplerType,
      ioConfig,
      dataSchema: {
        dataSource: 'sample',
        parser: {
          type: 'string',
          parseSpec: {
            format: 'regex',
            pattern: '(.*)',
            columns: ['a'],
            dimensionsSpec: {},
            timestampSpec: getEmptyTimestampSpec()
          }
        }
      }
    } as any,
    samplerConfig: BASE_SAMPLER_CONFIG
  };

  return postToSampler(sampleSpec, 'connect');
}

export async function sampleForParser(spec: IngestionSpec, sampleStrategy: SampleStrategy, cacheKey: string | undefined): Promise<SampleResponse> {
  const samplerType = getSamplerType(spec);
  const ioConfig: IoConfig = makeSamplerIoConfig(deepGet(spec, 'ioConfig'), samplerType, sampleStrategy);
  const parser: Parser = deepGet(spec, 'dataSchema.parser') || {};

  const sampleSpec: SampleSpec = {
    type: samplerType,
    spec: {
      type: samplerType,
      ioConfig: deepSet(ioConfig, 'type', samplerType),
      dataSchema: {
        dataSource: 'sample',
        parser: {
          type: parser.type,
          parseSpec: (
            parser.parseSpec ?
              Object.assign({}, parser.parseSpec, {
                dimensionsSpec: {},
                timestampSpec: getEmptyTimestampSpec()
              }) :
              undefined
          ) as any
        }
      }
    },
    samplerConfig: Object.assign({}, BASE_SAMPLER_CONFIG, {
      cacheKey
    })
  };

  return postToSampler(sampleSpec, 'parser');
}

export async function sampleForTimestamp(spec: IngestionSpec, sampleStrategy: SampleStrategy, cacheKey: string | undefined): Promise<SampleResponse> {
  const samplerType = getSamplerType(spec);
  const ioConfig: IoConfig = makeSamplerIoConfig(deepGet(spec, 'ioConfig'), samplerType, sampleStrategy);
  const parser: Parser = deepGet(spec, 'dataSchema.parser') || {};
  const parseSpec: ParseSpec = deepGet(spec, 'dataSchema.parser.parseSpec') || {};

  const sampleSpec: SampleSpec = {
    type: samplerType,
    spec: {
      type: samplerType,
      ioConfig: deepSet(ioConfig, 'type', samplerType),
      dataSchema: {
        dataSource: 'sample',
        parser: {
          type: parser.type,
          parseSpec: Object.assign({}, parseSpec, {
            dimensionsSpec: {}
          })
        }
      }
    },
    samplerConfig: Object.assign({}, BASE_SAMPLER_CONFIG, {
      cacheKey
    })
  };

  return postToSampler(sampleSpec, 'timestamp');
}

export async function sampleForTransform(spec: IngestionSpec, sampleStrategy: SampleStrategy, cacheKey: string | undefined): Promise<SampleResponse> {
  const samplerType = getSamplerType(spec);
  const ioConfig: IoConfig = makeSamplerIoConfig(deepGet(spec, 'ioConfig'), samplerType, sampleStrategy);
  const parser: Parser = deepGet(spec, 'dataSchema.parser') || {};
  const parseSpec: ParseSpec = deepGet(spec, 'dataSchema.parser.parseSpec') || {};
  const transforms: Transform[] = deepGet(spec, 'dataSchema.transformSpec.transforms') || [];

  // Extra step to simulate auto detecting dimension with transforms
  const specialDimensionSpec: DimensionsSpec = {};
  if (transforms && transforms.length) {

    const sampleSpecHack: SampleSpec = {
      type: samplerType,
      spec: {
        type: samplerType,
        ioConfig: deepSet(ioConfig, 'type', samplerType),
        dataSchema: {
          dataSource: 'sample',
          parser: {
            type: parser.type,
            parseSpec: Object.assign({}, parseSpec, {
              dimensionsSpec: {}
            })
          }
        }
      },
      samplerConfig: Object.assign({}, BASE_SAMPLER_CONFIG, {
        cacheKey
      })
    };

    const sampleResponseHack = await postToSampler(sampleSpecHack, 'transform-pre');

    specialDimensionSpec.dimensions = dedupe(headerFromSampleResponse(sampleResponseHack, '__time').concat(transforms.map(t => t.name)));
  }

  const sampleSpec: SampleSpec = {
    type: samplerType,
    spec: {
      type: samplerType,
      ioConfig: deepSet(ioConfig, 'type', samplerType),
      dataSchema: {
        dataSource: 'sample',
        parser: {
          type: parser.type,
          parseSpec: Object.assign({}, parseSpec, {
            dimensionsSpec: specialDimensionSpec // Hack Hack Hack
          })
        },
        transformSpec: {
          transforms
        }
      }
    },
    samplerConfig: Object.assign({}, BASE_SAMPLER_CONFIG, {
      cacheKey
    })
  };

  return postToSampler(sampleSpec, 'transform');
}

export async function sampleForFilter(spec: IngestionSpec, sampleStrategy: SampleStrategy, cacheKey: string | undefined): Promise<SampleResponse> {
  const samplerType = getSamplerType(spec);
  const ioConfig: IoConfig = makeSamplerIoConfig(deepGet(spec, 'ioConfig'), samplerType, sampleStrategy);
  const parser: Parser = deepGet(spec, 'dataSchema.parser') || {};
  const parseSpec: ParseSpec = deepGet(spec, 'dataSchema.parser.parseSpec') || {};
  const transforms: Transform[] = deepGet(spec, 'dataSchema.transformSpec.transforms') || [];
  const filter: any = deepGet(spec, 'dataSchema.transformSpec.filter');

  // Extra step to simulate auto detecting dimension with transforms
  const specialDimensionSpec: DimensionsSpec = {};
  if (transforms && transforms.length) {

    const sampleSpecHack: SampleSpec = {
      type: samplerType,
      spec: {
        type: samplerType,
        ioConfig: deepSet(ioConfig, 'type', samplerType),
        dataSchema: {
          dataSource: 'sample',
          parser: {
            type: parser.type,
            parseSpec: Object.assign({}, parseSpec, {
              dimensionsSpec: {}
            })
          }
        }
      },
      samplerConfig: Object.assign({}, BASE_SAMPLER_CONFIG, {
        cacheKey
      })
    };

    const sampleResponseHack = await postToSampler(sampleSpecHack, 'filter-pre');

    specialDimensionSpec.dimensions = dedupe(headerFromSampleResponse(sampleResponseHack, '__time').concat(transforms.map(t => t.name)));
  }

  const sampleSpec: SampleSpec = {
    type: samplerType,
    spec: {
      type: samplerType,
      ioConfig: deepSet(ioConfig, 'type', samplerType),
      dataSchema: {
        dataSource: 'sample',
        parser: {
          type: parser.type,
          parseSpec: Object.assign({}, parseSpec, {
            dimensionsSpec: specialDimensionSpec // Hack Hack Hack
          })
        },
        transformSpec: {
          transforms,
          filter
        }
      }
    },
    samplerConfig: Object.assign({}, BASE_SAMPLER_CONFIG, {
      cacheKey
    })
  };

  return postToSampler(sampleSpec, 'filter');
}

export async function sampleForSchema(spec: IngestionSpec, sampleStrategy: SampleStrategy, cacheKey: string | undefined): Promise<SampleResponse> {
  const samplerType = getSamplerType(spec);
  const ioConfig: IoConfig = makeSamplerIoConfig(deepGet(spec, 'ioConfig'), samplerType, sampleStrategy);
  const parser: Parser = deepGet(spec, 'dataSchema.parser') || {};
  const transformSpec: TransformSpec = deepGet(spec, 'dataSchema.transformSpec') || ({} as TransformSpec);
  const metricsSpec: MetricSpec[] = deepGet(spec, 'dataSchema.metricsSpec') || [];
  const queryGranularity: string = deepGet(spec, 'dataSchema.granularitySpec.queryGranularity') || 'NONE';

  const sampleSpec: SampleSpec = {
    type: samplerType,
    spec: {
      type: samplerType,
      ioConfig: deepSet(ioConfig, 'type', samplerType),
      dataSchema: {
        dataSource: 'sample',
        parser: whitelistKeys(parser, ['type', 'parseSpec']) as Parser,
        transformSpec,
        metricsSpec,
        granularitySpec: {
          queryGranularity
        }
      }
    },
    samplerConfig: Object.assign({}, BASE_SAMPLER_CONFIG, {
      cacheKey
    })
  };

  return postToSampler(sampleSpec, 'schema');
}
