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
import { alphanumericCompare, filterMap, sortWithPrefixSuffix } from './general';
import {
  DimensionsSpec,
  getEmptyTimestampSpec,
  getSpecType,
  IngestionSpec,
  IoConfig,
  isColumnTimestampSpec,
  MetricSpec,
  Parser,
  ParseSpec,
  Transform,
  TransformSpec,
} from './ingestion-spec';
import { deepGet, deepSet, whitelistKeys } from './object-change';

const SAMPLER_URL = `/druid/indexer/v1/sampler`;
const BASE_SAMPLER_CONFIG: SamplerConfig = {
  // skipCache: true,
  numRows: 500,
  timeoutMs: 15000,
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

type SamplerType = 'index' | 'kafka' | 'kinesis';

export function getSamplerType(spec: IngestionSpec): SamplerType {
  const specType = getSpecType(spec);
  if (specType === 'kafka' || specType === 'kinesis') return specType;
  return 'index';
}

export function headerFromSampleResponse(
  sampleResponse: SampleResponse,
  ignoreColumn?: string,
  columnOrder?: string[],
): string[] {
  console.log(JSON.stringify(sampleResponse));
  let columns = sortWithPrefixSuffix(
    dedupe(sampleResponse.data.flatMap(s => (s.parsed ? Object.keys(s.parsed) : []))).sort(),
    columnOrder || ['__time'],
    [],
    alphanumericCompare,
  );

  if (ignoreColumn) {
    columns = columns.filter(c => c !== ignoreColumn);
  }

  return columns;
}

export function headerAndRowsFromSampleResponse(
  sampleResponse: SampleResponse,
  ignoreColumn?: string,
  columnOrder?: string[],
  parsedOnly = false,
): HeaderAndRows {
  return {
    header: headerFromSampleResponse(sampleResponse, ignoreColumn, columnOrder),
    rows: parsedOnly ? sampleResponse.data.filter((d: any) => d.parsed) : sampleResponse.data,
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

function makeSamplerIoConfig(
  ioConfig: IoConfig,
  samplerType: SamplerType,
  sampleStrategy: SampleStrategy,
): IoConfig {
  ioConfig = deepSet(ioConfig || {}, 'type', samplerType);
  if (samplerType === 'kafka') {
    ioConfig = deepSet(ioConfig, 'useEarliestOffset', sampleStrategy === 'start');
  } else if (samplerType === 'kinesis') {
    ioConfig = deepSet(ioConfig, 'useEarliestSequenceNumber', sampleStrategy === 'start');
  }
  return ioConfig;
}

export async function sampleForConnect(
  spec: IngestionSpec,
  sampleStrategy: SampleStrategy,
): Promise<SampleResponse> {
  const samplerType = getSamplerType(spec);
  const ioConfig: IoConfig = makeSamplerIoConfig(
    deepGet(spec, 'ioConfig'),
    samplerType,
    sampleStrategy,
  );

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
            timestampSpec: getEmptyTimestampSpec(),
          },
        },
      },
    } as any,
    samplerConfig: BASE_SAMPLER_CONFIG,
  };

  return postToSampler(sampleSpec, 'connect');
}

export async function sampleForParser(
  spec: IngestionSpec,
  sampleStrategy: SampleStrategy,
  cacheKey: string | undefined,
): Promise<SampleResponse> {
  const samplerType = getSamplerType(spec);
  const ioConfig: IoConfig = makeSamplerIoConfig(
    deepGet(spec, 'ioConfig'),
    samplerType,
    sampleStrategy,
  );
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
          parseSpec: (parser.parseSpec
            ? Object.assign({}, parser.parseSpec, {
                dimensionsSpec: {},
                timestampSpec: getEmptyTimestampSpec(),
              })
            : undefined) as any,
        },
      },
    },
    samplerConfig: Object.assign({}, BASE_SAMPLER_CONFIG, {
      cacheKey,
    }),
  };

  return postToSampler(sampleSpec, 'parser');
}

export async function sampleForTimestamp(
  spec: IngestionSpec,
  sampleStrategy: SampleStrategy,
  cacheKey: string | undefined,
): Promise<SampleResponse> {
  const samplerType = getSamplerType(spec);
  const ioConfig: IoConfig = makeSamplerIoConfig(
    deepGet(spec, 'ioConfig'),
    samplerType,
    sampleStrategy,
  );
  const parser: Parser = deepGet(spec, 'dataSchema.parser') || {};
  const parseSpec: ParseSpec = deepGet(spec, 'dataSchema.parser.parseSpec') || {};
  const timestampSpec: ParseSpec =
    deepGet(spec, 'dataSchema.parser.parseSpec.timestampSpec') || getEmptyTimestampSpec();
  const columnTimestampSpec = isColumnTimestampSpec(timestampSpec);

  // First do a query with a static timestamp spec
  const sampleSpecColumns: SampleSpec = {
    type: samplerType,
    spec: {
      type: samplerType,
      ioConfig: deepSet(ioConfig, 'type', samplerType),
      dataSchema: {
        dataSource: 'sample',
        parser: {
          type: parser.type,
          parseSpec: (parser.parseSpec
            ? Object.assign({}, parseSpec, {
                dimensionsSpec: {},
                timestampSpec: columnTimestampSpec ? getEmptyTimestampSpec() : timestampSpec,
              })
            : undefined) as any,
        },
      },
    },
    samplerConfig: Object.assign({}, BASE_SAMPLER_CONFIG, {
      cacheKey,
    }),
  };

  const sampleColumns = await postToSampler(sampleSpecColumns, 'timestamp-columns');

  // If we are not parsing a column then there is nothing left to do
  if (!columnTimestampSpec) return sampleColumns;

  // If we are trying to parts a column then get a bit fancy:
  // Query the same sample again (same cache key)
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
            dimensionsSpec: {},
          }),
        },
      },
    },
    samplerConfig: Object.assign({}, BASE_SAMPLER_CONFIG, {
      cacheKey: sampleColumns.cacheKey || cacheKey,
    }),
  };

  const sampleTime = await postToSampler(sampleSpec, 'timestamp-time');

  if (
    sampleTime.cacheKey !== sampleColumns.cacheKey ||
    sampleTime.data.length !== sampleColumns.data.length
  ) {
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
  sampleStrategy: SampleStrategy,
  cacheKey: string | undefined,
): Promise<SampleResponse> {
  const samplerType = getSamplerType(spec);
  const ioConfig: IoConfig = makeSamplerIoConfig(
    deepGet(spec, 'ioConfig'),
    samplerType,
    sampleStrategy,
  );
  const parser: Parser = deepGet(spec, 'dataSchema.parser') || {};
  const parseSpec: ParseSpec = deepGet(spec, 'dataSchema.parser.parseSpec') || {};
  const parserColumns: string[] = deepGet(parseSpec, 'columns') || [];
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
              dimensionsSpec: {},
            }),
          },
        },
      },
      samplerConfig: Object.assign({}, BASE_SAMPLER_CONFIG, {
        cacheKey,
      }),
    };

    const sampleResponseHack = await postToSampler(sampleSpecHack, 'transform-pre');

    specialDimensionSpec.dimensions = dedupe(
      headerFromSampleResponse(
        sampleResponseHack,
        '__time',
        ['__time'].concat(parserColumns),
      ).concat(transforms.map(t => t.name)),
    );
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
            dimensionsSpec: specialDimensionSpec, // Hack Hack Hack
          }),
        },
        transformSpec: {
          transforms,
        },
      },
    },
    samplerConfig: Object.assign({}, BASE_SAMPLER_CONFIG, {
      cacheKey,
    }),
  };

  return postToSampler(sampleSpec, 'transform');
}

export async function sampleForFilter(
  spec: IngestionSpec,
  sampleStrategy: SampleStrategy,
  cacheKey: string | undefined,
): Promise<SampleResponse> {
  const samplerType = getSamplerType(spec);
  const ioConfig: IoConfig = makeSamplerIoConfig(
    deepGet(spec, 'ioConfig'),
    samplerType,
    sampleStrategy,
  );
  const parser: Parser = deepGet(spec, 'dataSchema.parser') || {};
  const parseSpec: ParseSpec = deepGet(spec, 'dataSchema.parser.parseSpec') || {};
  const parserColumns: string[] = deepGet(parser, 'columns') || [];
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
              dimensionsSpec: {},
            }),
          },
        },
      },
      samplerConfig: Object.assign({}, BASE_SAMPLER_CONFIG, {
        cacheKey,
      }),
    };

    const sampleResponseHack = await postToSampler(sampleSpecHack, 'filter-pre');

    specialDimensionSpec.dimensions = dedupe(
      headerFromSampleResponse(
        sampleResponseHack,
        '__time',
        ['__time'].concat(parserColumns),
      ).concat(transforms.map(t => t.name)),
    );
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
            dimensionsSpec: specialDimensionSpec, // Hack Hack Hack
          }),
        },
        transformSpec: {
          transforms,
          filter,
        },
      },
    },
    samplerConfig: Object.assign({}, BASE_SAMPLER_CONFIG, {
      cacheKey,
    }),
  };

  return postToSampler(sampleSpec, 'filter');
}

export async function sampleForSchema(
  spec: IngestionSpec,
  sampleStrategy: SampleStrategy,
  cacheKey: string | undefined,
): Promise<SampleResponse> {
  const samplerType = getSamplerType(spec);
  const ioConfig: IoConfig = makeSamplerIoConfig(
    deepGet(spec, 'ioConfig'),
    samplerType,
    sampleStrategy,
  );
  const parser: Parser = deepGet(spec, 'dataSchema.parser') || {};
  const transformSpec: TransformSpec =
    deepGet(spec, 'dataSchema.transformSpec') || ({} as TransformSpec);
  const metricsSpec: MetricSpec[] = deepGet(spec, 'dataSchema.metricsSpec') || [];
  const queryGranularity: string =
    deepGet(spec, 'dataSchema.granularitySpec.queryGranularity') || 'NONE';

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
          queryGranularity,
        },
      },
    },
    samplerConfig: Object.assign({}, BASE_SAMPLER_CONFIG, {
      cacheKey,
    }),
  };

  return postToSampler(sampleSpec, 'schema');
}

export async function sampleForExampleManifests(
  exampleManifestUrl: string,
): Promise<ExampleManifest[]> {
  const sampleSpec: SampleSpec = {
    type: 'index',
    spec: {
      type: 'index',
      ioConfig: {
        type: 'index',
        firehose: { type: 'http', uris: [exampleManifestUrl] },
      },
      dataSchema: {
        dataSource: 'sample',
        parser: {
          type: 'string',
          parseSpec: {
            format: 'tsv',
            timestampSpec: {
              column: 'timestamp',
              missingValue: '2010-01-01T00:00:00Z',
            },
            dimensionsSpec: {},
            hasHeaderRow: true,
          },
        },
      },
    },
    samplerConfig: { numRows: 50, timeoutMs: 10000, skipCache: true },
  };

  const exampleData = await postToSampler(sampleSpec, 'example-manifest');

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
        spec,
      };
    } else {
      return;
    }
  });
}
