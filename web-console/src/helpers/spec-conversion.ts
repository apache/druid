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

import {
  C,
  dedupe,
  L,
  RefName,
  SqlColumnDeclaration,
  SqlExpression,
  SqlType,
  T,
} from '@druid-toolkit/query';
import * as JSONBig from 'json-bigint-native';

import type {
  DimensionSpec,
  IngestionSpec,
  MetricSpec,
  QueryWithContext,
  TimestampSpec,
  Transform,
} from '../druid-models';
import { inflateDimensionSpec, NO_SUCH_COLUMN, TIME_COLUMN, upgradeSpec } from '../druid-models';
import { deepGet, filterMap, nonEmptyArray, oneOf } from '../utils';

export function getSpecDatasourceName(spec: Partial<IngestionSpec>): string {
  return deepGet(spec, 'spec.dataSchema.dataSource') || 'unknown_datasource';
}

function convertFilter(filter: any): SqlExpression {
  switch (filter.type) {
    case 'selector':
      return C(filter.dimension).equal(filter.value);

    case 'in':
      return C(filter.dimension).in(filter.values);

    case 'not':
      return convertFilter(filter.field).not();

    case 'and':
      return SqlExpression.and(filter.fields.map(convertFilter));

    case 'or':
      return SqlExpression.or(filter.fields.map(convertFilter));

    default:
      throw new Error(`unknown filter type '${filter.type}'`);
  }
}

const SOURCE_TABLE = T('source');

export function convertSpecToSql(spec: any): QueryWithContext {
  if (!oneOf(spec.type, 'index_parallel', 'index', 'index_hadoop')) {
    throw new Error('Only index_parallel, index, and index_hadoop specs are supported');
  }
  spec = upgradeSpec(spec, true);

  const context: Record<string, any> = {
    finalizeAggregations: false,
    groupByEnableMultiValueUnnesting: false,
    arrayIngestMode: 'array',
  };

  const indexSpec = deepGet(spec, 'spec.tuningConfig.indexSpec');
  if (indexSpec) {
    context.indexSpec = indexSpec;
  }

  const lines: string[] = [];

  const rollup = deepGet(spec, 'spec.dataSchema.granularitySpec.rollup') ?? true;

  if (nonEmptyArray(deepGet(spec, 'spec.dataSchema.dimensionsSpec.spatialDimensions'))) {
    throw new Error(`spatialDimensions are not currently supported in SQL-based ingestion`);
  }

  const timestampSpec: TimestampSpec = deepGet(spec, 'spec.dataSchema.timestampSpec');
  if (!timestampSpec) throw new Error(`spec.dataSchema.timestampSpec is not defined`);

  const specDimensions: (string | DimensionSpec)[] = deepGet(
    spec,
    'spec.dataSchema.dimensionsSpec.dimensions',
  );
  if (!Array.isArray(specDimensions)) {
    throw new Error(`spec.dataSchema.dimensionsSpec.dimensions must be an array`);
  }
  const dimensions = specDimensions.map(inflateDimensionSpec);

  let columnDeclarations: SqlColumnDeclaration[] = dimensions.map(d =>
    SqlColumnDeclaration.create(d.name, dimensionSpecToSqlType(d)),
  );

  const metricsSpec = deepGet(spec, 'spec.dataSchema.metricsSpec');
  if (Array.isArray(metricsSpec)) {
    columnDeclarations = columnDeclarations.concat(
      filterMap(metricsSpec, metricSpec =>
        metricSpec.fieldName
          ? SqlColumnDeclaration.create(
              metricSpec.fieldName,
              SqlType.fromNativeType(metricSpecTypeToNativeDataInputType(metricSpec.type)),
            )
          : undefined,
      ),
    );
  }

  columnDeclarations = dedupe(columnDeclarations, d => d.getColumnName());

  const transforms: Transform[] = deepGet(spec, 'spec.dataSchema.transformSpec.transforms') || [];
  if (!Array.isArray(transforms)) {
    throw new Error(`spec.dataSchema.transformSpec.transforms is not an array`);
  }

  let timeExpression: string;
  const timestampColumnName = timestampSpec.column || 'timestamp';
  const timeTransform = transforms.find(t => t.name === TIME_COLUMN);
  if (timestampColumnName === NO_SUCH_COLUMN) {
    timeExpression = timestampSpec.missingValue
      ? `TIME_PARSE(${L(timestampSpec.missingValue)})`
      : `TIMESTAMP '1970-01-01'`;
  } else {
    const timestampColumn = C(timestampColumnName);
    const format = timestampSpec.format || 'auto';
    if (timeTransform) {
      timeExpression = `REWRITE_[${timeTransform.expression}]_TO_SQL`;
    } else if (timestampColumnName === TIME_COLUMN) {
      timeExpression = String(timestampColumn);
      columnDeclarations.unshift(SqlColumnDeclaration.create(timestampColumnName, SqlType.BIGINT));
    } else {
      let timestampColumnType: SqlType;
      switch (format) {
        case 'auto':
          timestampColumnType = SqlType.VARCHAR;
          timeExpression = `CASE WHEN CAST(${timestampColumn} AS BIGINT) > 0 THEN MILLIS_TO_TIMESTAMP(CAST(${timestampColumn} AS BIGINT)) ELSE TIME_PARSE(TRIM(${timestampColumn})) END`;
          break;

        case 'iso':
          timestampColumnType = SqlType.VARCHAR;
          timeExpression = `TIME_PARSE(${timestampColumn})`;
          break;

        case 'posix':
          timestampColumnType = SqlType.BIGINT;
          timeExpression = `MILLIS_TO_TIMESTAMP(${timestampColumn} * 1000)`;
          break;

        case 'millis':
          timestampColumnType = SqlType.BIGINT;
          timeExpression = `MILLIS_TO_TIMESTAMP(${timestampColumn})`;
          break;

        case 'micro':
          timestampColumnType = SqlType.BIGINT;
          timeExpression = `MILLIS_TO_TIMESTAMP(${timestampColumn} / 1000)`;
          break;

        case 'nano':
          timestampColumnType = SqlType.BIGINT;
          timeExpression = `MILLIS_TO_TIMESTAMP(${timestampColumn} / 1000000)`;
          break;

        default:
          timestampColumnType = SqlType.VARCHAR;
          timeExpression = `TIME_PARSE(${timestampColumn}, ${L(format)})`;
          break;
      }
      columnDeclarations.unshift(
        SqlColumnDeclaration.create(timestampColumnName, timestampColumnType),
      );
    }

    if (timestampSpec.missingValue) {
      timeExpression = `COALESCE(${timeExpression}, TIME_PARSE(${L(timestampSpec.missingValue)}))`;
    }

    timeExpression = convertQueryGranularity(
      timeExpression,
      deepGet(spec, 'spec.dataSchema.granularitySpec.queryGranularity'),
    );
  }

  lines.push(`-- This SQL query was auto generated from an ingestion spec`);

  const maxNumConcurrentSubTasks = deepGet(spec, 'spec.tuningConfig.maxNumConcurrentSubTasks');
  if (maxNumConcurrentSubTasks > 1) {
    context.maxNumTasks = maxNumConcurrentSubTasks + 1;
  }

  const maxParseExceptions = deepGet(spec, 'spec.tuningConfig.maxParseExceptions');
  if (typeof maxParseExceptions === 'number') {
    context.maxParseExceptions = maxParseExceptions;
  }

  const dataSource = deepGet(spec, 'spec.dataSchema.dataSource');
  if (typeof dataSource !== 'string') throw new Error(`spec.dataSchema.dataSource is not a string`);

  if (deepGet(spec, 'spec.ioConfig.appendToExisting')) {
    lines.push(`INSERT INTO ${T(dataSource)}`);
  } else {
    const overwrite = deepGet(spec, 'spec.ioConfig.dropExisting')
      ? 'WHERE ' +
        SqlExpression.fromTimeExpressionAndInterval(
          C('__time'),
          deepGet(spec, 'spec.dataSchema.granularitySpec.intervals'),
        )
      : 'ALL';

    lines.push(`REPLACE INTO ${T(dataSource)} OVERWRITE ${overwrite}`);
  }

  let inputSource: any;
  if (oneOf(spec.type, 'index_parallel', 'index')) {
    inputSource = deepGet(spec, 'spec.ioConfig.inputSource');
    if (!inputSource) throw new Error(`spec.ioConfig.inputSource is not defined`);
  } else {
    // index_hadoop
    const inputSpec = deepGet(spec, 'spec.ioConfig.inputSpec');
    if (!inputSpec) throw new Error(`spec.ioConfig.inputSpec is not defined`);
    if (inputSpec.type !== 'static') {
      throw new Error(`can only convert when spec.ioConfig.inputSpec.type = 'static'`);
    }

    const paths = inputSpec.paths.split(',');
    const firstPath = paths[0];
    if (firstPath.startsWith('s3://')) {
      inputSource = { type: 's3', uris: paths };
    } else if (firstPath.startsWith('gs://')) {
      inputSource = { type: 'google', uris: paths };
    } else if (firstPath.startsWith('hdfs://')) {
      inputSource = { type: 'hdfs', paths };
    } else {
      throw new Error('unsupported');
    }
  }

  if (inputSource.type === 'druid') {
    lines.push(
      `WITH ${SOURCE_TABLE} AS (`,
      `  SELECT *`,
      `  FROM ${T(inputSource.dataSource)}`,
      `  WHERE ${SqlExpression.fromTimeExpressionAndInterval(C('__time'), inputSource.interval)}`,
      ')',
    );
  } else {
    lines.push(
      `WITH ${SOURCE_TABLE} AS (SELECT * FROM TABLE(`,
      `  EXTERN(`,
      `    ${L(JSONBig.stringify(inputSource))},`,
    );

    const inputFormat = deepGet(spec, 'spec.ioConfig.inputFormat');
    if (!inputFormat) throw new Error(`spec.ioConfig.inputFormat is not defined`);
    lines.push(
      `    ${L(JSONBig.stringify(inputFormat))}`,
      `  )`,
      `) EXTEND (${columnDeclarations.join(', ')}))`,
    );
  }

  lines.push(`SELECT`);

  if (transforms.length) {
    lines.push(
      `  --:ISSUE: The spec contained transforms that could not be automatically converted.`,
    );
  }

  const dimensionExpressions = [
    `  ${timeExpression} AS "__time",${
      timeTransform ? ` --:ISSUE: Transform for __time could not be converted` : ''
    }`,
  ].concat(
    dimensions.flatMap((dimension: DimensionSpec) => {
      const dimensionName = dimension.name;
      const relevantTransform = transforms.find(t => t.name === dimensionName);
      return `  ${
        relevantTransform ? `REWRITE_[${relevantTransform.expression}]_TO_SQL AS ` : ''
      }${C(dimensionName)},${
        relevantTransform ? ` --:ISSUE: Transform for dimension could not be converted` : ''
      }`;
    }),
  );

  const selectExpressions = dimensionExpressions.concat(
    Array.isArray(metricsSpec)
      ? metricsSpec.map(metricSpec => `  ${metricSpecToSelect(metricSpec)},`)
      : [],
  );

  // Remove trailing comma from last expression
  selectExpressions[selectExpressions.length - 1] = selectExpressions[selectExpressions.length - 1]
    .replace(/,$/, '')
    .replace(/,(\s+--)/, '$1');

  lines.push(selectExpressions.join('\n'));
  lines.push(`FROM ${SOURCE_TABLE}`);

  const filter = deepGet(spec, 'spec.dataSchema.transformSpec.filter');
  if (filter) {
    try {
      lines.push(`WHERE ${convertFilter(filter)}`);
    } catch {
      lines.push(
        `WHERE REWRITE_[${JSONBig.stringify(
          filter,
        )}]_TO_SQL --:ISSUE: The spec contained a filter that could not be automatically converted, please convert it manually`,
      );
    }
  }

  if (rollup) {
    lines.push(`GROUP BY ${dimensionExpressions.map((_, i) => i + 1).join(', ')}`);
  }

  const segmentGranularity = deepGet(spec, 'spec.dataSchema.granularitySpec.segmentGranularity');
  if (typeof segmentGranularity !== 'string') {
    throw new Error(`spec.dataSchema.granularitySpec.segmentGranularity is not a string`);
  }
  lines.push(
    `PARTITIONED BY ${
      segmentGranularity.toLowerCase() === 'all' ? 'ALL TIME' : segmentGranularity.toUpperCase()
    }`,
  );

  const partitionsSpec = deepGet(spec, 'spec.tuningConfig.partitionsSpec') || {};
  const partitionDimensions =
    partitionsSpec.partitionDimensions ||
    (partitionsSpec.partitionDimension ? [partitionsSpec.partitionDimension] : undefined);
  if (Array.isArray(partitionDimensions)) {
    lines.push(`CLUSTERED BY ${partitionDimensions.map(d => C(d)).join(', ')}`);
  }

  return {
    queryString: lines.join('\n'),
    queryContext: context,
  };
}

function convertQueryGranularity(
  timeExpression: string,
  queryGranularity: { type: unknown } | string | undefined,
) {
  if (!queryGranularity) return timeExpression;

  const effectiveQueryGranularity =
    typeof queryGranularity === 'string'
      ? queryGranularity
      : typeof queryGranularity?.type === 'string'
      ? queryGranularity.type
      : undefined;

  if (effectiveQueryGranularity) {
    const queryGranularitySql = QUERY_GRANULARITY_MAP[effectiveQueryGranularity.toLowerCase()];

    if (queryGranularitySql) {
      return queryGranularitySql.replace('?', timeExpression);
    }
  }

  throw new Error(`spec.dataSchema.granularitySpec.queryGranularity is not recognized`);
}

const QUERY_GRANULARITY_MAP: Record<string, string> = {
  none: `?`,
  second: `TIME_FLOOR(?, 'PT1S')`,
  minute: `TIME_FLOOR(?, 'PT1M')`,
  fifteen_minute: `TIME_FLOOR(?, 'PT15M')`,
  thirty_minute: `TIME_FLOOR(?, 'PT30M')`,
  hour: `TIME_FLOOR(?, 'PT1H')`,
  day: `TIME_FLOOR(?, 'P1D')`,
  week: `TIME_FLOOR(?, 'P7D')`,
  month: `TIME_FLOOR(?, 'P1M')`,
  quarter: `TIME_FLOOR(?, 'P3M')`,
  year: `TIME_FLOOR(?, 'P1Y')`,
};

function dimensionSpecToSqlType(dimensionSpec: DimensionSpec): SqlType {
  switch (dimensionSpec.type) {
    case 'auto':
      if (dimensionSpec.castToType) {
        return SqlType.fromNativeType(dimensionSpec.castToType);
      } else {
        return SqlType.VARCHAR;
      }

    case 'json':
      return SqlType.fromNativeType('COMPLEX<json>');

    default:
      return SqlType.fromNativeType(dimensionSpec.type);
  }
}

function metricSpecTypeToNativeDataInputType(metricSpecType: string): string {
  const m = /^(long|float|double|string)/.exec(String(metricSpecType));
  if (m) return m[1];

  switch (metricSpecType) {
    case 'thetaSketch':
    case 'HLLSketchBuild':
    case 'HLLSketchMerge':
    case 'quantilesDoublesSketch':
    case 'momentSketch':
    case 'fixedBucketsHistogram':
    case 'hyperUnique':
      return 'string';
  }

  return 'double';
}

function metricSpecToSelect(metricSpec: MetricSpec): string {
  const name = metricSpec.name;
  const expression = metricSpecToSqlExpression(metricSpec);
  if (!name || !expression) {
    return `-- could not convert metric: ${JSONBig.stringify(metricSpec)}`;
  }

  return `${expression} AS ${RefName.create(name, true)}`;
}

function metricSpecToSqlExpression(metricSpec: MetricSpec): string | undefined {
  if (metricSpec.type === 'count') {
    return `COUNT(*)`; // count is special as it does not have a fieldName
  }

  if (!metricSpec.fieldName) return;
  const column = C(metricSpec.fieldName);

  switch (metricSpec.type) {
    case 'longSum':
    case 'floatSum':
    case 'doubleSum':
      return `SUM(${column})`;

    case 'longMin':
    case 'floatMin':
    case 'doubleMin':
      return `MIN(${column})`;

    case 'longMax':
    case 'floatMax':
    case 'doubleMax':
      return `MAX(${column})`;

    case 'doubleFirst':
    case 'floatFirst':
    case 'longFirst':
      return `EARLIEST(${column})`;

    case 'stringFirst':
      return `EARLIEST(${column}, ${L(metricSpec.maxStringBytes || 128)})`;

    case 'doubleLast':
    case 'floatLast':
    case 'longLast':
      return `LATEST(${column})`;

    case 'stringLast':
      return `LATEST(${column}, ${L(metricSpec.maxStringBytes || 128)})`;

    case 'thetaSketch':
      return `APPROX_COUNT_DISTINCT_DS_THETA(${column}${extraArgs([metricSpec.size, 16384])})`;

    case 'HLLSketchBuild':
    case 'HLLSketchMerge':
      return `APPROX_COUNT_DISTINCT_DS_HLL(${column}${extraArgs(
        [metricSpec.lgK, 12],
        [metricSpec.tgtHllType, 'HLL_4'],
      )})`;

    case 'quantilesDoublesSketch':
      // For consistency with the above this should be APPROX_QUANTILE_DS but that requires a post agg so it does not work quite right.
      return `DS_QUANTILES_SKETCH(${column}${extraArgs([metricSpec.k, 128])})`;

    case 'hyperUnique':
      return `APPROX_COUNT_DISTINCT_BUILTIN(${column})`;

    default:
      // The following things are (knowingly) not supported:
      // tDigestSketch, momentSketch, fixedBucketsHistogram
      return;
  }
}

function extraArgs(...thingAndDefaults: [any, any?][]): string {
  while (
    thingAndDefaults.length &&
    typeof thingAndDefaults[thingAndDefaults.length - 1][0] === 'undefined'
  ) {
    thingAndDefaults.pop();
  }

  if (!thingAndDefaults.length) return '';
  return ', ' + thingAndDefaults.map(([x, def]) => L(x ?? def)).join(', ');
}
