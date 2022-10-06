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

import { RefName, SqlExpression, SqlLiteral, SqlRef, SqlTableRef } from 'druid-query-toolkit';
import * as JSONBig from 'json-bigint-native';

import {
  DimensionSpec,
  inflateDimensionSpec,
  IngestionSpec,
  MetricSpec,
  QueryWithContext,
  TimestampSpec,
  Transform,
  upgradeSpec,
} from '../druid-models';
import { deepGet, filterMap, oneOf } from '../utils';

export function getSpecDatasourceName(spec: IngestionSpec): string {
  return deepGet(spec, 'spec.dataSchema.dataSource') || 'unknown_datasource';
}

function convertFilter(filter: any): SqlExpression {
  switch (filter.type) {
    case 'selector':
      return SqlRef.columnWithQuotes(filter.dimension).equal(filter.value);

    case 'in':
      return SqlRef.columnWithQuotes(filter.dimension).in(filter.values);

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

const SOURCE_REF = SqlTableRef.create('source');

export function convertSpecToSql(spec: any): QueryWithContext {
  if (!oneOf(spec.type, 'index_parallel', 'index', 'index_hadoop')) {
    throw new Error('Only index_parallel, index, and index_hadoop specs are supported');
  }
  spec = upgradeSpec(spec, true);

  const context: Record<string, any> = {
    finalizeAggregations: false,
    groupByEnableMultiValueUnnesting: false,
  };

  const lines: string[] = [];

  const rollup = deepGet(spec, 'spec.dataSchema.granularitySpec.rollup') ?? true;

  const timestampSpec: TimestampSpec = deepGet(spec, 'spec.dataSchema.timestampSpec');
  if (!timestampSpec) throw new Error(`spec.dataSchema.timestampSpec is not defined`);

  let dimensions = deepGet(spec, 'spec.dataSchema.dimensionsSpec.dimensions');
  if (!Array.isArray(dimensions)) {
    throw new Error(`spec.dataSchema.dimensionsSpec.dimensions must be an array`);
  }
  dimensions = dimensions.map(inflateDimensionSpec);

  let columns = dimensions.map((d: DimensionSpec) => ({
    name: d.name,
    type: d.type,
  }));

  const metricsSpec = deepGet(spec, 'spec.dataSchema.metricsSpec');
  if (Array.isArray(metricsSpec)) {
    columns = columns.concat(
      filterMap(metricsSpec, metricSpec =>
        metricSpec.fieldName
          ? {
              name: metricSpec.fieldName,
              type: metricSpecTypeToDataType(metricSpec.type),
            }
          : undefined,
      ),
    );
  }

  const transforms: Transform[] = deepGet(spec, 'spec.dataSchema.transformSpec.transforms') || [];
  if (!Array.isArray(transforms)) {
    throw new Error(`spec.dataSchema.transformSpec.transforms is not an array`);
  }

  let timeExpression: string;
  const column = timestampSpec.column || 'timestamp';
  const columnRef = SqlRef.column(column);
  const format = timestampSpec.format || 'auto';
  const timeTransform = transforms.find(t => t.name === '__time');
  if (timeTransform) {
    timeExpression = `REWRITE_[${timeTransform.expression}]_TO_SQL`;
  } else {
    switch (format) {
      case 'auto':
        columns.unshift({ name: column, type: 'string' });
        timeExpression = `CASE WHEN CAST(${columnRef} AS BIGINT) > 0 THEN MILLIS_TO_TIMESTAMP(CAST(${columnRef} AS BIGINT)) ELSE TIME_PARSE(${columnRef}) END`;
        break;

      case 'iso':
        columns.unshift({ name: column, type: 'string' });
        timeExpression = `TIME_PARSE(${columnRef})`;
        break;

      case 'posix':
        columns.unshift({ name: column, type: 'long' });
        timeExpression = `MILLIS_TO_TIMESTAMP(${columnRef} * 1000)`;
        break;

      case 'millis':
        columns.unshift({ name: column, type: 'long' });
        timeExpression = `MILLIS_TO_TIMESTAMP(${columnRef})`;
        break;

      case 'micro':
        columns.unshift({ name: column, type: 'long' });
        timeExpression = `MILLIS_TO_TIMESTAMP(${columnRef} / 1000)`;
        break;

      case 'nano':
        columns.unshift({ name: column, type: 'long' });
        timeExpression = `MILLIS_TO_TIMESTAMP(${columnRef} / 1000000)`;
        break;

      default:
        columns.unshift({ name: column, type: 'string' });
        timeExpression = `TIME_PARSE(${columnRef}, ${SqlLiteral.create(format)})`;
        break;
    }
  }

  if (timestampSpec.missingValue) {
    timeExpression = `COALESCE(${timeExpression}, TIME_PARSE(${SqlLiteral.create(
      timestampSpec.missingValue,
    )}))`;
  }

  timeExpression = convertQueryGranularity(
    timeExpression,
    deepGet(spec, 'spec.dataSchema.granularitySpec.queryGranularity'),
  );

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
    lines.push(`INSERT INTO ${SqlTableRef.create(dataSource)}`);
  } else {
    const overwrite = deepGet(spec, 'spec.ioConfig.dropExisting')
      ? 'WHERE ' +
        SqlExpression.fromTimeRefAndInterval(
          SqlRef.column('__time'),
          deepGet(spec, 'spec.dataSchema.granularitySpec.intervals'),
        )
      : 'ALL';

    lines.push(`REPLACE INTO ${SqlTableRef.create(dataSource)} OVERWRITE ${overwrite}`);
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
      `WITH ${SOURCE_REF} AS (`,
      `  SELECT *`,
      `  FROM ${SqlTableRef.create(inputSource.dataSource)}`,
      `  WHERE ${SqlExpression.fromTimeRefAndInterval(
        SqlRef.column('__time'),
        inputSource.interval,
      )}`,
      ')',
    );
  } else {
    lines.push(
      `WITH ${SOURCE_REF} AS (SELECT * FROM TABLE(`,
      `  EXTERN(`,
      `    ${SqlLiteral.create(JSONBig.stringify(inputSource))},`,
    );

    const inputFormat = deepGet(spec, 'spec.ioConfig.inputFormat');
    if (!inputFormat) throw new Error(`spec.ioConfig.inputFormat is not defined`);
    lines.push(
      `    ${SqlLiteral.create(JSONBig.stringify(inputFormat))},`,
      `    ${SqlLiteral.create(JSONBig.stringify(columns))}`,
      `  )`,
      `))`,
    );
  }

  lines.push(`SELECT`);

  if (transforms.length) {
    lines.push(
      `  --:ISSUE: The spec contained transforms that could not be automatically converted.`,
    );
  }

  const dimensionExpressions = [
    `  ${timeExpression} AS __time,${
      timeTransform ? ` --:ISSUE: Transform for __time could not be converted` : ''
    }`,
  ].concat(
    dimensions.flatMap((dimension: DimensionSpec) => {
      const dimensionName = dimension.name;
      const relevantTransform = transforms.find(t => t.name === dimensionName);
      return `  ${
        relevantTransform ? `REWRITE_[${relevantTransform.expression}]_TO_SQL AS ` : ''
      }${SqlRef.columnWithQuotes(dimensionName)},${
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
  lines.push(`FROM ${SOURCE_REF}`);

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
    lines.push(
      `CLUSTERED BY ${partitionDimensions.map(d => SqlRef.columnWithQuotes(d)).join(', ')}`,
    );
  }

  return {
    queryString: lines.join('\n'),
    queryContext: context,
  };
}

function convertQueryGranularity(
  timeExpression: string,
  queryGranularity: { type: string } | string | undefined,
) {
  if (!queryGranularity) return timeExpression;

  const effectiveQueryGranularity =
    typeof queryGranularity === 'string'
      ? queryGranularity
      : typeof queryGranularity.type === 'string'
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

function metricSpecTypeToDataType(metricSpecType: string): string {
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
  const ref = SqlRef.columnWithQuotes(metricSpec.fieldName);

  switch (metricSpec.type) {
    case 'longSum':
    case 'floatSum':
    case 'doubleSum':
      return `SUM(${ref})`;

    case 'longMin':
    case 'floatMin':
    case 'doubleMin':
      return `MIN(${ref})`;

    case 'longMax':
    case 'floatMax':
    case 'doubleMax':
      return `MAX(${ref})`;

    case 'doubleFirst':
    case 'floatFirst':
    case 'longFirst':
      return `EARLIEST(${ref})`;

    case 'stringFirst':
      return `EARLIEST(${ref}, ${SqlLiteral.create(metricSpec.maxStringBytes || 128)})`;

    case 'doubleLast':
    case 'floatLast':
    case 'longLast':
      return `LATEST(${ref})`;

    case 'stringLast':
      return `LATEST(${ref}, ${SqlLiteral.create(metricSpec.maxStringBytes || 128)})`;

    case 'thetaSketch':
      return `APPROX_COUNT_DISTINCT_DS_THETA(${ref}${extraArgs([metricSpec.size, 16384])})`;

    case 'HLLSketchBuild':
    case 'HLLSketchMerge':
      return `APPROX_COUNT_DISTINCT_DS_HLL(${ref}${extraArgs(
        [metricSpec.lgK, 12],
        [metricSpec.tgtHllType, 'HLL_4'],
      )})`;

    case 'quantilesDoublesSketch':
      // For consistency with the above this should be APPROX_QUANTILE_DS but that requires a post agg so it does not work quite right.
      return `DS_QUANTILES_SKETCH(${ref}${extraArgs([metricSpec.k, 128])})`;

    case 'hyperUnique':
      return `APPROX_COUNT_DISTINCT_BUILTIN(${ref})`;

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
  return ', ' + thingAndDefaults.map(([x, def]) => SqlLiteral.create(x ?? def)).join(', ');
}
