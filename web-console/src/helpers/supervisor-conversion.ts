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

import { C, F, L, SqlExpression, SqlQuery } from 'druid-query-toolkit';

interface MetricSpec {
  type: string;
  name?: string;
  fieldName?: string;
  maxStringBytes?: number;
  size?: number;
  lgK?: number;
  tgtHllType?: string;
  k?: number;
}

function extraArgs(...args: [any, any][]): string {
  const filtered = args.filter(
    ([value, defaultValue]) => value !== undefined && value !== defaultValue,
  );
  if (filtered.length === 0) return '';
  return (
    ', ' + filtered.map(([value]) => (typeof value === 'string' ? `'${value}'` : value)).join(', ')
  );
}

function metricSpecToSqlExpression(metricSpec: MetricSpec): SqlExpression | undefined {
  if (metricSpec.type === 'count') {
    return SqlExpression.parse('COUNT(*)');
  }

  if (!metricSpec.fieldName) return undefined;
  const column = C(metricSpec.fieldName);

  switch (metricSpec.type) {
    case 'longSum':
    case 'floatSum':
    case 'doubleSum':
      return F('SUM', column);

    case 'longMin':
    case 'floatMin':
    case 'doubleMin':
      return F('MIN', column);

    case 'longMax':
    case 'floatMax':
    case 'doubleMax':
      return F('MAX', column);

    case 'doubleFirst':
    case 'floatFirst':
    case 'longFirst':
      return F('EARLIEST', column);

    case 'stringFirst':
      return F('EARLIEST', column, L(metricSpec.maxStringBytes || 128));

    case 'doubleLast':
    case 'floatLast':
    case 'longLast':
      return F('LATEST', column);

    case 'stringLast':
      return F('LATEST', column, L(metricSpec.maxStringBytes || 128));

    case 'thetaSketch':
      return SqlExpression.parse(
        `APPROX_COUNT_DISTINCT_DS_THETA(${column}${extraArgs([metricSpec.size, 16384])})`,
      );

    case 'HLLSketchBuild':
    case 'HLLSketchMerge':
      return SqlExpression.parse(
        `APPROX_COUNT_DISTINCT_DS_HLL(${column}${extraArgs(
          [metricSpec.lgK, 12],
          [metricSpec.tgtHllType, 'HLL_4'],
        )})`,
      );

    case 'quantilesDoublesSketch':
      return SqlExpression.parse(`DS_QUANTILES_SKETCH(${column}${extraArgs([metricSpec.k, 128])})`);

    case 'hyperUnique':
      return F('APPROX_COUNT_DISTINCT_BUILTIN', column);

    default:
      // Unsupported: tDigestSketch, momentSketch, fixedBucketsHistogram
      return undefined;
  }
}

export interface SupervisorSpec {
  type: string;
  spec: {
    dataSchema: {
      dataSource: string;
      timestampSpec: {
        column: string;
        format: string;
      };
      dimensionsSpec?: {
        dimensions: (string | { name: string; type: string })[];
      };
      metricsSpec?: { name?: string; fieldName?: string; type: string }[];
      granularitySpec?: {
        segmentGranularity?: string;
        queryGranularity?: string | { type: string };
      };
    };
    ioConfig?: {
      topic?: string;
      inputSource?: {
        type: string;
        uris?: string[];
        baseDir?: string;
      };
      inputFormat?: Record<string, any>;
    };
  };
}

export interface SupervisorConversionOptions {
  fileLocation: string;
  fileType: string;
}

interface QueryWithContext {
  queryString: string;
  queryContext: Record<string, any>;
}

function extractDimensionName(dimension: string | { name: string; type: string }): string {
  return typeof dimension === 'string' ? dimension : dimension.name;
}

// Maps a dimension's native type to the EXTERN signature type so that typed dimensions
// (long/float/double/json) are not all declared as strings.
function dimensionTypeToSignatureType(dimensionType: string | undefined): string {
  switch (dimensionType) {
    case 'long':
    case 'float':
    case 'double':
    case 'string':
      return dimensionType.toUpperCase();

    case 'json':
      return 'COMPLEX<json>';

    default:
      // includes 'auto' and anything unrecognized
      return 'STRING';
  }
}

// Maps a metric's type to the native type of the raw input column it reads from, mirroring the
// ingestion-spec converter so that SUM/MIN/MAX/sketches aggregate over numeric inputs, not VARCHAR.
function metricFieldSignatureType(metricSpecType: string): string {
  const m = /^(long|float|double|string)/.exec(String(metricSpecType));
  if (m) return m[1].toUpperCase();

  switch (metricSpecType) {
    case 'thetaSketch':
    case 'HLLSketchBuild':
    case 'HLLSketchMerge':
    case 'quantilesDoublesSketch':
    case 'hyperUnique':
      return 'STRING';

    default:
      return 'DOUBLE';
  }
}

// ISO period for each supported query granularity, used to TIME_FLOOR the parsed timestamp so the
// generated rollup grouping matches the supervisor's queryGranularity.
const QUERY_GRANULARITY_TO_PERIOD: Record<string, string> = {
  second: 'PT1S',
  minute: 'PT1M',
  fifteen_minute: 'PT15M',
  thirty_minute: 'PT30M',
  hour: 'PT1H',
  day: 'P1D',
  week: 'P7D',
  month: 'P1M',
  quarter: 'P3M',
  year: 'P1Y',
};

function getQueryGranularityPeriod(
  queryGranularity: string | { type: string } | undefined,
): string | undefined {
  if (!queryGranularity) return undefined;
  const effective = typeof queryGranularity === 'string' ? queryGranularity : queryGranularity.type;
  if (!effective || effective.toLowerCase() === 'none') return undefined;
  return QUERY_GRANULARITY_TO_PERIOD[effective.toLowerCase()];
}

export function convertSupervisorToSql(
  supervisorSpec: SupervisorSpec,
  options: SupervisorConversionOptions,
): QueryWithContext {
  const { fileLocation, fileType } = options;
  const { dataSchema } = supervisorSpec.spec;

  if (!dataSchema) {
    throw new Error('Supervisor spec missing dataSchema');
  }

  const datasource = dataSchema.dataSource;
  if (!datasource) {
    throw new Error('Supervisor spec missing dataSource');
  }

  const timestampColumn = dataSchema.timestampSpec?.column || '__time';
  const timestampFormat = dataSchema.timestampSpec?.format || 'auto';
  const queryGranularityPeriod = getQueryGranularityPeriod(
    dataSchema.granularitySpec?.queryGranularity,
  );

  // Extract dimensions (and remember their native types for the EXTERN signature)
  const rawDimensions = dataSchema.dimensionsSpec?.dimensions || [];
  const dimensions = rawDimensions.map(extractDimensionName);
  const dimensionSignatureType: Record<string, string> = {};
  for (const dimension of rawDimensions) {
    if (typeof dimension === 'string') {
      dimensionSignatureType[dimension] = 'STRING';
    } else {
      dimensionSignatureType[dimension.name] = dimensionTypeToSignatureType(dimension.type);
    }
  }

  // Extract and convert metrics to SQL aggregations
  const metricSpecs = dataSchema.metricsSpec || [];
  const metricExpressions: { expr: SqlExpression; name: string }[] = [];

  for (const metricSpec of metricSpecs) {
    const sqlExpr = metricSpecToSqlExpression(metricSpec as MetricSpec);
    if (sqlExpr && metricSpec.name) {
      metricExpressions.push({ expr: sqlExpr, name: metricSpec.name });
    }
  }

  // Determine if we need GROUP BY (if we have aggregations)
  const hasAggregations = metricExpressions.length > 0;

  // Build column list for SELECT
  // If no aggregations, just select dimensions as-is
  // If aggregations exist, dimensions become GROUP BY and we add aggregations
  const selectColumns = dimensions;

  // All columns for EXTERN (timestamp + dimensions + every fieldName the metrics reference), each
  // declared with its native type. Dimensions win over metric fields when a name appears in both,
  // and the timestamp column is read as a string for TIME_PARSE to parse.
  const columnSignatureType = new Map<string, string>();
  columnSignatureType.set(timestampColumn, 'STRING');
  for (const dimension of dimensions) {
    columnSignatureType.set(dimension, dimensionSignatureType[dimension] || 'STRING');
  }
  for (const metricSpec of metricSpecs) {
    if (metricSpec.fieldName && !columnSignatureType.has(metricSpec.fieldName)) {
      columnSignatureType.set(metricSpec.fieldName, metricFieldSignatureType(metricSpec.type));
    }
  }

  // Create input format based on file type
  const inputFormatType = fileType === 'json' ? 'json' : fileType === 'csv' ? 'csv' : fileType;

  // Build proper Druid input source
  let inputSource: any;
  if (fileLocation.startsWith('s3://')) {
    inputSource = {
      type: 's3',
      uris: [fileLocation],
    };
    // Add objectGlob based on file type if it's a directory
    if (fileLocation.endsWith('/')) {
      inputSource.objectGlob = `**.${inputFormatType}`;
    }
  } else if (fileLocation.startsWith('gs://')) {
    inputSource = {
      type: 'google',
      uris: [fileLocation],
    };
  } else if (fileLocation.startsWith('http://') || fileLocation.startsWith('https://')) {
    inputSource = {
      type: 'http',
      uris: [fileLocation],
    };
  } else {
    // Default to local for file:// or absolute paths
    inputSource = {
      type: 'local',
      baseDir: fileLocation.replace('file://', ''),
      filter: `*.${inputFormatType}`,
    };
  }

  // Preserve the supervisor's inputFormat settings (CSV columns/header, JSON flattenSpec, etc.) when
  // present, overriding only the type with the file type the user selected for the backfill files.
  const supervisorInputFormat = supervisorSpec.spec.ioConfig?.inputFormat;
  const inputFormat = supervisorInputFormat
    ? { ...supervisorInputFormat, type: inputFormatType }
    : { type: inputFormatType };

  // Build EXTERN expression. L() emits a properly single-quote-escaped SQL string literal.
  const columnSchema = Array.from(columnSignatureType, ([name, type]) => ({ name, type }));
  const externExpression = F(
    'EXTERN',
    L(JSON.stringify(inputSource)),
    L(JSON.stringify(inputFormat)),
    L(JSON.stringify(columnSchema)),
  );

  // Build SELECT expressions
  const selectExpressions: SqlExpression[] = selectColumns.map(col => C(col));

  // Add metric aggregations
  for (const { expr, name } of metricExpressions) {
    selectExpressions.push(expr.as(name));
  }

  // Add timestamp parsing. L() escapes any single quotes in custom Joda formats (e.g. a quoted
  // literal 'T'), and the query granularity (if any) is applied with TIME_FLOOR so the rollup
  // grouping matches the supervisor.
  const parsedTimestamp =
    timestampFormat === 'auto'
      ? F('TIME_PARSE', C(timestampColumn))
      : F('TIME_PARSE', C(timestampColumn), L(timestampFormat));

  const timeExpression = queryGranularityPeriod
    ? F('TIME_FLOOR', parsedTimestamp, L(queryGranularityPeriod))
    : parsedTimestamp;

  selectExpressions.push(timeExpression.as('__time'));

  // Build the query using druid-query-toolkit
  let query = SqlQuery.from(F('TABLE', externExpression));

  // Add select expressions one by one
  for (const expr of selectExpressions) {
    query = query.addSelect(expr);
  }

  // Add GROUP BY if we have aggregations
  if (hasAggregations) {
    const groupByExprs: any[] = [timeExpression];
    dimensions.forEach(d => groupByExprs.push(C(d)));
    query = query.changeGroupByExpressions(groupByExprs as any);
  }

  // Convert to string and manually add INSERT, PARTITIONED BY, and CLUSTERED BY
  // because the query builder API is giving us trouble
  let sqlString = query.toString();

  // Prepend INSERT INTO
  sqlString = `INSERT INTO ${C(datasource)}\n${sqlString}`;

  // Append PARTITIONED BY using the supervisor's segment granularity (defaulting to DAY)
  const segmentGranularity = dataSchema.granularitySpec?.segmentGranularity;
  const partitionedBy = segmentGranularity
    ? segmentGranularity.toLowerCase() === 'all'
      ? 'ALL TIME'
      : segmentGranularity.toUpperCase()
    : 'DAY';
  sqlString += `\nPARTITIONED BY ${partitionedBy}`;

  // Append CLUSTERED BY
  if (dimensions.length > 0) {
    const clusterColumns = dimensions
      .slice(0, 2)
      .map(d => C(d))
      .join(', ');
    sqlString += `\nCLUSTERED BY ${clusterColumns}`;
  }

  return {
    queryString: sqlString,
    queryContext: {},
  };
}
