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

function extraArgs(...args: Array<[any, any]>): string {
  const filtered = args.filter(([value, defaultValue]) => value !== undefined && value !== defaultValue);
  if (filtered.length === 0) return '';
  return ', ' + filtered.map(([value]) => (typeof value === 'string' ? `'${value}'` : value)).join(', ');
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
      return SqlExpression.parse(`APPROX_COUNT_DISTINCT_DS_THETA(${column}${extraArgs([metricSpec.size, 16384])})`);

    case 'HLLSketchBuild':
    case 'HLLSketchMerge':
      return SqlExpression.parse(`APPROX_COUNT_DISTINCT_DS_HLL(${column}${extraArgs([metricSpec.lgK, 12], [metricSpec.tgtHllType, 'HLL_4'])})`);

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
        dimensions: Array<string | { name: string; type: string }>;
      };
      metricsSpec?: Array<{ name?: string; fieldName?: string; type: string }>;
    };
    ioConfig?: {
      topic?: string;
      inputSource?: {
        type: string;
        uris?: string[];
        baseDir?: string;
      };
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

  // Extract dimensions
  const dimensions = (dataSchema.dimensionsSpec?.dimensions || []).map(extractDimensionName);

  // Extract and convert metrics to SQL aggregations
  const metricSpecs = dataSchema.metricsSpec || [];
  const metricExpressions: Array<{ expr: SqlExpression; name: string }> = [];
  
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
  
  // All columns for EXTERN (includes timestamp + all raw input columns)
  // For EXTERN, we need all the fieldNames that metrics reference
  const metricFieldNames = metricSpecs
    .map(m => m.fieldName)
    .filter((name): name is string => !!name);
  const allExternColumns = [timestampColumn, ...dimensions, ...metricFieldNames];
  const uniqueExternColumns = Array.from(new Set(allExternColumns));

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

  // Build EXTERN expression with proper escaping
  const columnSchema = uniqueExternColumns.map(col => ({name: col, type: 'string'}));
  const columnSchemaJson = JSON.stringify(columnSchema);
  const inputSourceJson = JSON.stringify(inputSource);
  const inputFormatJson = JSON.stringify({type: inputFormatType});
  
  const externExpression = F(
    'EXTERN',
    SqlExpression.parse(`'${inputSourceJson.replace(/'/g, "''")}'`),
    SqlExpression.parse(`'${inputFormatJson.replace(/'/g, "''")}'`),
    SqlExpression.parse(`'${columnSchemaJson.replace(/'/g, "''")}'`),
  );

  // Build SELECT expressions
  const selectExpressions: SqlExpression[] = selectColumns.map(col => C(col));

  // Add metric aggregations
  for (const { expr, name } of metricExpressions) {
    selectExpressions.push(expr.as(name));
  }

  // Add timestamp parsing
  const timeParseExpression =
    timestampFormat === 'auto'
      ? F('TIME_PARSE', C(timestampColumn))
      : F('TIME_PARSE', C(timestampColumn), SqlExpression.parse(`'${timestampFormat}'`));

  selectExpressions.push(timeParseExpression.as('__time'));

  // Build the query using druid-query-toolkit
  let query = SqlQuery.from(F('TABLE', externExpression));
  
  // Add select expressions one by one
  for (const expr of selectExpressions) {
    query = query.addSelect(expr);
  }

  // Add GROUP BY if we have aggregations
  if (hasAggregations) {
    const groupByExprs: any[] = [timeParseExpression];
    dimensions.forEach(d => groupByExprs.push(C(d)));
    query = query.changeGroupByExpressions(groupByExprs as any);
  }

  // Convert to string and manually add INSERT, PARTITIONED BY, and CLUSTERED BY
  // because the query builder API is giving us trouble
  let sqlString = query.toString();
  
  // Prepend INSERT INTO
  sqlString = `INSERT INTO ${C(datasource)}\n${sqlString}`;

  // Append PARTITIONED BY
  sqlString += `\nPARTITIONED BY DAY`;

  // Append CLUSTERED BY
  if (dimensions.length > 0) {
    const clusterColumns = dimensions.slice(0, 2).map(d => C(d)).join(', ');
    sqlString += `\nCLUSTERED BY ${clusterColumns}`;
  }

  return {
    queryString: sqlString,
    queryContext: {},
  };
}