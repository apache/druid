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
  SqlExpression,
  SqlFunction,
  SqlLiteral,
  SqlPartitionedByClause,
  SqlQuery,
  SqlReplaceClause,
  SqlWithPart,
  T,
} from '@druid-toolkit/query';

import { filterMap, oneOf } from '../../utils';
import type { ExternalConfig } from '../external-config/external-config';
import {
  externalConfigToInitDimensions,
  externalConfigToTableExpression,
  fitExternalConfigPattern,
} from '../external-config/external-config';
import type { ArrayMode } from '../ingestion-spec/ingestion-spec';
import { guessDataSourceNameFromInputSource } from '../ingestion-spec/ingestion-spec';

export type IngestMode = 'insert' | 'replace';

function removeTopLevelArrayToMvOrUndefined(dimension: SqlExpression): SqlExpression | undefined {
  const ex = dimension.getUnderlyingExpression();
  return ex instanceof SqlFunction && ex.getEffectiveFunctionName() === 'ARRAY_TO_MV'
    ? ex.getArg(0)
    : undefined;
}

export interface IngestQueryPattern {
  destinationTableName: string;
  mode: IngestMode;
  overwriteWhere?: SqlExpression;
  mainExternalName: string;
  mainExternalConfig: ExternalConfig;
  filters: readonly SqlExpression[];
  dimensions: readonly SqlExpression[];
  metrics?: readonly SqlExpression[];
  partitionedBy: string;
  clusteredBy: readonly number[];
}

export function externalConfigToIngestQueryPattern(
  config: ExternalConfig,
  timeExpression: SqlExpression | undefined,
  partitionedByHint: string | undefined,
  arrayMode: ArrayMode,
): IngestQueryPattern {
  return {
    destinationTableName: guessDataSourceNameFromInputSource(config.inputSource) || 'data',
    mode: 'replace',
    mainExternalName: 'ext',
    mainExternalConfig: config,
    filters: [],
    dimensions: externalConfigToInitDimensions(config, timeExpression, arrayMode),
    partitionedBy: partitionedByHint || (timeExpression ? 'day' : 'all'),
    clusteredBy: [],
  };
}

export function getQueryPatternExpression(
  pattern: IngestQueryPattern,
  index: number,
): SqlExpression | undefined {
  const { dimensions, metrics } = pattern;
  if (index < dimensions.length) {
    return dimensions[index];
  } else if (metrics) {
    return metrics[index - dimensions.length];
  }
  return;
}

export function getQueryPatternExpressionType(
  pattern: IngestQueryPattern,
  index: number,
): 'dimension' | 'metric' | undefined {
  const { dimensions, metrics } = pattern;
  if (index < dimensions.length) {
    return 'dimension';
  } else if (metrics) {
    return 'metric';
  }
  return;
}

export function changeQueryPatternExpression(
  pattern: IngestQueryPattern,
  index: number,
  type: 'dimension' | 'metric',
  ex: SqlExpression | undefined,
): IngestQueryPattern {
  let { dimensions, metrics } = pattern;
  if (index === -1) {
    if (ex) {
      if (type === 'dimension') {
        dimensions = dimensions.concat(ex);
      } else if (metrics) {
        metrics = metrics.concat(ex);
      }
    }
  } else if (index < dimensions.length) {
    dimensions = filterMap(dimensions, (d, i) => (i === index ? ex : d));
  } else if (metrics) {
    const metricIndex = index - dimensions.length;
    metrics = filterMap(metrics, (m, i) => (i === metricIndex ? ex : m));
  }
  return { ...pattern, dimensions, metrics };
}

function verifyHasOutputName(expression: SqlExpression): void {
  if (expression.getOutputName()) return;
  throw new Error(`${expression} must have an AS alias`);
}

export function fitIngestQueryPattern(query: SqlQuery): IngestQueryPattern {
  if (query.explain) throw new Error(`Can not use EXPLAIN in the data loader flow`);
  if (query.havingClause) throw new Error(`Can not use HAVING in the data loader flow`);
  if (query.orderByClause) throw new Error(`Can not USE ORDER BY in the data loader flow`);
  if (query.limitClause) throw new Error(`Can not use LIMIT in the data loader flow`);
  if (query.offsetClause) throw new Error(`Can not use OFFSET in the data loader flow`);
  if (query.unionQuery) throw new Error(`Can not use UNION in the data loader flow`);

  if (query.hasStarInSelect()) {
    throw new Error(
      `Can not have * in SELECT in the data loader flow, the columns need to be explicitly listed out`,
    );
  }

  let destinationTableName: string;
  let mode: IngestMode;
  let overwriteWhere: SqlExpression | undefined;
  if (query.insertClause) {
    mode = 'insert';
    destinationTableName = query.insertClause.table.getName();
  } else if (query.replaceClause) {
    mode = 'replace';
    destinationTableName = query.replaceClause.table.getName();
    overwriteWhere = query.replaceClause.whereClause?.expression;
  } else {
    throw new Error(`Must have an INSERT or REPLACE clause`);
  }

  const withParts = query.getWithParts();
  if (withParts.length !== 1) {
    throw new Error(`Must have exactly one with part`);
  }

  const { table: withTable, columns: withColumns, query: withQuery } = withParts[0];
  if (withColumns) {
    throw new Error(`Can not have columns in the WITH expression`);
  }
  if (!(withQuery instanceof SqlQuery)) {
    throw new Error(`The body of the WITH expression must be a query`);
  }

  const mainExternalName = withTable.name;
  const mainExternalConfig = fitExternalConfigPattern(withQuery);

  const filters = query.getWhereExpression()?.decomposeViaAnd() || [];

  let dimensions: readonly SqlExpression[];
  let metrics: readonly SqlExpression[] | undefined;
  if (query.hasGroupBy()) {
    dimensions = query.getGroupedSelectExpressions();
    metrics = query.getAggregateSelectExpressions();
    metrics.forEach(verifyHasOutputName);
  } else {
    dimensions = query.getSelectExpressionsArray();
  }

  dimensions.forEach(verifyHasOutputName);

  const partitionedByClause = query.partitionedByClause;
  if (!partitionedByClause) {
    throw new Error(`Must have a PARTITIONED BY clause`);
  }
  let partitionedBy: string;
  if (partitionedByClause.expression) {
    if (partitionedByClause.expression instanceof SqlLiteral) {
      partitionedBy = String(partitionedByClause.expression.value).toLowerCase();
    } else {
      partitionedBy = '';
    }
  } else {
    partitionedBy = 'all';
  }
  if (!oneOf(partitionedBy, 'hour', 'day', 'month', 'year', 'all')) {
    throw new Error(`Must partition by HOUR, DAY, MONTH, YEAR, or ALL TIME`);
  }

  const clusteredByExpressions = query.clusteredByClause
    ? query.clusteredByClause.expressions.values
    : [];
  const clusteredBy = clusteredByExpressions.map(clusteredByExpression => {
    const selectIndex = query.getSelectIndexForExpression(clusteredByExpression, true);
    if (selectIndex === -1) {
      throw new Error(
        `Invalid CLUSTERED BY expression ${clusteredByExpression}, can only partition on a dimension`,
      );
    }
    return selectIndex;
  });

  return {
    destinationTableName,
    mode,
    overwriteWhere,
    mainExternalName,
    mainExternalConfig,
    filters,
    dimensions,
    metrics,
    partitionedBy,
    clusteredBy,
  };
}

export function ingestQueryPatternToQuery(
  ingestQueryPattern: IngestQueryPattern,
  preview?: boolean,
  sampleDataQuery?: SqlQuery,
): SqlQuery {
  const {
    destinationTableName,
    mode,
    overwriteWhere,
    mainExternalName,
    mainExternalConfig,
    filters,
    dimensions,
    metrics,
    partitionedBy,
    clusteredBy,
  } = ingestQueryPattern;
  return SqlQuery.from(T(mainExternalName))
    .applyIf(!preview, q =>
      mode === 'insert'
        ? q.changeInsertIntoTable(destinationTableName)
        : q.changeReplaceClause(SqlReplaceClause.create(destinationTableName, overwriteWhere)),
    )
    .changeWithParts([
      SqlWithPart.simple(
        mainExternalName,
        sampleDataQuery ||
          SqlQuery.create(externalConfigToTableExpression(mainExternalConfig)).applyIf(preview, q =>
            q.changeLimitValue(10000),
          ),
      ),
    ])
    .applyForEach(dimensions, (query, ex) =>
      query.addSelect(
        ex,
        metrics
          ? { addToGroupBy: 'end', groupByExpression: removeTopLevelArrayToMvOrUndefined(ex) }
          : {},
      ),
    )
    .applyForEach(metrics || [], (query, ex) => query.addSelect(ex))
    .applyIf(filters.length, query => query.changeWhereExpression(SqlExpression.and(...filters)))
    .applyIf(!preview, q =>
      q
        .changePartitionedByClause(
          SqlPartitionedByClause.create(
            partitionedBy !== 'all'
              ? new SqlLiteral({
                  value: partitionedBy.toUpperCase(),
                  stringValue: partitionedBy.toUpperCase(),
                })
              : undefined,
          ),
        )
        .changeClusteredByExpressions(clusteredBy.map(p => SqlLiteral.index(p))),
    );
}

export type DestinationMode = 'new' | 'replace' | 'insert';

export interface DestinationInfo {
  mode: DestinationMode;
  table: string;
}

export function getDestinationMode(
  ingestQueryPattern: IngestQueryPattern,
  existingTables: string[],
): DestinationMode {
  return existingTables.includes(ingestQueryPattern.destinationTableName)
    ? ingestQueryPattern.mode
    : 'new';
}

export function getDestinationInfo(
  ingestQueryPattern: IngestQueryPattern,
  existingTables: string[],
): DestinationInfo {
  return {
    mode: getDestinationMode(ingestQueryPattern, existingTables),
    table: ingestQueryPattern.destinationTableName,
  };
}
