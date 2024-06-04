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

import type { SqlAlias, SqlExpression, SqlOrderByExpression, SqlTable } from '@druid-toolkit/query';
import {
  C,
  F,
  SqlCase,
  SqlColumn,
  SqlFunction,
  SqlLiteral,
  SqlQuery,
  SqlType,
  SqlWithPart,
  T,
} from '@druid-toolkit/query';
import type { ExpressionMeta } from '@druid-toolkit/visuals-core';

import type { ColumnHint } from '../../../../utils';
import { forceSignInNumberFormatter, formatNumber, formatPercent } from '../../../../utils';
import { addTableScope, getInitQuery } from '../../utils';

import type { Compare } from './utils';
import { decodeWhereForCompares, getWhereForCompares, shiftTimeInExpression } from './utils';

export type MultipleValueMode = 'null' | 'empty' | 'latest' | 'latestNonNull' | 'count';

export type CompareStrategy = 'auto' | 'filtered' | 'join';

export type CompareType = 'value' | 'delta' | 'absDelta' | 'percent' | 'absPercent';

export type RestrictTop = 'always' | 'never';

const KNOWN_AGGREGATIONS = [
  'COUNT',
  'SUM',
  'MIN',
  'MAX',
  'AVG',
  'APPROX_COUNT_DISTINCT',
  'APPROX_COUNT_DISTINCT_DS_HLL',
  'APPROX_COUNT_DISTINCT_DS_THETA',
  'DS_HLL',
  'DS_THETA',
  'APPROX_QUANTILE',
  'APPROX_QUANTILE_DS',
  'APPROX_QUANTILE_FIXED_BUCKETS',
  'DS_QUANTILES_SKETCH',
  'BLOOM_FILTER',
  'TDIGEST_QUANTILE',
  'TDIGEST_GENERATE_SKETCH',
  'VAR_POP',
  'VAR_SAMP',
  'VARIANCE',
  'STDDEV_POP',
  'STDDEV_SAMP',
  'STDDEV',
  'EARLIEST',
  'LATEST',
  'ANY_VALUE',
];

const DRUID_DEFAULT_TOTAL_SUB_QUERY_LIMIT = 100000;

const TOP_VALUES_NAME = 'top_values';
export const DEFAULT_TOP_VALUES_K = 5000;

function coalesce0(ex: SqlExpression) {
  return F('COALESCE', ex, SqlLiteral.ZERO);
}

function doubleSafeDivide0(a: SqlExpression, b: SqlExpression) {
  return coalesce0(F('SAFE_DIVIDE', a.cast(SqlType.DOUBLE), b));
}

function anyValue(ex: SqlExpression) {
  return F('ANY_VALUE', ex);
}

function toGroupByExpression(
  splitColumn: ExpressionMeta,
  timeBucket: string,
  compareShiftDuration: string | undefined,
): SqlAlias {
  const { expression, sqlType, name } = splitColumn;
  return addTableScope(expression, 't')
    .applyIf(sqlType === 'TIMESTAMP' && compareShiftDuration, e =>
      F.timeShift(e, compareShiftDuration!, 1),
    )
    .applyIf(sqlType === 'TIMESTAMP', e => F.timeFloor(e, timeBucket))
    .as(name);
}

function toShowColumnExpression(showColumn: ExpressionMeta, mode: MultipleValueMode): SqlAlias {
  let ex: SqlExpression = F('LATEST', showColumn.expression);

  let elseEx: SqlExpression | undefined;
  switch (mode) {
    case 'null':
      elseEx = SqlLiteral.NULL;
      break;

    case 'empty':
      elseEx = SqlLiteral.create('');
      break;

    case 'latestNonNull':
      elseEx = SqlFunction.simple(
        'LATEST',
        [showColumn.expression],
        showColumn.expression.isNotNull(),
      );
      break;

    case 'count':
      elseEx = SqlFunction.simple('CONCAT', [
        'Multiple values (',
        SqlFunction.countDistinct(showColumn.expression),
        ')',
      ]);
      break;

    default:
      // latest
      break;
  }

  if (elseEx) {
    ex = SqlCase.ifThenElse(SqlFunction.countDistinct(showColumn.expression).equal(1), ex, elseEx);
  }

  return ex.as(showColumn.name);
}

function getJoinConditions(
  groupingExpressions: SqlAlias[],
  table1: SqlTable,
  table2: SqlTable,
): SqlExpression[] {
  return groupingExpressions.map(groupingExpression => {
    const outputName = groupingExpression.getOutputName()!;
    return table1.column(outputName).isNotDistinctFrom(table2.column(outputName));
  });
}

function getInnerJoinConditions(groupByExpressions: SqlAlias[]): SqlExpression[] {
  return groupByExpressions.map(groupByExpression =>
    groupByExpression
      .getUnderlyingExpression()
      .isNotDistinctFrom(T(TOP_VALUES_NAME).column(groupByExpression.getOutputName()!)),
  );
}

export function makeCompareMetricName(
  metricName: string,
  compare: Compare,
  compareType: CompareType,
) {
  return `${metricName}:compare:${compare}:${compareType}`;
}

interface DecodedOrderBy {
  orderedThing: ExpressionMeta;
  orderedSplitColumn?: ExpressionMeta;
  orderedShowColumn?: ExpressionMeta;
  orderedMetric?: ExpressionMeta;
  orderedCompareDuration?: string;
  orderedCompareType?: CompareType;
}
function decodeTableOrderBy(
  orderBy: SqlOrderByExpression | undefined,
  hasCompare: boolean,
  splitColumns: ExpressionMeta[],
  showColumns: ExpressionMeta[],
  metrics: ExpressionMeta[],
): DecodedOrderBy | undefined {
  if (!(orderBy?.expression instanceof SqlColumn)) return;

  const orderByColumnName = orderBy.expression.getName();
  let orderedThing: ExpressionMeta;
  let orderedSplitColumn: ExpressionMeta | undefined;
  let orderedShowColumn: ExpressionMeta | undefined;
  let orderedMetric: ExpressionMeta | undefined;
  let orderedCompareDuration: Compare | undefined;
  let orderedCompareType: CompareType | undefined;

  const m = orderByColumnName.match(
    /^(.+):compare:(P[^:]+):(value|delta|absDelta|percent|absPercent)$/,
  );
  if (m) {
    if (!hasCompare) return;
    const compareMetricName = m[1];
    orderedCompareDuration = m[2] as Compare;
    orderedCompareType = m[3] as CompareType;
    orderedMetric = metrics.find(metric => metric.name === compareMetricName);
    if (!orderedMetric) return;
    orderedThing = orderedMetric;
  } else {
    orderedSplitColumn = splitColumns.find(splitColumn => splitColumn.name === orderByColumnName);
    orderedShowColumn = showColumns.find(showColumn => showColumn.name === orderByColumnName);
    orderedMetric = metrics.find(metric => metric.name === orderByColumnName);
    if (
      (orderedSplitColumn ? 1 : 0) + (orderedShowColumn ? 1 : 0) + (orderedMetric ? 1 : 0) !==
      1
    ) {
      return;
    }
    orderedThing = (orderedSplitColumn || orderedShowColumn || orderedMetric)!;
  }

  return {
    orderedThing,
    orderedSplitColumn,
    orderedShowColumn,
    orderedMetric,
    orderedCompareDuration,
    orderedCompareType,
  };
}

function addPivotValuesToMetrics(
  metrics: ExpressionMeta[],
  pivotColumn: ExpressionMeta | undefined,
  pivotValues: string[] | undefined,
): ExpressionMeta[] {
  if (pivotColumn && pivotValues) {
    return pivotValues.flatMap(pivotValue =>
      metrics.map(metric => ({
        ...metric,
        name: `${pivotValue}:${metric.name}`,
        expression: metric.expression.addFilterToAggregations(
          pivotColumn.expression.equal(pivotValue),
          KNOWN_AGGREGATIONS,
        ),
      })),
    );
  } else {
    return metrics;
  }
}

function addDefaultMetricIfNeeded(
  metrics: ExpressionMeta[],
  splitColumns: ExpressionMeta[],
  showColumns: ExpressionMeta[],
): ExpressionMeta[] {
  if (splitColumns.length || showColumns.length || metrics.length) return metrics;
  return [{ expression: SqlFunction.COUNT_STAR, name: 'Count' }];
}

function makeCompareAggregatorsAndAddHints(
  metricName: string,
  compare: Compare,
  compareTypes: CompareType[],
  mainMetric: SqlExpression,
  prevMetric: SqlExpression,
  columnHints: Map<string, ColumnHint>,
): SqlExpression[] {
  const group = `${compare} comparison`;
  const diff = mainMetric.subtract(prevMetric);

  const ret: SqlExpression[] = [];

  if (compareTypes.includes('value')) {
    const valueName = makeCompareMetricName(metricName, compare, 'value');
    columnHints.set(valueName, {
      group,
      displayName: `${metricName} (value)`,
    });
    ret.push(prevMetric.as(valueName));
  }

  if (compareTypes.includes('delta')) {
    const deltaName = makeCompareMetricName(metricName, compare, 'delta');
    columnHints.set(deltaName, {
      group,
      displayName: `${metricName} (delta)`,
      formatter: forceSignInNumberFormatter(formatNumber),
    });
    ret.push(diff.as(deltaName));
  }

  if (compareTypes.includes('absDelta')) {
    const absDeltaName = makeCompareMetricName(metricName, compare, 'absDelta');
    columnHints.set(absDeltaName, {
      group,
      displayName: `${metricName} (abs. delta)`,
    });
    ret.push(F('ABS', diff).as(absDeltaName));
  }

  if (compareTypes.includes('percent')) {
    const percentName = makeCompareMetricName(metricName, compare, 'percent');
    columnHints.set(percentName, {
      group,
      displayName: `${metricName} (%)`,
      formatter: forceSignInNumberFormatter(formatPercent),
    });
    ret.push(doubleSafeDivide0(diff, prevMetric).as(percentName));
  }

  if (compareTypes.includes('absPercent')) {
    const absPercentName = makeCompareMetricName(metricName, compare, 'absPercent');
    columnHints.set(absPercentName, {
      group,
      displayName: `${metricName} (abs. %)`,
      formatter: formatPercent,
    });
    ret.push(F('ABS', doubleSafeDivide0(diff, prevMetric)).as(absPercentName));
  }

  return ret;
}

export interface QueryAndHints {
  query: SqlQuery;
  columnHints: Map<string, ColumnHint>;
}

export interface MakeTableQueryAndHintsOptions {
  table: SqlExpression;
  where: SqlExpression;
  splitColumns: ExpressionMeta[];
  timeBucket?: string;
  showColumns: ExpressionMeta[];
  multipleValueMode?: MultipleValueMode;
  pivotColumn?: ExpressionMeta;
  metrics: ExpressionMeta[];
  compares?: Compare[];
  compareStrategy?: CompareStrategy;
  compareTypes?: CompareType[];
  restrictTop?: RestrictTop;
  maxRows: number;

  pivotValues?: string[];
  orderBy: SqlOrderByExpression | undefined;

  // Druid config / version related
  totalSubQueryLimit?: number;
  useGroupingToOrderSubQueries?: boolean;
  topValuesK?: number;
}
export function makeTableQueryAndHints(options: MakeTableQueryAndHintsOptions): QueryAndHints {
  const table = options.table;
  const where = options.where;
  const splitColumns = options.splitColumns;
  const timeBucket = options.timeBucket || 'PT1H';
  const showColumns = options.showColumns;
  const multipleValueMode = options.multipleValueMode || 'null';
  const metrics = addDefaultMetricIfNeeded(
    addPivotValuesToMetrics(options.metrics, options.pivotColumn, options.pivotValues),
    splitColumns,
    showColumns,
  );
  const compares = options.compares || [];
  const compareStrategy = options.compareStrategy || 'auto';
  const compareTypes = options.compareTypes || ['value', 'delta'];
  const restrictTop = options.restrictTop || 'always';
  const maxRows = options.maxRows;
  const orderBy = options.orderBy;

  const totalSubQueryLimit = options.totalSubQueryLimit || DRUID_DEFAULT_TOTAL_SUB_QUERY_LIMIT;
  const useGroupingToOrderSubQueries = Boolean(options.useGroupingToOrderSubQueries);
  const topValuesK = options.topValuesK || DEFAULT_TOP_VALUES_K;

  if (splitColumns.length + showColumns.length + metrics.length === 0) {
    throw new Error('nothing to show');
  }

  const hasCompare = Boolean(compares.length) && Boolean(compareTypes.length);

  if (hasCompare) {
    const effectiveCompareStrategy =
      compareStrategy === 'auto'
        ? splitColumns.some(splitColumn => splitColumn.sqlType === 'TIMESTAMP')
          ? 'join'
          : 'filtered'
        : compareStrategy;

    if (effectiveCompareStrategy === 'filtered') {
      return makeFilteredCompareTableQueryAndHints({
        table,
        where,
        splitColumns,
        timeBucket,
        showColumns,
        multipleValueMode,
        metrics,
        compares,
        compareTypes,
        maxRows,
        orderBy,
      });
    } else {
      return makeJoinCompareTableQueryAndHints({
        table,
        where,
        splitColumns,
        timeBucket,
        showColumns,
        multipleValueMode,
        metrics,
        compares,
        compareTypes,
        restrictTop,
        maxRows,
        orderBy,
        totalSubQueryLimit,
        useGroupingToOrderSubQueries,
        topValuesK,
      });
    }
  } else {
    return makeNonCompareTableQueryAndHints({
      table,
      where,
      splitColumns,
      timeBucket,
      showColumns,
      multipleValueMode,
      metrics,
      maxRows,
      orderBy,
    });
  }
}

interface MakeNonCompareTableQueryAndHintsOptions {
  table: SqlExpression;
  where: SqlExpression;
  splitColumns: ExpressionMeta[];
  timeBucket: string;
  showColumns: ExpressionMeta[];
  multipleValueMode: MultipleValueMode;
  metrics: ExpressionMeta[];
  maxRows: number;

  orderBy: SqlOrderByExpression | undefined;
}
function makeNonCompareTableQueryAndHints(
  options: MakeNonCompareTableQueryAndHintsOptions,
): QueryAndHints {
  const {
    table,
    where,
    splitColumns,
    timeBucket,
    showColumns,
    multipleValueMode,
    metrics,
    maxRows,
    orderBy,
  } = options;

  let decodedOrderBy = decodeTableOrderBy(orderBy, false, splitColumns, showColumns, metrics);
  let effectiveOrderBy: SqlOrderByExpression;
  if (decodedOrderBy) {
    effectiveOrderBy = orderBy!;
  } else {
    effectiveOrderBy = C(
      metrics[0]?.name || splitColumns[0]?.name || showColumns[0]?.name,
    ).toOrderByExpression('DESC');
    decodedOrderBy = decodeTableOrderBy(
      effectiveOrderBy,
      false,
      splitColumns,
      showColumns,
      metrics,
    );
    if (!decodedOrderBy) {
      throw new Error('should never get here: must be able to decode own default order by');
    }
  }

  const mainGroupByExpressions = splitColumns.map(splitColumn =>
    toGroupByExpression(splitColumn, timeBucket, undefined),
  );

  const showColumnExpressions: SqlAlias[] = showColumns.map(showColumn =>
    toShowColumnExpression(showColumn, multipleValueMode),
  );

  return {
    columnHints: new Map<string, ColumnHint>(),
    query: getInitQuery(table, where)
      .applyForEach(mainGroupByExpressions, (q, groupByExpression) =>
        q.addSelect(groupByExpression, {
          addToGroupBy: 'end',
        }),
      )
      .applyForEach(showColumnExpressions, (q, showColumnExpression) =>
        q.addSelect(showColumnExpression),
      )
      .applyForEach(metrics, (q, metric) => q.addSelect(metric.expression.as(metric.name)))
      .changeOrderByExpression(effectiveOrderBy)
      .changeLimitValue(maxRows),
  };
}

// ------------------------------

interface MakeJoinCompareTableQueryAndHintsOptions {
  table: SqlExpression;
  where: SqlExpression;
  splitColumns: ExpressionMeta[];
  timeBucket: string;
  showColumns: ExpressionMeta[];
  multipleValueMode: MultipleValueMode;
  metrics: ExpressionMeta[];
  compares: Compare[];
  compareTypes: CompareType[];
  restrictTop: RestrictTop;
  maxRows: number;
  orderBy: SqlOrderByExpression | undefined;
  totalSubQueryLimit: number;
  useGroupingToOrderSubQueries: boolean;
  topValuesK: number;
}

function makeJoinCompareTableQueryAndHints(
  options: MakeJoinCompareTableQueryAndHintsOptions,
): QueryAndHints {
  const {
    table,
    where,
    splitColumns,
    timeBucket,
    showColumns,
    multipleValueMode,
    metrics,
    compares,
    compareTypes,
    restrictTop,
    maxRows,
    orderBy,
    totalSubQueryLimit,
    useGroupingToOrderSubQueries,
    topValuesK,
  } = options;

  let decodedOrderBy = decodeTableOrderBy(orderBy, true, splitColumns, showColumns, metrics);
  let effectiveOrderBy: SqlOrderByExpression;
  if (decodedOrderBy) {
    effectiveOrderBy = orderBy!;
  } else {
    effectiveOrderBy = C(
      metrics[0]?.name || splitColumns[0]?.name || showColumns[0]?.name,
    ).toOrderByExpression('DESC');
    decodedOrderBy = decodeTableOrderBy(effectiveOrderBy, true, splitColumns, showColumns, metrics);
    if (!decodedOrderBy) {
      throw new Error('should never get here: must be able to decode own default order by');
    }
  }

  const orderByCompareDuration = decodedOrderBy.orderedCompareDuration;
  const orderByCompareType = decodedOrderBy.orderedCompareType;

  const mainGroupByExpressions = splitColumns.map(splitColumn =>
    toGroupByExpression(splitColumn, timeBucket, undefined),
  );

  const showColumnExpressions: SqlAlias[] = showColumns.map(showColumn =>
    toShowColumnExpression(showColumn, multipleValueMode),
  );

  const topValuesQuery =
    splitColumns.length && restrictTop !== 'never'
      ? getInitQuery(table, getWhereForCompares(where, compares))
          .applyForEach(mainGroupByExpressions, (q, groupByExpression) =>
            q.addSelect(groupByExpression, {
              addToGroupBy: 'end',
            }),
          )
          .changeOrderByExpression(
            (decodedOrderBy.orderedSplitColumn
              ? toGroupByExpression(decodedOrderBy.orderedSplitColumn, timeBucket, undefined)
              : decodedOrderBy.orderedShowColumn
              ? toShowColumnExpression(decodedOrderBy.orderedShowColumn, multipleValueMode)
              : decodedOrderBy.orderedThing.expression
            )
              .getUnderlyingExpression()
              .toOrderByExpression('DESC'),
          )
          .changeLimitValue(topValuesK)
      : undefined;

  const safeSubQueryLimit = Math.floor(totalSubQueryLimit / (compares.length + 1));

  const columnHints = new Map<string, ColumnHint>();
  const mainQuery = getInitQuery(table, where)
    .applyForEach(mainGroupByExpressions, (q, groupByExpression) =>
      q.addSelect(groupByExpression, {
        addToGroupBy: 'end',
      }),
    )
    .applyForEach(showColumnExpressions, (q, showColumnExpression) =>
      q.addSelect(showColumnExpression),
    )
    .applyForEach(metrics, (q, metric) => q.addSelect(metric.expression.as(metric.name)))
    .changeOrderByExpression(effectiveOrderBy)
    .changeLimitValue(maxRows)
    .applyIf(
      orderByCompareDuration || !decodedOrderBy.orderedMetric,
      // In case where we are ordering on something other than a main metric value so either something from a compare or simply not on a measure
      q =>
        topValuesQuery
          ? q
              .addInnerJoin(T(TOP_VALUES_NAME), getInnerJoinConditions(mainGroupByExpressions))
              .changeOrderByExpression(undefined)
              .changeLimitValue(undefined)
          : q
              .applyIf(orderByCompareDuration, q =>
                q.changeOrderByExpression(
                  decodedOrderBy!.orderedMetric!.expression.toOrderByExpression('DESC'),
                ),
              )
              .changeLimitValue(safeSubQueryLimit),
    );

  const main = T('main');
  const query = SqlQuery.from('main')
    .changeWithParts([
      ...(topValuesQuery ? [SqlWithPart.simple(TOP_VALUES_NAME, topValuesQuery)] : []),
      SqlWithPart.simple('main', mainQuery),
      ...compares.map(compare =>
        SqlWithPart.simple(
          `compare_${compare}`,
          getInitQuery(table, shiftTimeInExpression(where, compare))
            .applyForEach(splitColumns, (q, splitColumn) =>
              q.addSelect(toGroupByExpression(splitColumn, timeBucket, compare), {
                addToGroupBy: 'end',
              }),
            )
            .applyForEach(showColumnExpressions, (q, showColumnExpression) =>
              q.addSelect(showColumnExpression),
            )
            .applyForEach(metrics, (q, metric) => q.addSelect(metric.expression.as(metric.name)))
            .applyIf(
              compare === orderByCompareDuration && orderByCompareType === 'value',
              q =>
                q
                  .changeOrderByExpression(
                    effectiveOrderBy.changeExpression(C(decodedOrderBy!.orderedMetric!.name)),
                  )
                  .changeLimitValue(maxRows),
              q =>
                topValuesQuery
                  ? q.addInnerJoin(
                      T(TOP_VALUES_NAME),
                      getInnerJoinConditions(
                        splitColumns.map(splitColumn =>
                          toGroupByExpression(splitColumn, timeBucket, compare),
                        ),
                      ),
                    )
                  : q
                      .changeOrderByExpression(
                        C(decodedOrderBy!.orderedThing.name).toOrderByExpression('DESC'),
                      )
                      .changeLimitValue(safeSubQueryLimit),
            ),
        ),
      ),
    ])
    .changeSelectExpressions([
      ...splitColumns.map(splitColumn =>
        F(
          'COALESCE',
          main.column(splitColumn.name),
          ...compares.map(compare => T(`compare_${compare}`).column(splitColumn.name)),
        ).as(splitColumn.name),
      ),
      ...showColumns.map(showColumn =>
        F(
          'COALESCE',
          main.column(showColumn.name).applyIf(useGroupingToOrderSubQueries, anyValue),
          ...compares.map(compare =>
            T(`compare_${compare}`)
              .column(showColumn.name)
              .applyIf(useGroupingToOrderSubQueries, anyValue),
          ),
        ).as(showColumn.name),
      ),
      ...metrics.map(metric =>
        main
          .column(metric.name)
          .applyIf(useGroupingToOrderSubQueries, anyValue)
          .apply(coalesce0)
          .as(metric.name),
      ),
      ...compares.flatMap(compare => {
        return metrics.flatMap(metric => {
          const metricName = metric.name;

          const mainMetric = main
            .column(metricName)
            .applyIf(useGroupingToOrderSubQueries, anyValue)
            .apply(coalesce0);

          const prevMetric = T(`compare_${compare}`)
            .column(metricName)
            .applyIf(useGroupingToOrderSubQueries, anyValue)
            .apply(coalesce0);

          return makeCompareAggregatorsAndAddHints(
            metricName,
            compare,
            compareTypes,
            mainMetric,
            prevMetric,
            columnHints,
          );
        });
      }),
    ])
    .applyForEach(compares, (q, compare) =>
      q.addFullJoin(
        T(`compare_${compare}`),
        getJoinConditions(
          splitColumns.map(splitColumn => toGroupByExpression(splitColumn, timeBucket, compare)),
          main,
          T(`compare_${compare}`),
        ),
      ),
    )
    .applyIf(useGroupingToOrderSubQueries, q =>
      q.changeGroupByExpressions(splitColumns.map((_, i) => SqlLiteral.index(i))),
    )
    .addOrderBy(effectiveOrderBy)
    .changeLimitValue(maxRows);

  for (const showColumn of showColumns) {
    columnHints.set(showColumn.name, { group: 'Current' });
  }
  for (const metric of metrics) {
    columnHints.set(metric.name, { group: 'Current' });
  }

  return {
    query,
    columnHints,
  };
}

// ------------------------------

interface MakeFilteredCompareTableQueryAndHintsOptions {
  table: SqlExpression;
  where: SqlExpression;
  splitColumns: ExpressionMeta[];
  timeBucket: string;
  showColumns: ExpressionMeta[];
  multipleValueMode: MultipleValueMode;
  metrics: ExpressionMeta[];
  compares: Compare[];
  compareTypes: CompareType[];
  maxRows: number;
  orderBy: SqlOrderByExpression | undefined;
}

function makeFilteredCompareTableQueryAndHints(
  options: MakeFilteredCompareTableQueryAndHintsOptions,
): QueryAndHints {
  const {
    table,
    where,
    splitColumns,
    timeBucket,
    showColumns,
    multipleValueMode,
    metrics,
    compares,
    compareTypes,
    maxRows,
    orderBy,
  } = options;

  let decodedOrderBy = decodeTableOrderBy(orderBy, true, splitColumns, showColumns, metrics);
  let effectiveOrderBy: SqlOrderByExpression;
  if (decodedOrderBy) {
    effectiveOrderBy = orderBy!;
  } else {
    effectiveOrderBy = C(
      metrics[0]?.name || splitColumns[0]?.name || showColumns[0]?.name,
    ).toOrderByExpression('DESC');
    decodedOrderBy = decodeTableOrderBy(effectiveOrderBy, true, splitColumns, showColumns, metrics);
    if (!decodedOrderBy) {
      throw new Error('should never get here: must be able to decode own default order by');
    }
  }

  const mainGroupByExpressions = splitColumns.map(splitColumn =>
    toGroupByExpression(splitColumn, timeBucket, undefined),
  );

  const showColumnExpressions: SqlAlias[] = showColumns.map(showColumn =>
    toShowColumnExpression(showColumn, multipleValueMode),
  );

  const { mainWherePart, perCompareWhereParts } = decodeWhereForCompares(where, compares);

  const columnHints = new Map<string, ColumnHint>();
  const query = getInitQuery(table, getWhereForCompares(where, compares))
    .applyForEach(mainGroupByExpressions, (q, groupByExpression) =>
      q.addSelect(groupByExpression, {
        addToGroupBy: 'end',
      }),
    )
    .applyForEach(showColumnExpressions, (q, showColumnExpression) =>
      q.addSelect(showColumnExpression),
    )
    .applyForEach(metrics, (q, metric) =>
      q.addSelect(
        metric.expression
          .addFilterToAggregations(mainWherePart, KNOWN_AGGREGATIONS)
          .apply(coalesce0)
          .as(metric.name),
      ),
    )
    .applyForEach(
      compares.flatMap((compare, compareIndex) => {
        return metrics.flatMap(metric => {
          const metricName = metric.name;

          const mainMetric = metric.expression
            .addFilterToAggregations(mainWherePart, KNOWN_AGGREGATIONS)
            .apply(coalesce0);

          const prevMetric = metric.expression
            .addFilterToAggregations(perCompareWhereParts[compareIndex], KNOWN_AGGREGATIONS)
            .apply(coalesce0);

          return makeCompareAggregatorsAndAddHints(
            metricName,
            compare,
            compareTypes,
            mainMetric,
            prevMetric,
            columnHints,
          );
        });
      }),
      (q, ex) => q.addSelect(ex),
    )
    .changeOrderByExpression(effectiveOrderBy)
    .changeLimitValue(maxRows);

  for (const showColumn of showColumns) {
    columnHints.set(showColumn.name, { group: 'Current' });
  }
  for (const metric of metrics) {
    columnHints.set(metric.name, { group: 'Current' });
  }

  return {
    query,
    columnHints,
  };
}
