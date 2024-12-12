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

import type { SqlAlias, SqlExpression, SqlOrderByExpression, SqlTable } from 'druid-query-toolkit';
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
} from 'druid-query-toolkit';

import type { ColumnHint } from '../../../utils';
import { Duration, forceSignInNumberFormatter, formatNumber, formatPercent } from '../../../utils';
import type { ExpressionMeta } from '../models';
import { Measure } from '../models';

import { addTableScope } from './general';
import { KNOWN_AGGREGATIONS } from './known-aggregations';
import type { Compare } from './time-manipulation';
import { computeWhereForCompares } from './time-manipulation';

export type MultipleValueMode = 'null' | 'empty' | 'latest' | 'latestNonNull' | 'count';

export type CompareStrategy = 'auto' | 'filtered' | 'join';

export type CompareType = 'value' | 'delta' | 'absDelta' | 'percent' | 'absPercent';

export type RestrictTop = 'always' | 'never';

const DRUID_DEFAULT_TOTAL_SUB_QUERY_LIMIT = 100000;

const COMMON_NAME = 'common';
const TOP_VALUES_NAME = 'top_values';
export const DEFAULT_TOP_VALUES_K = 5000;

function isTimestamp(em: ExpressionMeta): boolean {
  const { expression } = em;
  return expression instanceof SqlColumn && expression.getName().toLowerCase() === '__time';
}

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
  forceTableScope: string | undefined,
  compareShiftDuration: string | undefined,
): SqlAlias {
  const { expression, name } = splitColumn;
  const ts = isTimestamp(splitColumn);
  return expression
    .applyIf(forceTableScope, ex => addTableScope(ex, forceTableScope!))
    .applyIf(ts && compareShiftDuration, e => F.timeShift(e, compareShiftDuration!, 1))
    .applyIf(ts, e => F.timeFloor(e, timeBucket))
    .as(name);
}

function makeBaseColumnHints(
  splitColumns: ExpressionMeta[],
  timeBucket: string,
  showColumns: ExpressionMeta[],
) {
  const columnHints = new Map<string, ColumnHint>();
  for (const splitColumn of splitColumns) {
    const hint: ColumnHint = {
      expressionForWhere: toGroupByExpression(
        splitColumn,
        timeBucket,
        undefined,
        undefined,
      ).getUnderlyingExpression(),
    };
    if (isTimestamp(splitColumn)) {
      hint.displayName = `${splitColumn.name} (by ${new Duration(timeBucket).getDescription()})`;
    }
    columnHints.set(splitColumn.name, hint);
  }
  for (const showColumn of showColumns) {
    columnHints.set(showColumn.name, {
      expressionForWhere: showColumn.expression,
    });
  }
  return columnHints;
}

function toShowColumnExpression(
  showColumn: ExpressionMeta,
  mode: MultipleValueMode,
  forceTableScope: string | undefined,
): SqlAlias {
  let ex: SqlExpression = F('LATEST_BY', showColumn.expression, C('__time'));

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
        'LATEST_BY',
        [showColumn.expression, C('__time')],
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
      // latest: nothing to do
      break;
  }

  if (elseEx) {
    ex = SqlCase.ifThenElse(SqlFunction.countDistinct(showColumn.expression).equal(1), ex, elseEx);
  }

  if (forceTableScope) {
    ex = addTableScope(ex, forceTableScope);
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

export function makeCompareMeasureName(
  measureName: string,
  compare: Compare,
  compareType: CompareType,
) {
  return `${measureName}:compare:${compare}:${compareType}`;
}

interface DecodedOrderBy {
  orderedThing: ExpressionMeta | Measure;
  orderedSplitColumn?: ExpressionMeta;
  orderedShowColumn?: ExpressionMeta;
  orderedMeasure?: Measure;
  orderedCompareDuration?: string;
  orderedCompareType?: CompareType;
}

function decodeTableOrderBy(
  orderBy: SqlOrderByExpression | undefined,
  hasCompare: boolean,
  splitColumns: ExpressionMeta[],
  showColumns: ExpressionMeta[],
  measures: Measure[],
): DecodedOrderBy | undefined {
  if (!(orderBy?.expression instanceof SqlColumn)) return;

  const orderByColumnName = orderBy.expression.getName();
  let orderedThing: ExpressionMeta | Measure;
  let orderedSplitColumn: ExpressionMeta | undefined;
  let orderedShowColumn: ExpressionMeta | undefined;
  let orderedMeasure: Measure | undefined;
  let orderedCompareDuration: Compare | undefined;
  let orderedCompareType: CompareType | undefined;

  const m = /^(.+):compare:(P[^:]+):(value|delta|absDelta|percent|absPercent)$/.exec(
    orderByColumnName,
  );
  if (m) {
    if (!hasCompare) return;
    const compareMeasureName = m[1];
    orderedCompareDuration = m[2] as Compare;
    orderedCompareType = m[3] as CompareType;
    orderedMeasure = measures.find(measure => measure.name === compareMeasureName);
    if (!orderedMeasure) return;
    orderedThing = orderedMeasure;
  } else {
    orderedSplitColumn = splitColumns.find(splitColumn => splitColumn.name === orderByColumnName);
    orderedShowColumn = showColumns.find(showColumn => showColumn.name === orderByColumnName);
    orderedMeasure = measures.find(measure => measure.name === orderByColumnName);
    if (
      (orderedSplitColumn ? 1 : 0) + (orderedShowColumn ? 1 : 0) + (orderedMeasure ? 1 : 0) !==
      1
    ) {
      return;
    }
    orderedThing = (orderedSplitColumn || orderedShowColumn || orderedMeasure)!;
  }

  return {
    orderedThing,
    orderedSplitColumn,
    orderedShowColumn,
    orderedMeasure,
    orderedCompareDuration,
    orderedCompareType,
  };
}

function addPivotValuesToMeasures(
  measures: Measure[],
  pivotColumn: ExpressionMeta | undefined,
  pivotValues: string[] | undefined,
): Measure[] {
  if (pivotColumn && pivotValues) {
    return pivotValues.flatMap(pivotValue =>
      measures.map(
        measure =>
          new Measure({
            as: `${pivotValue}:${measure.name}`,
            expression: measure.expression.addFilterToAggregations(
              pivotColumn.expression.equal(pivotValue),
              KNOWN_AGGREGATIONS,
            ),
          }),
      ),
    );
  } else {
    return measures;
  }
}

function addDefaultMeasureIfNeeded(
  measures: Measure[],
  splitColumns: ExpressionMeta[],
  showColumns: ExpressionMeta[],
): Measure[] {
  if (splitColumns.length || showColumns.length || measures.length) return measures;
  return [Measure.COUNT];
}

function makeCompareAggregatorsAndAddHints(
  measureName: string,
  compare: Compare,
  compareTypes: CompareType[],
  mainMeasure: SqlExpression,
  prevMeasure: SqlExpression,
  columnHints: Map<string, ColumnHint>,
): SqlExpression[] {
  const group = `Previous ${new Duration(compare).getDescription()}`;
  const diff = mainMeasure.subtract(prevMeasure);

  const ret: SqlExpression[] = [];

  if (compareTypes.includes('value')) {
    const valueName = makeCompareMeasureName(measureName, compare, 'value');
    columnHints.set(valueName, {
      group,
      displayName: `${measureName} (value)`,
    });
    ret.push(prevMeasure.as(valueName));
  }

  if (compareTypes.includes('delta')) {
    const deltaName = makeCompareMeasureName(measureName, compare, 'delta');
    columnHints.set(deltaName, {
      group,
      displayName: `${measureName} (delta)`,
      formatter: forceSignInNumberFormatter(formatNumber),
    });
    ret.push(diff.as(deltaName));
  }

  if (compareTypes.includes('absDelta')) {
    const absDeltaName = makeCompareMeasureName(measureName, compare, 'absDelta');
    columnHints.set(absDeltaName, {
      group,
      displayName: `${measureName} (abs. delta)`,
    });
    ret.push(F('ABS', diff).as(absDeltaName));
  }

  if (compareTypes.includes('percent')) {
    const percentName = makeCompareMeasureName(measureName, compare, 'percent');
    columnHints.set(percentName, {
      group,
      displayName: `${measureName} (%)`,
      formatter: forceSignInNumberFormatter(formatPercent),
    });
    ret.push(doubleSafeDivide0(diff, prevMeasure).as(percentName));
  }

  if (compareTypes.includes('absPercent')) {
    const absPercentName = makeCompareMeasureName(measureName, compare, 'absPercent');
    columnHints.set(absPercentName, {
      group,
      displayName: `${measureName} (abs. %)`,
      formatter: formatPercent,
    });
    ret.push(F('ABS', doubleSafeDivide0(diff, prevMeasure)).as(absPercentName));
  }

  return ret;
}

export interface QueryAndHints {
  query: SqlQuery;
  columnHints: Map<string, ColumnHint>;
}

export interface MakeTableQueryAndHintsOptions {
  source: SqlQuery;
  where: SqlExpression;
  splitColumns: ExpressionMeta[];
  timeBucket?: string;
  showColumns: ExpressionMeta[];
  multipleValueMode?: MultipleValueMode;
  pivotColumn?: ExpressionMeta;
  measures: Measure[];
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
  const source = options.source;
  const where = options.where;
  const splitColumns = options.splitColumns;
  const timeBucket = options.timeBucket || 'PT1H';
  const showColumns = options.showColumns;
  const multipleValueMode = options.multipleValueMode || 'null';
  const measures = addDefaultMeasureIfNeeded(
    addPivotValuesToMeasures(options.measures, options.pivotColumn, options.pivotValues),
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

  if (splitColumns.length + showColumns.length + measures.length === 0) {
    throw new Error('nothing to show');
  }

  const hasCompare = Boolean(compares.length) && Boolean(compareTypes.length);

  if (hasCompare) {
    const effectiveCompareStrategy =
      compareStrategy === 'auto'
        ? splitColumns.some(isTimestamp)
          ? 'join'
          : 'filtered'
        : compareStrategy;

    if (effectiveCompareStrategy === 'filtered') {
      return makeFilteredCompareTableQueryAndHints({
        source,
        where,
        splitColumns,
        timeBucket,
        showColumns,
        multipleValueMode,
        measures,
        compares,
        compareTypes,
        maxRows,
        orderBy,
      });
    } else {
      return makeJoinCompareTableQueryAndHints({
        source,
        where,
        splitColumns,
        timeBucket,
        showColumns,
        multipleValueMode,
        measures,
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
      source,
      where,
      splitColumns,
      timeBucket,
      showColumns,
      multipleValueMode,
      measures,
      maxRows,
      orderBy,
    });
  }
}

interface MakeNonCompareTableQueryAndHintsOptions {
  source: SqlQuery;
  where: SqlExpression;
  splitColumns: ExpressionMeta[];
  timeBucket: string;
  showColumns: ExpressionMeta[];
  multipleValueMode: MultipleValueMode;
  measures: Measure[];
  maxRows: number;

  orderBy: SqlOrderByExpression | undefined;
}

function makeNonCompareTableQueryAndHints(
  options: MakeNonCompareTableQueryAndHintsOptions,
): QueryAndHints {
  const {
    source,
    where,
    splitColumns,
    timeBucket,
    showColumns,
    multipleValueMode,
    measures,
    maxRows,
    orderBy,
  } = options;

  let decodedOrderBy = decodeTableOrderBy(orderBy, false, splitColumns, showColumns, measures);
  let effectiveOrderBy: SqlOrderByExpression;
  if (decodedOrderBy) {
    effectiveOrderBy = orderBy!;
  } else {
    effectiveOrderBy = C(
      measures[0]?.name || splitColumns[0]?.name || showColumns[0]?.name,
    ).toOrderByExpression('DESC');
    decodedOrderBy = decodeTableOrderBy(
      effectiveOrderBy,
      false,
      splitColumns,
      showColumns,
      measures,
    );
    if (!decodedOrderBy) {
      throw new Error('should never get here: must be able to decode own default order by');
    }
  }

  const mainGroupByExpressions = splitColumns.map(splitColumn =>
    toGroupByExpression(splitColumn, timeBucket, undefined, undefined),
  );

  const showColumnExpressions: SqlAlias[] = showColumns.map(showColumn =>
    toShowColumnExpression(showColumn, multipleValueMode, undefined),
  );

  return {
    columnHints: makeBaseColumnHints(splitColumns, timeBucket, showColumns),
    query: SqlQuery.from(source.as('t'))
      .addWhere(where)
      .applyForEach(mainGroupByExpressions, (q, groupByExpression) =>
        q.addSelect(groupByExpression, {
          addToGroupBy: 'end',
        }),
      )
      .applyForEach(showColumnExpressions, (q, showColumnExpression) =>
        q.addSelect(showColumnExpression),
      )
      .applyForEach(measures, (q, measure) => q.addSelect(measure.expression.as(measure.name)))
      .changeOrderByExpression(effectiveOrderBy)
      .changeLimitValue(maxRows),
  };
}

// ------------------------------

interface MakeJoinCompareTableQueryAndHintsOptions {
  source: SqlQuery;
  where: SqlExpression;
  splitColumns: ExpressionMeta[];
  timeBucket: string;
  showColumns: ExpressionMeta[];
  multipleValueMode: MultipleValueMode;
  measures: Measure[];
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
    source,
    where,
    splitColumns,
    timeBucket,
    showColumns,
    multipleValueMode,
    measures,
    compares,
    compareTypes,
    restrictTop,
    maxRows,
    orderBy,
    totalSubQueryLimit,
    useGroupingToOrderSubQueries,
    topValuesK,
  } = options;

  let decodedOrderBy = decodeTableOrderBy(orderBy, true, splitColumns, showColumns, measures);
  let effectiveOrderBy: SqlOrderByExpression;
  if (decodedOrderBy) {
    effectiveOrderBy = orderBy!;
  } else {
    effectiveOrderBy = C(
      measures[0]?.name || splitColumns[0]?.name || showColumns[0]?.name,
    ).toOrderByExpression('DESC');
    decodedOrderBy = decodeTableOrderBy(
      effectiveOrderBy,
      true,
      splitColumns,
      showColumns,
      measures,
    );
    if (!decodedOrderBy) {
      throw new Error('should never get here: must be able to decode own default order by');
    }
  }

  const orderByCompareDuration = decodedOrderBy.orderedCompareDuration;
  const orderByCompareType = decodedOrderBy.orderedCompareType;

  const mainGroupByExpressions = splitColumns.map(splitColumn =>
    toGroupByExpression(splitColumn, timeBucket, 't', undefined),
  );

  const showColumnExpressions: SqlAlias[] = showColumns.map(showColumn =>
    toShowColumnExpression(showColumn, multipleValueMode, 't'),
  );

  const { commonWhere, mainWherePart, perCompareWhereParts } = computeWhereForCompares(
    addTableScope(where, 't'),
    compares,
    splitColumns.some(isTimestamp) ? timeBucket : undefined,
  );

  const commonQuery = SqlQuery.selectStarFrom(source.as('t')).addWhere(commonWhere);

  const topValuesQuery =
    splitColumns.length && restrictTop !== 'never'
      ? SqlQuery.from(T(COMMON_NAME).as('t'))
          .applyForEach(mainGroupByExpressions, (q, groupByExpression) =>
            q.addSelect(groupByExpression, {
              addToGroupBy: 'end',
            }),
          )
          .changeOrderByExpression(
            (decodedOrderBy.orderedSplitColumn
              ? toGroupByExpression(
                  decodedOrderBy.orderedSplitColumn,
                  timeBucket,
                  undefined,
                  undefined,
                )
              : decodedOrderBy.orderedShowColumn
              ? toShowColumnExpression(decodedOrderBy.orderedShowColumn, multipleValueMode, 't')
              : decodedOrderBy.orderedThing.expression
            )
              .getUnderlyingExpression()
              .toOrderByExpression('DESC'),
          )
          .changeLimitValue(topValuesK)
      : undefined;

  const safeSubQueryLimit = Math.floor(totalSubQueryLimit / (compares.length + 1));

  const columnHints = makeBaseColumnHints(splitColumns, timeBucket, showColumns);
  const mainQuery = SqlQuery.from(T(COMMON_NAME).as('t'))
    .changeWhereExpression(mainWherePart)
    .applyForEach(mainGroupByExpressions, (q, groupByExpression) =>
      q.addSelect(groupByExpression, {
        addToGroupBy: 'end',
      }),
    )
    .applyForEach(showColumnExpressions, (q, showColumnExpression) =>
      q.addSelect(showColumnExpression),
    )
    .applyForEach(measures, (q, measure) => q.addSelect(measure.expression.as(measure.name)))
    .changeOrderByExpression(effectiveOrderBy)
    .changeLimitValue(maxRows)
    .applyIf(
      orderByCompareDuration || !decodedOrderBy.orderedMeasure,
      // In case where we are ordering on something other than a main measure value so either something from a compare or simply not on a measure
      q =>
        topValuesQuery
          ? q
              .addInnerJoin(T(TOP_VALUES_NAME), getInnerJoinConditions(mainGroupByExpressions))
              .changeOrderByExpression(undefined)
              .changeLimitValue(undefined)
          : q
              .applyIf(orderByCompareDuration, q =>
                q.changeOrderByExpression(
                  decodedOrderBy.orderedMeasure!.expression.toOrderByExpression('DESC'),
                ),
              )
              .changeLimitValue(safeSubQueryLimit),
    );

  const main = T('main');
  const query = SqlQuery.from('main')
    .changeWithParts([
      SqlWithPart.simple(COMMON_NAME, commonQuery),
      ...(topValuesQuery ? [SqlWithPart.simple(TOP_VALUES_NAME, topValuesQuery)] : []),
      SqlWithPart.simple('main', mainQuery),
      ...compares.map((compare, i) =>
        SqlWithPart.simple(
          `compare_${compare}`,
          SqlQuery.from(T(COMMON_NAME).as('t'))
            .changeWhereExpression(perCompareWhereParts[i])
            .applyForEach(splitColumns, (q, splitColumn) =>
              q.addSelect(toGroupByExpression(splitColumn, timeBucket, 't', compare), {
                addToGroupBy: 'end',
              }),
            )
            .applyForEach(showColumnExpressions, (q, showColumnExpression) =>
              q.addSelect(showColumnExpression),
            )
            .applyForEach(measures, (q, measure) =>
              q.addSelect(measure.expression.as(measure.name)),
            )
            .applyIf(
              compare === orderByCompareDuration && orderByCompareType === 'value',
              q =>
                q
                  .changeOrderByExpression(
                    effectiveOrderBy.changeExpression(C(decodedOrderBy.orderedMeasure!.name)),
                  )
                  .changeLimitValue(maxRows),
              q =>
                topValuesQuery
                  ? q.addInnerJoin(
                      T(TOP_VALUES_NAME),
                      getInnerJoinConditions(
                        splitColumns.map(splitColumn =>
                          toGroupByExpression(splitColumn, timeBucket, 't', compare),
                        ),
                      ),
                    )
                  : q
                      .changeOrderByExpression(
                        C(decodedOrderBy.orderedThing.name).toOrderByExpression('DESC'),
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
      ...measures.map(measure =>
        main
          .column(measure.name)
          .applyIf(useGroupingToOrderSubQueries, anyValue)
          .apply(coalesce0)
          .as(measure.name),
      ),
      ...compares.flatMap(compare => {
        return measures.flatMap(measure => {
          const measureName = measure.name;

          const mainMeasure = main
            .column(measureName)
            .applyIf(useGroupingToOrderSubQueries, anyValue)
            .apply(coalesce0);

          const prevMeasure = T(`compare_${compare}`)
            .column(measureName)
            .applyIf(useGroupingToOrderSubQueries, anyValue)
            .apply(coalesce0);

          return makeCompareAggregatorsAndAddHints(
            measureName,
            compare,
            compareTypes,
            mainMeasure,
            prevMeasure,
            columnHints,
          );
        });
      }),
    ])
    .applyForEach(compares, (q, compare) =>
      q.addFullJoin(
        T(`compare_${compare}`),
        getJoinConditions(
          splitColumns.map(splitColumn =>
            toGroupByExpression(splitColumn, timeBucket, 't', compare),
          ),
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

  for (const measure of measures) {
    columnHints.set(measure.name, { group: 'Current' });
  }

  return {
    query,
    columnHints,
  };
}

// ------------------------------

interface MakeFilteredCompareTableQueryAndHintsOptions {
  source: SqlQuery;
  where: SqlExpression;
  splitColumns: ExpressionMeta[];
  timeBucket: string;
  showColumns: ExpressionMeta[];
  multipleValueMode: MultipleValueMode;
  measures: Measure[];
  compares: Compare[];
  compareTypes: CompareType[];
  maxRows: number;
  orderBy: SqlOrderByExpression | undefined;
}

function makeFilteredCompareTableQueryAndHints(
  options: MakeFilteredCompareTableQueryAndHintsOptions,
): QueryAndHints {
  const {
    source,
    where,
    splitColumns,
    timeBucket,
    showColumns,
    multipleValueMode,
    measures,
    compares,
    compareTypes,
    maxRows,
    orderBy,
  } = options;

  let decodedOrderBy = decodeTableOrderBy(orderBy, true, splitColumns, showColumns, measures);
  let effectiveOrderBy: SqlOrderByExpression;
  if (decodedOrderBy) {
    effectiveOrderBy = orderBy!;
  } else {
    effectiveOrderBy = C(
      measures[0]?.name || splitColumns[0]?.name || showColumns[0]?.name,
    ).toOrderByExpression('DESC');
    decodedOrderBy = decodeTableOrderBy(
      effectiveOrderBy,
      true,
      splitColumns,
      showColumns,
      measures,
    );
    if (!decodedOrderBy) {
      throw new Error('should never get here: must be able to decode own default order by');
    }
  }

  const mainGroupByExpressions = splitColumns.map(splitColumn =>
    toGroupByExpression(splitColumn, timeBucket, undefined, undefined),
  );

  const showColumnExpressions: SqlAlias[] = showColumns.map(showColumn =>
    toShowColumnExpression(showColumn, multipleValueMode, undefined),
  );

  const { commonWhere, mainWherePart, perCompareWhereParts } = computeWhereForCompares(
    where,
    compares,
    splitColumns.some(isTimestamp) ? timeBucket : undefined,
  );

  const columnHints = makeBaseColumnHints(splitColumns, timeBucket, showColumns);
  const query = SqlQuery.from(source.as('t'))
    .addWhere(commonWhere)
    .applyForEach(mainGroupByExpressions, (q, groupByExpression) =>
      q.addSelect(groupByExpression, {
        addToGroupBy: 'end',
      }),
    )
    .applyForEach(showColumnExpressions, (q, showColumnExpression) =>
      q.addSelect(showColumnExpression),
    )
    .applyForEach(measures, (q, measure) =>
      q.addSelect(
        measure.expression
          .addFilterToAggregations(mainWherePart, KNOWN_AGGREGATIONS)
          .apply(coalesce0)
          .as(measure.name),
      ),
    )
    .applyForEach(
      compares.flatMap((compare, compareIndex) => {
        return measures.flatMap(measure => {
          const measureName = measure.name;

          const mainMeasure = measure.expression
            .addFilterToAggregations(mainWherePart, KNOWN_AGGREGATIONS)
            .apply(coalesce0);

          const prevMeasure = measure.expression
            .addFilterToAggregations(perCompareWhereParts[compareIndex], KNOWN_AGGREGATIONS)
            .apply(coalesce0);

          return makeCompareAggregatorsAndAddHints(
            measureName,
            compare,
            compareTypes,
            mainMeasure,
            prevMeasure,
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
  for (const measure of measures) {
    columnHints.set(measure.name, { group: 'Current' });
  }

  return {
    query,
    columnHints,
  };
}
