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

import { Button } from '@blueprintjs/core';
import type { SqlOrderByExpression, SqlTable } from '@druid-toolkit/query';
import {
  C,
  F,
  SqlCase,
  SqlColumn,
  SqlExpression,
  SqlFunction,
  SqlLiteral,
  SqlQuery,
  SqlWithPart,
  T,
} from '@druid-toolkit/query';
import type { ExpressionMeta, Host } from '@druid-toolkit/visuals-core';
import { typedVisualModule } from '@druid-toolkit/visuals-core';
import React, { useMemo, useState } from 'react';
import ReactDOM from 'react-dom';

import { Loader } from '../../../components';
import { useQueryManager } from '../../../hooks';
import type { ColumnHint } from '../../../utils';
import { formatInteger, formatPercent } from '../../../utils';
import { getInitQuery } from '../utils';

import { GenericOutputTable } from './components';
import { getWhereForCompares, shiftTimeInExpression } from './utils/utils';

import './table-react-module.scss';

type MultipleValueMode = 'null' | 'empty' | 'latest' | 'latestNonNull' | 'count';

type CompareType = 'value' | 'delta' | 'absDelta' | 'percent' | 'absPercent';

// As of this writing ordering the outer query on something other than __time sometimes throws an error, set this to false / remove it
// when ordering on non __time is more robust
const NEEDS_GROUPING_TO_ORDER = true;

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

const TOP_VALUES_NAME = 'top_values';
const TOP_VALUES_K = 5000;

function coalesce0(ex: SqlExpression) {
  return F('COALESCE', ex, 0);
}

function anyValue(ex: SqlExpression) {
  return F('ANY_VALUE', ex);
}

function addTableScope(expression: SqlExpression, newTableScope: string): SqlExpression {
  return expression.walk(ex => {
    if (ex instanceof SqlColumn && !ex.getTableName()) {
      return ex.changeTableName(newTableScope);
    }
    return ex;
  }) as SqlExpression;
}

function toGroupByExpression(
  splitColumn: ExpressionMeta,
  timeBucket: string,
  compareShiftDuration?: string,
) {
  const { expression, sqlType, name } = splitColumn;
  return addTableScope(expression, 't')
    .applyIf(sqlType === 'TIMESTAMP' && compareShiftDuration, e =>
      F.timeShift(e, compareShiftDuration!, 1),
    )
    .applyIf(sqlType === 'TIMESTAMP', e => F.timeFloor(e, timeBucket))
    .as(name);
}

function toShowColumnExpression(
  showColumn: ExpressionMeta,
  mode: MultipleValueMode,
): SqlExpression {
  let ex: SqlExpression = SqlFunction.simple('LATEST', [showColumn.expression, 1024]);

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
        [showColumn.expression, 1024],
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

function getJoinCondition(
  splitColumns: ExpressionMeta[],
  table1: SqlTable,
  table2: SqlTable,
): SqlExpression {
  return SqlExpression.and(
    ...splitColumns.map(splitColumn =>
      table1.column(splitColumn.name).isNotDistinctFrom(table2.column(splitColumn.name)),
    ),
  );
}

interface QueryAndHints {
  query: SqlQuery;
  columnHints: Map<string, ColumnHint>;
}

export default typedVisualModule({
  parameters: {
    splitColumns: {
      type: 'columns',
      control: {
        label: 'Group by',
        // transferGroup: 'show',
      },
    },

    timeBucket: {
      type: 'option',
      options: ['PT1M', 'PT5M', 'PT1H', 'P1D', 'P1M'],
      default: 'PT1H',
      control: {
        label: 'Time bucket',
        optionLabels: {
          PT1M: '1 minute',
          PT5M: '5 minutes',
          PT1H: '1 hour',
          P1D: '1 day',
          P1M: '1 month',
        },
        visible: ({ params }) => (params.splitColumns || []).some((c: any) => c.name === '__time'),
      },
    },

    showColumns: {
      type: 'columns',
      control: {
        label: 'Show columns',
      },
    },
    multipleValueMode: {
      type: 'option',
      options: ['null', 'latest', 'latestNonNull', 'count'],
      control: {
        label: 'For shown column with multiple values...',
        optionLabels: {
          null: 'Show null',
          latest: 'Show latest value',
          latestNonNull: 'Show latest value (non-null)',
          count: `Show '<count> values'`,
        },
        visible: ({ params }) => Boolean((params.showColumns || []).length),
      },
    },
    pivotColumn: {
      type: 'column',
      control: {
        label: 'Pivot column',
      },
    },
    metrics: {
      type: 'aggregates',
      default: [{ expression: SqlFunction.count(), name: 'Count', sqlType: 'BIGINT' }],
      control: {
        label: 'Aggregates',
        // transferGroup: 'show-agg',
      },
    },

    compares: {
      type: 'options',
      options: ['PT1M', 'PT5M', 'PT1H', 'PT6H', 'P1D', 'P1M'],
      control: {
        label: 'Compares',
        optionLabels: {
          PT1M: '1 minute',
          PT5M: '5 minutes',
          PT1H: '1 hour',
          PT6H: '6 hours',
          P1D: '1 day',
          P1M: '1 month',
        },
        visible: ({ params }) => !params.pivotColumn,
      },
    },

    compareTypes: {
      type: 'options',
      options: ['value', 'delta', 'absDelta', 'percent', 'absPercent'],
      default: ['value', 'delta'],
      control: {
        label: 'Compare types',
        visible: ({ params }) => Boolean((params.compares || []).length) && !params.pivotColumn,
        optionLabels: {
          value: 'Value',
          delta: 'Delta',
          absDelta: 'Abs. delta',
          percent: 'Percent',
          absPercent: 'Abs. percent',
        },
      },
    },
    restrictTop: {
      type: 'boolean',
      default: true,
      control: {
        label: `Restrict to top ${formatInteger(TOP_VALUES_K)} when ordering on delta`,
        visible: ({ params }) => Boolean((params.compares || []).length) && !params.pivotColumn,
      },
    },

    maxRows: {
      type: 'number',
      default: 200,
      min: 1,
      max: 1000000,
      control: {
        label: 'Max rows',
        required: true,
      },
    },
  },
  module: ({ container, host, updateWhere }) => {
    return {
      update({ table, where, parameterValues }) {
        ReactDOM.render(
          <TableModule
            host={host}
            table={table}
            where={where}
            parameterValues={parameterValues}
            updateWhere={updateWhere}
          />,
          container,
        );
      },
      destroy() {
        ReactDOM.unmountComponentAtNode(container);
      },
    };
  },
});

interface TableModuleProps {
  host: Host;
  table: SqlExpression;
  where: SqlExpression;
  parameterValues: Record<string, any>;
  updateWhere: (where: SqlExpression) => void;
}

function TableModule(props: TableModuleProps) {
  const { host, table, where, parameterValues, updateWhere } = props;
  const { sqlQuery } = host;
  const [orderBy, setOrderBy] = useState<SqlOrderByExpression | undefined>();

  const pivotValueQuery = useMemo(() => {
    const pivotColumn: ExpressionMeta = parameterValues.pivotColumn;
    const metrics: ExpressionMeta[] = parameterValues.metrics;
    if (!pivotColumn) return;

    return getInitQuery(table, where)
      .addSelect(pivotColumn.expression.as('v'), { addToGroupBy: 'end' })
      .changeOrderByExpression(
        metrics.length
          ? metrics[0].expression.toOrderByExpression('DESC')
          : F.count().toOrderByExpression('DESC'),
      )
      .changeLimitValue(20);
  }, [table, where, parameterValues]);

  const [pivotValueState] = useQueryManager({
    query: pivotValueQuery,
    processQuery: async (pivotValueQuery: SqlQuery) => {
      return (await sqlQuery(pivotValueQuery)).getColumnByName('v') as string[];
    },
  });

  const queryAndHints = useMemo((): QueryAndHints | undefined => {
    const splitColumns: ExpressionMeta[] = parameterValues.splitColumns;
    const timeBucket: string = parameterValues.timeBucket || 'PT1H';
    const showColumns: ExpressionMeta[] = parameterValues.showColumns;
    const multipleValueMode: MultipleValueMode = parameterValues.multipleValueMode || 'null';
    const pivotColumn: ExpressionMeta = parameterValues.pivotColumn;
    const metrics: ExpressionMeta[] = parameterValues.metrics;
    const compares: string[] = parameterValues.compares || [];
    const compareTypes: CompareType[] = parameterValues.compareTypes;
    const restrictTop: boolean = parameterValues.restrictTop;
    const maxRows: number = parameterValues.maxRows;

    const pivotValues = pivotColumn ? pivotValueState.data : undefined;
    if (pivotColumn && !pivotValues) return;

    const effectiveOrderBy =
      orderBy || C(metrics[0]?.name || splitColumns[0]?.name).toOrderByExpression('DESC');

    const hasCompare = !pivotColumn && Boolean(compares.length) && Boolean(compareTypes.length);

    const orderByColumnName = (effectiveOrderBy.expression as SqlColumn).getName();
    let orderByCompareMeasure: string | undefined;
    let orderByCompareDuration: string | undefined;
    let orderByCompareType: CompareType | undefined;
    if (hasCompare) {
      const m = orderByColumnName.match(
        /^(.+):cmp:([^:]+):(value|delta|absDelta|percent|absPercent)$/,
      );
      if (m) {
        orderByCompareMeasure = m[1];
        orderByCompareDuration = m[2];
        orderByCompareType = m[3] as CompareType;
      }
    }

    const metricExpression = metrics.find(m => m.name === orderByCompareMeasure)?.expression;
    const topValuesQuery =
      restrictTop && metricExpression && orderByCompareType !== 'value' && splitColumns.length
        ? getInitQuery(table, getWhereForCompares(where, compares))
            .applyForEach(splitColumns, (q, splitColumn) =>
              q.addSelect(toGroupByExpression(splitColumn, timeBucket), {
                addToGroupBy: 'end',
              }),
            )
            .changeOrderByExpression(metricExpression.toOrderByExpression('DESC'))
            .changeLimitValue(TOP_VALUES_K)
        : undefined;

    const columnHints = new Map<string, ColumnHint>();
    const mainQuery = getInitQuery(table, where)
      .applyIf(topValuesQuery, q =>
        q.addInnerJoin(
          T(TOP_VALUES_NAME),
          getJoinCondition(splitColumns, T('t'), T(TOP_VALUES_NAME)),
        ),
      )
      .applyForEach(splitColumns, (q, splitColumn) =>
        q.addSelect(toGroupByExpression(splitColumn, timeBucket), {
          addToGroupBy: 'end',
        }),
      )
      .applyIf(!orderByCompareDuration, q =>
        q.applyForEach(showColumns, (q, showColumn) =>
          q.addSelect(toShowColumnExpression(showColumn, multipleValueMode)),
        ),
      )
      .applyForEach(pivotValues || [''], (q, pivotValue, i) =>
        q.applyForEach(metrics, (q, metric) => {
          const alias = `${metric.name}${pivotColumn && i > 0 ? `:${pivotValue}` : ''}`;
          if (pivotColumn) {
            columnHints.set(alias, { displayName: metric.name, group: pivotValue });
          }
          return q.addSelect(
            metric.expression
              .as(metric.name)
              .applyIf(pivotColumn, q =>
                q
                  .addFilterToAggregations(
                    pivotColumn.expression.equal(pivotValue),
                    KNOWN_AGGREGATIONS,
                  )
                  .as(alias),
              ),
          );
        }),
      )
      .applyIf(!orderByCompareDuration, q =>
        q
          .applyIf(metrics.length > 0 || splitColumns.length > 0, q =>
            q.changeOrderByExpression(effectiveOrderBy),
          )
          .changeLimitValue(maxRows),
      );

    if (!hasCompare) {
      return {
        query: mainQuery,
        columnHints,
      };
    }

    const main = T('main');
    const leader = T(orderByCompareDuration ? `compare_${orderByCompareDuration}` : 'main');
    const query = SqlQuery.from(leader)
      .changeWithParts(
        (
          (topValuesQuery
            ? [SqlWithPart.simple(TOP_VALUES_NAME, topValuesQuery)]
            : []) as SqlWithPart[]
        ).concat(
          SqlWithPart.simple('main', mainQuery),
          compares.map(compare =>
            SqlWithPart.simple(
              `compare_${compare}`,
              getInitQuery(table, shiftTimeInExpression(where, compare))
                .applyIf(topValuesQuery, q =>
                  q.addInnerJoin(
                    T(TOP_VALUES_NAME),
                    getJoinCondition(splitColumns, T('t'), T(TOP_VALUES_NAME)),
                  ),
                )
                .applyForEach(splitColumns, (q, splitColumn) =>
                  q.addSelect(toGroupByExpression(splitColumn, timeBucket, compare), {
                    addToGroupBy: 'end',
                  }),
                )
                .applyIf(orderByCompareDuration === compare, q =>
                  q.applyForEach(showColumns, (q, showColumn) =>
                    q.addSelect(toShowColumnExpression(showColumn, multipleValueMode)),
                  ),
                )
                .applyForEach(metrics, (q, metric) =>
                  q.addSelect(metric.expression.as(metric.name)),
                )
                .applyIf(compare === orderByCompareDuration && orderByCompareType === 'value', q =>
                  q
                    .changeOrderByExpression(
                      effectiveOrderBy.changeExpression(C(orderByCompareMeasure!)),
                    )
                    .changeLimitValue(maxRows),
                ),
            ),
          ),
        ),
      )
      .changeSelectExpressions(
        splitColumns
          .map(splitColumn => main.column(splitColumn.name).as(splitColumn.name))
          .concat(
            showColumns.map(showColumn =>
              leader
                .column(showColumn.name)
                .applyIf(NEEDS_GROUPING_TO_ORDER, anyValue)
                .as(showColumn.name),
            ),
            metrics.map(metric =>
              main
                .column(metric.name)
                .applyIf(NEEDS_GROUPING_TO_ORDER, anyValue)
                .applyIf(orderByCompareDuration, coalesce0)
                .as(metric.name),
            ),
            compares.flatMap(compare =>
              metrics.flatMap(metric => {
                const c = T(`compare_${compare}`)
                  .column(metric.name)
                  .applyIf(NEEDS_GROUPING_TO_ORDER, anyValue)
                  .applyIf(compare !== orderByCompareDuration, coalesce0);

                const mainMetric = main
                  .column(metric.name)
                  .applyIf(NEEDS_GROUPING_TO_ORDER, anyValue)
                  .applyIf(orderByCompareDuration, coalesce0);

                const diff = mainMetric.subtract(c);

                const ret: SqlExpression[] = [];

                if (compareTypes.includes('value')) {
                  const valueName = `${metric.name}:cmp:${compare}:value`;
                  columnHints.set(valueName, {
                    group: `Comparison to ${compare}`,
                    displayName: `${metric.name} (value)`,
                  });
                  ret.push(c.as(valueName));
                }

                if (compareTypes.includes('delta')) {
                  const deltaName = `${metric.name}:cmp:${compare}:delta`;
                  columnHints.set(deltaName, {
                    group: `Comparison to ${compare}`,
                    displayName: `${metric.name} (delta)`,
                  });
                  ret.push(diff.as(deltaName));
                }

                if (compareTypes.includes('absDelta')) {
                  const deltaName = `${metric.name}:cmp:${compare}:absDelta`;
                  columnHints.set(deltaName, {
                    group: `Comparison to ${compare}`,
                    displayName: `${metric.name} (Abs. delta)`,
                  });
                  ret.push(F('ABS', diff).as(deltaName));
                }

                if (compareTypes.includes('percent')) {
                  const percentName = `${metric.name}:cmp:${compare}:percent`;
                  columnHints.set(percentName, {
                    group: `Comparison to ${compare}`,
                    displayName: `${metric.name} (%)`,
                    formatter: formatPercent,
                  });
                  ret.push(
                    F('SAFE_DIVIDE', diff.multiply(SqlLiteral.ONE_POINT_ZERO), c).as(percentName),
                  );
                }

                if (compareTypes.includes('absPercent')) {
                  const percentName = `${metric.name}:cmp:${compare}:absPercent`;
                  columnHints.set(percentName, {
                    group: `Comparison to ${compare}`,
                    displayName: `${metric.name} (abs. %)`,
                    formatter: formatPercent,
                  });
                  ret.push(
                    F('ABS', F('SAFE_DIVIDE', diff.multiply(SqlLiteral.ONE_POINT_ZERO), c)).as(
                      percentName,
                    ),
                  );
                }

                return ret;
              }),
            ),
          ),
      )
      .applyIf(orderByCompareDuration, q =>
        q.addLeftJoin(
          main,
          getJoinCondition(splitColumns, main, T(`compare_${orderByCompareDuration}`)),
        ),
      )
      .applyForEach(
        compares.filter(c => c !== orderByCompareDuration),
        (q, compare) =>
          q.addLeftJoin(
            T(`compare_${compare}`),
            getJoinCondition(splitColumns, main, T(`compare_${compare}`)),
          ),
      )
      .applyIf(NEEDS_GROUPING_TO_ORDER, q =>
        q.changeGroupByExpressions(splitColumns.map((_, i) => SqlLiteral.index(i))),
      )
      .addOrderBy(effectiveOrderBy)
      .changeLimitValue(maxRows);

    for (const splitColumn of splitColumns) {
      columnHints.set(splitColumn.name, { group: 'Current' });
    }
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
  }, [table, where, parameterValues, orderBy, pivotValueState.data]);

  const [resultState] = useQueryManager({
    query: queryAndHints,
    processQuery: async (queryAndHints: QueryAndHints) => {
      const { query, columnHints } = queryAndHints;
      return {
        result: await sqlQuery(query),
        columnHints,
      };
    },
  });

  const resultData = resultState.getSomeData();
  return (
    <div className="table-module">
      {resultState.error ? (
        <div>
          <div>{resultState.getErrorMessage()}</div>
          {resultState.getErrorMessage()?.includes('not found in any table') && orderBy && (
            <Button text="Clear order by" onClick={() => setOrderBy(undefined)} />
          )}
        </div>
      ) : resultData ? (
        <GenericOutputTable
          runeMode={false}
          queryResult={resultData.result}
          columnHints={resultData.columnHints}
          showTypeIcons={false}
          onOrderByChange={(columnName, desc) => {
            const column = C(columnName);
            if (orderBy && orderBy.expression.equals(column)) {
              setOrderBy(orderBy.reverseDirection());
            } else {
              setOrderBy(column.toOrderByExpression(desc ? 'DESC' : 'ASC'));
            }
          }}
          onQueryAction={action => {
            const query = getInitQuery(table, where);
            if (!query) return;
            const nextQuery = action(query);
            const prevWhere = query.getWhereExpression() || SqlLiteral.TRUE;
            const nextWhere = nextQuery.getWhereExpression() || SqlLiteral.TRUE;
            if (prevWhere && nextWhere && !prevWhere.equals(nextWhere)) {
              updateWhere(nextWhere);
            }
          }}
        />
      ) : undefined}
      {resultState.loading && <Loader />}
    </div>
  );
}
