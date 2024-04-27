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

import type { SqlOrderByExpression } from '@druid-toolkit/query';
import {
  C,
  F,
  SqlCase,
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
import { getInitQuery } from '../utils';

import { GenericOutputTable } from './components';
import { shiftTimeInWhere } from './utils/utils';

import './table-react-module.scss';

type MultipleValueMode = 'null' | 'empty' | 'latest' | 'latestNonNull' | 'count';

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

function toGroupByExpression(
  splitColumn: ExpressionMeta,
  timeBucket: string,
  compareShiftDuration?: string,
) {
  const { expression, sqlType, name } = splitColumn;
  return expression
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

interface QueryAndHints {
  query: SqlQuery;
  groupHints: string[];
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
      options: ['PT1M', 'PT5M', 'PT1H', 'P1D', 'P1M'],
      control: {
        label: 'Compares',
        optionLabels: {
          PT1M: '1 minute',
          PT5M: '5 minutes',
          PT1H: '1 hour',
          P1D: '1 day',
          P1M: '1 month',
        },
        visible: ({ params }) => !params.pivotColumn,
      },
    },

    showDelta: {
      type: 'boolean',
      control: {
        visible: ({ params }) => Boolean((params.compares || []).length),
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

  const queryAndHints = useMemo(() => {
    const splitColumns: ExpressionMeta[] = parameterValues.splitColumns;
    const timeBucket: string = parameterValues.timeBucket || 'PT1H';
    const showColumns: ExpressionMeta[] = parameterValues.showColumns;
    const multipleValueMode: MultipleValueMode = parameterValues.multipleValueMode || 'null';
    const pivotColumn: ExpressionMeta = parameterValues.pivotColumn;
    const metrics: ExpressionMeta[] = parameterValues.metrics;
    const compares: string[] = parameterValues.compares || [];
    const showDelta: boolean = parameterValues.showDelta;
    const maxRows: number = parameterValues.maxRows;

    const pivotValues = pivotColumn ? pivotValueState.data : undefined;
    if (pivotColumn && !pivotValues) return;

    const hasCompare = Boolean(compares.length);

    const mainQuery = getInitQuery(table, where)
      .applyForEach(splitColumns, (q, splitColumn) =>
        q.addSelect(toGroupByExpression(splitColumn, timeBucket), {
          addToGroupBy: 'end',
        }),
      )
      .applyForEach(showColumns, (q, showColumn) =>
        q.addSelect(toShowColumnExpression(showColumn, multipleValueMode)),
      )
      .applyForEach(pivotValues || [''], (q, pivotValue, i) =>
        q.applyForEach(metrics, (q, metric) =>
          q.addSelect(
            metric.expression
              .as(metric.name)
              .applyIf(pivotColumn, q =>
                q
                  .addFilterToAggregations(
                    pivotColumn.expression.equal(pivotValue),
                    KNOWN_AGGREGATIONS,
                  )
                  .as(`${metric.name}${i > 0 ? ` [${pivotValue}]` : ''}`),
              ),
          ),
        ),
      )
      .applyIf(metrics.length > 0 || splitColumns.length > 0, q =>
        q.changeOrderByExpression(
          orderBy || C(metrics[0]?.name || splitColumns[0]?.name).toOrderByExpression('DESC'),
        ),
      )
      .changeLimitValue(maxRows);

    if (!hasCompare) {
      return {
        query: mainQuery,
        groupHints: pivotColumn
          ? splitColumns
              .map(() => '')
              .concat(
                showColumns.map(() => ''),
                (pivotValues || []).flatMap(v => metrics.map(() => v)),
              )
          : [],
      };
    }

    const main = T('main');
    return {
      query: SqlQuery.from(main)
        .changeWithParts(
          [SqlWithPart.simple('main', mainQuery)].concat(
            compares.map((comparePeriod, i) =>
              SqlWithPart.simple(
                `compare${i}`,
                getInitQuery(table, shiftTimeInWhere(where, comparePeriod))
                  .applyForEach(splitColumns, (q, splitColumn) =>
                    q.addSelect(toGroupByExpression(splitColumn, timeBucket, comparePeriod), {
                      addToGroupBy: 'end',
                    }),
                  )
                  .applyForEach(metrics, (q, metric) =>
                    q.addSelect(metric.expression.as(metric.name)),
                  ),
              ),
            ),
          ),
        )
        .changeSelectExpressions(
          splitColumns
            .map(splitColumn => main.column(splitColumn.name).as(splitColumn.name))
            .concat(
              showColumns.map(showColumn => main.column(showColumn.name).as(showColumn.name)),
              metrics.map(metric => main.column(metric.name).as(metric.name)),
              compares.flatMap((_, i) =>
                metrics.flatMap(metric => {
                  const c = T(`compare${i}`).column(metric.name);

                  const ret = [SqlFunction.simple('COALESCE', [c, 0]).as(`#prev: ${metric.name}`)];

                  if (showDelta) {
                    ret.push(
                      F.stringFormat(
                        '%.1f%%',
                        SqlFunction.simple('SAFE_DIVIDE', [
                          SqlExpression.parse(`(${main.column(metric.name)} - ${c}) * 100.0`),
                          c,
                        ]),
                      ).as(`%chg: ${metric.name}`),
                    );
                  }

                  return ret;
                }),
              ),
            ),
        )
        .applyForEach(compares, (q, _comparePeriod, i) =>
          q.addLeftJoin(
            T(`compare${i}`),
            SqlExpression.and(
              ...splitColumns.map(splitColumn =>
                main
                  .column(splitColumn.name)
                  .isNotDistinctFrom(T(`compare${i}`).column(splitColumn.name)),
              ),
            ),
          ),
        ),
      groupHints: splitColumns
        .map(() => 'Current')
        .concat(
          showColumns.map(() => 'Current'),
          metrics.map(() => 'Current'),
          compares.flatMap(comparePeriod =>
            metrics
              .flatMap(() => (showDelta ? ['', ''] : ['']))
              .map(() => `Comparison to ${comparePeriod}`),
          ),
        ),
    };
  }, [table, where, parameterValues, orderBy, pivotValueState.data]);

  const [resultState] = useQueryManager({
    query: queryAndHints,
    processQuery: async (queryAndHints: QueryAndHints) => {
      const { query, groupHints } = queryAndHints;
      return {
        result: await sqlQuery(query),
        groupHints,
      };
    },
  });

  const resultData = resultState.getSomeData();
  return (
    <div className="table-module">
      {resultState.error ? (
        resultState.getErrorMessage()
      ) : resultData ? (
        <GenericOutputTable
          runeMode={false}
          queryResult={resultData.result}
          groupHints={resultData.groupHints}
          showTypeIcons={false}
          onOrderByChange={(headerIndex, desc) => {
            const idx = SqlLiteral.index(headerIndex);
            if (orderBy && String(orderBy.expression) === String(idx)) {
              setOrderBy(orderBy.reverseDirection());
            } else {
              setOrderBy(idx.toOrderByExpression(desc ? 'DESC' : 'ASC'));
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
