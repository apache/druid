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
import type { SqlExpression, SqlOrderByExpression, SqlQuery } from '@druid-toolkit/query';
import { C, F, SqlFunction, SqlLiteral } from '@druid-toolkit/query';
import type { ExpressionMeta, Host } from '@druid-toolkit/visuals-core';
import { typedVisualModule } from '@druid-toolkit/visuals-core';
import React, { useMemo, useState } from 'react';
import ReactDOM from 'react-dom';

import { Loader } from '../../../components';
import { useQueryManager } from '../../../hooks';
import { formatInteger } from '../../../utils';
import { getInitQuery } from '../utils';

import { GenericOutputTable } from './components';
import type { QueryAndHints } from './utils/table-query';
import { DEFAULT_TOP_VALUES_K, makeTableQueryAndHints } from './utils/table-query';

import './table-react-module.scss';

// As of this writing ordering the outer query on something other than __time sometimes throws an error, set this to false / remove it
// when ordering on non __time is more robust
const NEEDS_GROUPING_TO_ORDER = true;

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
    maxPivotValues: {
      type: 'number',
      default: 10,
      min: 2,
      max: 100,
      control: {
        visible: ({ params }) => Boolean(params.pivotColumn),
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
      options: ['PT1M', 'PT5M', 'PT1H', 'PT6H', 'P1D', 'P1M', 'P1Y'],
      control: {
        label: 'Compares',
        optionLabels: {
          PT1M: '1 minute',
          PT5M: '5 minutes',
          PT1H: '1 hour',
          PT6H: '6 hours',
          P1D: '1 day',
          P1M: '1 month',
          P1Y: '1 year',
        },
      },
    },

    compareStrategy: {
      type: 'option',
      options: ['auto', 'filtered', 'join'],
      default: 'auto',
      control: {
        visible: ({ params }) => Boolean((params.compares || []).length),
      },
    },
    compareTypes: {
      type: 'options',
      options: ['value', 'delta', 'absDelta', 'percent', 'absPercent'],
      default: ['value', 'delta'],
      control: {
        label: 'Compare types',
        visible: ({ params }) => Boolean((params.compares || []).length),
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
      type: 'option',
      options: ['always', 'never'],
      default: 'always',
      control: {
        label: `Restrict to top ${formatInteger(DEFAULT_TOP_VALUES_K)} values when...`,
        visible: ({ params }) =>
          Boolean((params.compares || []).length && params.compareStrategy !== 'filtered'),
      },
    },

    maxRows: {
      type: 'number',
      default: 200,
      min: 1,
      max: 100000,
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
    const maxPivotValues = parameterValues.maxPivotValues || 10;
    if (!pivotColumn) return;

    return getInitQuery(table, where)
      .addSelect(pivotColumn.expression.as('v'), { addToGroupBy: 'end' })
      .changeOrderByExpression(
        (metrics.length ? metrics[0].expression : F.count()).toOrderByExpression('DESC'),
      )
      .changeLimitValue(maxPivotValues);
  }, [table, where, parameterValues]);

  const [pivotValueState] = useQueryManager({
    query: pivotValueQuery,
    processQuery: async (pivotValueQuery: SqlQuery) => {
      return (await sqlQuery(pivotValueQuery)).getColumnByName('v') as string[];
    },
  });

  const queryAndHints = useMemo((): QueryAndHints | undefined => {
    const pivotValues = pivotValueState.data;
    if (parameterValues.pivotColumn && !pivotValues) return;
    return makeTableQueryAndHints({
      table,
      where,
      splitColumns: parameterValues.splitColumns,
      timeBucket: parameterValues.timeBucket,
      showColumns: parameterValues.showColumns,
      multipleValueMode: parameterValues.multipleValueMode,
      pivotColumn: parameterValues.pivotColumn,
      pivotValues,
      metrics: parameterValues.metrics,
      compares: parameterValues.compares || [],
      compareStrategy: parameterValues.compareStrategy,
      compareTypes: parameterValues.compareTypes,
      restrictTop: parameterValues.restrictTop,
      maxRows: parameterValues.maxRows,
      orderBy,
      useGroupingToOrderSubQueries: NEEDS_GROUPING_TO_ORDER,
    });
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
