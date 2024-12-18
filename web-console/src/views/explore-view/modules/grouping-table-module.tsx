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
import { IconNames } from '@blueprintjs/icons';
import type { SqlExpression, SqlOrderByDirection, SqlQuery } from 'druid-query-toolkit';
import { C, F } from 'druid-query-toolkit';
import { useMemo } from 'react';

import { Loader } from '../../../components';
import { useQueryManager } from '../../../hooks';
import { formatInteger } from '../../../utils';
import { calculateInitPageSize, GenericOutputTable } from '../components';
import type { ExpressionMeta, Measure } from '../models';
import { ModuleRepository } from '../module-repository/module-repository';
import type {
  Compare,
  CompareStrategy,
  CompareType,
  MultipleValueMode,
  QueryAndHints,
  RestrictTop,
} from '../utils';
import { DEFAULT_TOP_VALUES_K, makeTableQueryAndHints } from '../utils';

import './grouping-table-module.scss';

// As of this writing ordering the outer query on something other than __time sometimes throws an error, set this to false / remove it
// when ordering on non __time is more robust
const NEEDS_GROUPING_TO_ORDER = true;

interface QueryAndMore {
  originalWhere: SqlExpression;
  queryAndHints: QueryAndHints;
}

interface GroupingTableParameterValues {
  splitColumns: ExpressionMeta[];
  timeBucket: string;
  showColumns: ExpressionMeta[];
  multipleValueMode: MultipleValueMode;
  pivotColumn: ExpressionMeta;
  maxPivotValues: number;
  measures: Measure[];
  compares: Compare[];
  compareStrategy: CompareStrategy;
  compareTypes: CompareType[];
  restrictTop: RestrictTop;
  maxRows: number;

  // String
  orderByColumn?: string;
  orderByDirection: SqlOrderByDirection;
}

ModuleRepository.registerModule<GroupingTableParameterValues>({
  id: 'grouping-table',
  title: 'Grouping table',
  icon: IconNames.PANEL_TABLE,
  parameters: {
    splitColumns: {
      type: 'expressions',
      label: 'Group by',
      transferGroup: 'show',
      defaultValue: [],
    },

    timeBucket: {
      type: 'option',
      label: 'Time bucket',
      options: ['PT1M', 'PT5M', 'PT1H', 'P1D', 'P1M'],
      optionLabels: {
        PT1M: '1 minute',
        PT5M: '5 minutes',
        PT1H: '1 hour',
        P1D: '1 day',
        P1M: '1 month',
      },
      defaultValue: 'PT1H',
      visible: ({ parameterValues }) =>
        (parameterValues.splitColumns || []).some((c: any) => c.name === '__time'),
    },

    showColumns: {
      type: 'expressions',
      label: 'Show columns',
      defaultValue: [],
    },
    multipleValueMode: {
      type: 'option',
      label: 'For shown column with multiple values...',
      options: ['null', 'latest', 'latestNonNull', 'count'],
      optionLabels: {
        null: 'Show null',
        latest: 'Show latest value',
        latestNonNull: 'Show latest value (non-null)',
        count: `Show '<count> values'`,
      },
      defaultValue: 'null',
      sticky: true,
      visible: ({ parameterValues }) => Boolean((parameterValues.showColumns || []).length),
    },
    pivotColumn: {
      type: 'expression',
      label: 'Pivot column',
    },
    maxPivotValues: {
      type: 'number',
      defaultValue: 10,
      min: 2,
      max: 100,
      visible: ({ parameterValues }) => Boolean(parameterValues.pivotColumn),
    },
    measures: {
      type: 'measures',
      transferGroup: 'show-agg',
      defaultValue: querySource => querySource.getFirstAggregateMeasureArray(),
      nonEmpty: true,
    },

    compares: {
      type: 'options',
      label: 'Compares',
      options: ['PT1M', 'PT5M', 'PT1H', 'PT6H', 'P1D', 'P1M', 'P1Y'],
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

    compareStrategy: {
      type: 'option',
      options: ['auto', 'filtered', 'join'],
      defaultValue: 'auto',
      visible: ({ parameterValues }) => Boolean((parameterValues.compares || []).length),
    },
    compareTypes: {
      type: 'options',
      label: 'Compare types',
      options: ['value', 'delta', 'absDelta', 'percent', 'absPercent'],
      optionLabels: {
        value: 'Value',
        delta: 'Delta',
        absDelta: 'Abs. delta',
        percent: 'Percent',
        absPercent: 'Abs. percent',
      },
      defaultValue: ['value', 'delta'],
      visible: ({ parameterValues }) => Boolean((parameterValues.compares || []).length),
    },
    restrictTop: {
      type: 'option',
      label: `Restrict to top ${formatInteger(DEFAULT_TOP_VALUES_K)} values when...`,
      options: ['always', 'never'],
      defaultValue: 'always',
      visible: ({ parameterValues }) =>
        Boolean(
          (parameterValues.compares || []).length && parameterValues.compareStrategy !== 'filtered',
        ),
    },

    maxRows: {
      type: 'number',
      label: 'Max rows',
      defaultValue: 200,
      min: 1,
      max: 100000,
      required: true,
    },

    orderByColumn: {
      type: 'string',
      visible: false,
    },
    orderByDirection: {
      type: 'option',
      options: ['ASC', 'DESC'],
      defaultValue: 'ASC',
      visible: false,
    },
  },
  component: function GroupingTableModule(props) {
    const {
      stage,
      querySource,
      where,
      setWhere,
      parameterValues,
      setParameterValues,
      runSqlQuery,
    } = props;

    const pivotValueQuery = useMemo(() => {
      const { measures, pivotColumn } = parameterValues;
      const maxPivotValues = parameterValues.maxPivotValues || 10;
      if (!pivotColumn) return;

      return querySource
        .getInitQuery(where)
        .addSelect(pivotColumn.expression.as('v'), { addToGroupBy: 'end' })
        .changeOrderByExpression(
          (measures.length ? measures[0].expression : F.count()).toOrderByExpression('DESC'),
        )
        .changeLimitValue(maxPivotValues);
    }, [querySource, where, parameterValues]);

    const [pivotValueState, queryManager] = useQueryManager({
      query: pivotValueQuery,
      processQuery: async (pivotValueQuery: SqlQuery) => {
        return (await runSqlQuery(pivotValueQuery)).getColumnByName('v') as string[];
      },
    });

    const queryAndMore = useMemo((): QueryAndMore | undefined => {
      const pivotValues = pivotValueState.data;
      if (parameterValues.pivotColumn && !pivotValues) return;
      const { orderByColumn, orderByDirection } = parameterValues;
      const orderBy = orderByColumn
        ? C(orderByColumn).toOrderByExpression(orderByDirection)
        : undefined;

      return {
        originalWhere: where,
        queryAndHints: makeTableQueryAndHints({
          source: querySource.query,
          where,
          splitColumns: parameterValues.splitColumns,
          timeBucket: parameterValues.timeBucket,
          showColumns: parameterValues.showColumns,
          multipleValueMode: parameterValues.multipleValueMode,
          pivotColumn: parameterValues.pivotColumn,
          pivotValues,
          measures: parameterValues.measures,
          compares: parameterValues.compares || [],
          compareStrategy: parameterValues.compareStrategy,
          compareTypes: parameterValues.compareTypes,
          restrictTop: parameterValues.restrictTop,
          maxRows: parameterValues.maxRows,
          orderBy,
          useGroupingToOrderSubQueries: NEEDS_GROUPING_TO_ORDER,
        }),
      };
    }, [querySource.query, where, parameterValues, pivotValueState.data]);

    const [resultState] = useQueryManager({
      query: queryAndMore,
      processQuery: async (queryAndMore, cancelToken) => {
        const { originalWhere, queryAndHints } = queryAndMore;
        const { query, columnHints } = queryAndHints;
        let result = await runSqlQuery(query, cancelToken);
        if (result.sqlQuery) {
          result = result.attachQuery(
            { query: '' },
            result.sqlQuery.changeWhereExpression(originalWhere),
          );
        }
        return {
          result,
          columnHints,
        };
      },
    });

    const resultData = resultState.getSomeData();
    return (
      <div className="grouping-table-module module">
        {resultState.error ? (
          <div>
            <div>{resultState.getErrorMessage()}</div>
            {resultState.getErrorMessage()?.includes('not found in any table') &&
              parameterValues.orderByColumn && (
                <Button
                  text="Clear order by"
                  onClick={() => setParameterValues({ orderByColumn: undefined })}
                />
              )}
          </div>
        ) : resultData ? (
          <GenericOutputTable
            queryResult={resultData.result}
            columnHints={resultData.columnHints}
            showTypeIcons={false}
            onOrderByChange={(columnName, desc) => {
              if (parameterValues.orderByColumn === columnName) {
                setParameterValues({
                  orderByDirection: parameterValues.orderByDirection === 'ASC' ? 'DESC' : 'ASC',
                });
              } else {
                setParameterValues({
                  orderByColumn: columnName,
                  orderByDirection: desc ? 'DESC' : 'ASC',
                });
              }
            }}
            onWhereChange={setWhere}
            initPageSize={calculateInitPageSize(stage.height)}
          />
        ) : undefined}
        {resultState.loading && (
          <Loader cancelText="Cancel query" onCancel={() => queryManager.cancelCurrent()} />
        )}
      </div>
    );
  },
});
