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

import { IconNames } from '@blueprintjs/icons';
import { Duration, Timezone } from 'chronoshift';
import type { SqlExpression } from 'druid-query-toolkit';
import { C, F, fitFilterPatterns, L, SqlCase } from 'druid-query-toolkit';
import { useMemo } from 'react';

import { Loader } from '../../../../components';
import { useQueryManager } from '../../../../hooks';
import {
  capitalizeFirst,
  FINE_GRANULARITY_OPTIONS,
  getAutoGranularity,
  getTimeSpanInExpression,
} from '../../../../utils';
import { Issue } from '../../components';
import type { ExpressionMeta } from '../../models';
import { ModuleRepository } from '../../module-repository/module-repository';
import { overqueryWhere, updateFilterClause } from '../../utils';

import type {
  ContinuousChartCurveType,
  ContinuousChartMarkType,
  Range,
  RangeDatum,
} from './continuous-chart-render';
import { ContinuousChartRender, OTHER_VALUE } from './continuous-chart-render';

const TIME_NAME = 't';
const MEASURE_NAME = 'm';
const STACK_NAME = 's';
const MIN_SLICE_WIDTH = 8;

function getRangeInExpression(
  expression: SqlExpression,
  timeColumnName: string,
  maxTime?: Date,
): Range | undefined {
  const patterns = fitFilterPatterns(expression);
  for (const pattern of patterns) {
    if (pattern.type === 'timeInterval' && pattern.column === timeColumnName) {
      return [pattern.start.valueOf(), pattern.end.valueOf()];
    } else if (pattern.type === 'timeRelative' && pattern.column === timeColumnName) {
      let anchor = pattern.anchor === 'timestamp' ? pattern.anchorTimestamp || new Date() : maxTime;
      if (!anchor) return;

      const timezone = pattern.timezone ? new Timezone(pattern.timezone) : Timezone.UTC;
      if (pattern.alignType && pattern.alignDuration) {
        const alignDuration = new Duration(pattern.alignDuration);
        anchor =
          pattern.alignType === 'floor'
            ? alignDuration.floor(anchor, timezone)
            : alignDuration.ceil(anchor, timezone);
      }

      if (pattern.shiftDuration && pattern.shiftStep) {
        anchor = new Duration(pattern.shiftDuration).shift(anchor, timezone, pattern.shiftStep);
      }

      const rangeStep = pattern.rangeStep || 1;
      const anchorWithRange = new Duration(pattern.rangeDuration).shift(
        anchor,
        timezone,
        -rangeStep,
      );

      return [
        (rangeStep >= 0 ? anchorWithRange : anchor).valueOf(),
        (rangeStep < 0 ? anchorWithRange : anchor).valueOf(),
      ];
    }
  }

  return;
}

interface TimeChartParameterValues {
  granularity: string;
  splitColumn?: ExpressionMeta;
  numberToStack: number;
  showOthers: boolean;
  measure: ExpressionMeta;
  markType: ContinuousChartMarkType;
  curveType: ContinuousChartCurveType;
}

ModuleRepository.registerModule<TimeChartParameterValues>({
  id: 'time-chart',
  title: 'Time chart',
  icon: IconNames.TIMELINE_LINE_CHART,
  parameters: {
    granularity: {
      type: 'option',
      options: ({ querySource, where }) => {
        let filterSpan: number | undefined;
        if (querySource) {
          const timeColumnName = querySource.columns.find(
            column => column.sqlType === 'TIMESTAMP',
          )?.name;
          if (timeColumnName) {
            filterSpan = getTimeSpanInExpression(where, timeColumnName);
          }
        }
        return [
          'auto',
          ...(typeof filterSpan === 'number'
            ? FINE_GRANULARITY_OPTIONS.filter(g => {
                const len = new Duration(g).getCanonicalLength();
                return filterSpan < len * 1000 && len <= filterSpan;
              })
            : FINE_GRANULARITY_OPTIONS),
        ];
      },
      defaultValue: 'auto',
      important: true,
      optionLabels: g => (g === 'auto' ? 'Auto' : new Duration(g).getDescription(true)),
    },
    splitColumn: {
      type: 'expression',
      label: 'Stack by',
      transferGroup: 'show',
      important: true,
    },
    numberToStack: {
      type: 'number',
      label: 'Max stacks',
      defaultValue: 7,
      min: 2,
      required: true,
      visible: ({ parameterValues }) => Boolean(parameterValues.splitColumn),
    },
    showOthers: {
      type: 'boolean',
      defaultValue: true,
      visible: ({ parameterValues }) => Boolean(parameterValues.splitColumn),
    },
    measure: {
      type: 'measure',
      label: 'Measure to show',
      transferGroup: 'show-agg',
      important: true,
      defaultValue: ({ querySource }) => querySource?.getFirstAggregateMeasure(),
      required: true,
    },
    markType: {
      type: 'option',
      options: ['area', 'bar', 'line'],
      defaultValue: 'area',
      optionLabels: capitalizeFirst,
    },
    curveType: {
      type: 'option',
      options: ['smooth', 'linear', 'step'],
      defaultValue: 'smooth',
      optionLabels: capitalizeFirst,
      defined: ({ parameterValues }) => parameterValues.markType !== 'bar',
    },
  },
  component: function TimeChartModule(props) {
    const { querySource, where, setWhere, parameterValues, stage, runSqlQuery } = props;

    const timeColumnName = querySource.columns.find(column => column.sqlType === 'TIMESTAMP')?.name;
    const timeGranularity =
      parameterValues.granularity === 'auto'
        ? getAutoGranularity(
            where,
            timeColumnName || '__time',
            Math.floor(Math.max(stage.width - 80, 10) / MIN_SLICE_WIDTH),
          )
        : parameterValues.granularity;

    const { splitColumn, numberToStack, showOthers, measure, markType } = parameterValues;

    const dataQuery = useMemo(() => {
      return {
        querySource,
        where,
        timeGranularity,
        measure,
        splitExpression: splitColumn?.expression,
        numberToStack,
        showOthers,
        oneExtra: markType !== 'bar',
      };
    }, [
      querySource,
      where,
      timeGranularity,
      measure,
      splitColumn,
      numberToStack,
      showOthers,
      markType,
    ]);

    const [sourceDataState, queryManager] = useQueryManager({
      query: dataQuery,
      processQuery: async (
        {
          querySource,
          where,
          timeGranularity,
          measure,
          splitExpression,
          numberToStack,
          showOthers,
          oneExtra,
        },
        cancelToken,
      ) => {
        if (!timeColumnName) {
          throw new Error(`Must have a column of type TIMESTAMP for the time chart to work`);
        }

        const granularity = new Duration(timeGranularity);

        const vs = splitExpression
          ? (
              await runSqlQuery(
                querySource
                  .getInitQuery(where)
                  .addSelect(splitExpression.cast('VARCHAR').as('v'), { addToGroupBy: 'end' })
                  .changeOrderByExpression(measure.expression.toOrderByExpression('DESC'))
                  .changeLimitValue(numberToStack),
                cancelToken,
              )
            ).getColumnByIndex(0)!
          : undefined;

        cancelToken.throwIfRequested();

        if (vs?.length === 0) {
          // If vs is empty then there is no data at all and no need to do a larger query
          return {
            effectiveVs: [],
            sourceData: [],
            measure,
            granularity,
          };
        }

        const effectiveVs = vs && showOthers ? vs.concat(OTHER_VALUE) : vs;

        const result = await runSqlQuery(
          querySource
            .getInitQuery(overqueryWhere(where, timeColumnName, granularity, oneExtra))
            .applyIf(splitExpression && vs && !showOthers, q =>
              q.addWhere(splitExpression!.cast('VARCHAR').in(vs!)),
            )
            .addSelect(F.timeFloor(C(timeColumnName), L(timeGranularity)).as(TIME_NAME), {
              addToGroupBy: 'end',
              addToOrderBy: 'end',
              direction: 'DESC',
            })
            .applyIf(splitExpression, q => {
              if (!splitExpression || !vs) return q; // Should never get here, doing this to make peace between eslint and TS
              return q.addSelect(
                (showOthers
                  ? SqlCase.ifThenElse(splitExpression.in(vs), splitExpression, L(OTHER_VALUE))
                  : splitExpression
                )
                  .cast('VARCHAR')
                  .as(STACK_NAME),
                { addToGroupBy: 'end' },
              );
            })
            .addSelect(measure.expression.as(MEASURE_NAME))
            .changeLimitValue(10000 * (effectiveVs ? Math.min(effectiveVs.length, 10) : 1)),
          cancelToken,
        );

        const dataset = result.toObjectArray().map(
          (b): RangeDatum => ({
            start: b[TIME_NAME].valueOf(),
            end: granularity.shift(b[TIME_NAME], Timezone.UTC, 1).valueOf(),
            measure: b[MEASURE_NAME],
            stack: b[STACK_NAME],
          }),
        );

        return {
          effectiveVs,
          sourceData: dataset,
          measure,
          granularity,
          maxTime: result.resultContext?.maxTime,
        };
      },
    });

    const sourceData = sourceDataState.getSomeData();
    const domainRange = getRangeInExpression(
      where,
      timeColumnName || '__time',
      sourceData?.maxTime,
    );
    const errorMessage = sourceDataState.getErrorMessage();
    return (
      <div className="time-chart-module module">
        {sourceData && (
          <ContinuousChartRender
            data={sourceData.sourceData}
            stacks={sourceData.effectiveVs}
            granularity={sourceData.granularity}
            markType={parameterValues.markType}
            curveType={parameterValues.curveType}
            stage={stage}
            yAxis="right"
            domainRange={domainRange}
            onChangeRange={([start, end]) => {
              setWhere(
                updateFilterClause(
                  where,
                  F(
                    'TIME_IN_INTERVAL',
                    C(timeColumnName || '__time'),
                    `${new Date(start).toISOString()}/${new Date(end).toISOString()}`,
                  ),
                ),
              );
            }}
          />
        )}
        {errorMessage && <Issue issue={errorMessage} />}
        {sourceDataState.loading && (
          <Loader cancelText="Cancel query" onCancel={() => queryManager.cancelCurrent()} />
        )}
      </div>
    );
  },
});
