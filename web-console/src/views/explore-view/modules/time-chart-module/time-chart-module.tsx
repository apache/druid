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
import { ColorAssigner } from '../../../../singletons';
import {
  capitalizeFirst,
  FINE_GRANULARITY_OPTIONS,
  getAutoGranularity,
  getTimeSpanInExpression,
  smartTimeFloor,
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
import { ContinuousChartRender } from './continuous-chart-render';

const TIME_NAME = 't';
const MEASURE_NAME_PREFIX = 'm';
const FACET_NAME = 'f';
const MIN_SLICE_WIDTH = 8;

function getMeasureName(index: number): string {
  return `${MEASURE_NAME_PREFIX}${index}`;
}

const OTHER_VALUE = 'Other';
const OTHER_COLOR = '#666666';

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
  markType: ContinuousChartMarkType;
  granularity: string;
  facetColumn?: ExpressionMeta;
  maxFacets: number;
  showOthers: boolean;
  measures: ExpressionMeta[];
  curveType: ContinuousChartCurveType;
}

ModuleRepository.registerModule<TimeChartParameterValues>({
  id: 'time-chart',
  title: 'Time chart',
  icon: IconNames.TIMELINE_LINE_CHART,
  parameters: {
    markType: {
      type: 'option',
      options: ['line', 'area', 'bar'],
      defaultValue: 'line',
      optionLabels: capitalizeFirst,
    },
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
        if (typeof filterSpan !== 'number') {
          // If we have no span apply a default span
          filterSpan = new Duration('P1M').getCanonicalLength();
        }
        return [
          'auto',
          ...FINE_GRANULARITY_OPTIONS.filter(g => {
            const len = new Duration(g).getCanonicalLength();
            return filterSpan < len * 1000 && len <= filterSpan;
          }),
        ];
      },
      defaultValue: 'auto',
      important: true,
      optionLabels: g => (g === 'auto' ? 'Auto' : new Duration(g).getDescription(true)),
    },
    facetColumn: {
      type: 'expression',
      label: 'Facet by',
      transferGroup: 'show',
      important: true,
      legacyName: 'splitColumn',
    },
    maxFacets: {
      type: 'number',
      defaultValue: 7,
      min: 1,
      required: true,
      visible: ({ parameterValues }) => Boolean(parameterValues.facetColumn),
      legacyName: 'numberToStack',
    },
    showOthers: {
      type: 'boolean',
      defaultValue: true,
      visible: ({ parameterValues }) => Boolean(parameterValues.facetColumn),
    },
    measures: {
      type: 'measures',
      transferGroup: 'show-agg',
      defaultValue: ({ querySource }) => querySource?.getFirstAggregateMeasureArray(),
      nonEmpty: true,
      important: true,
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
    const {
      querySource,
      timezone,
      where,
      setWhere,
      moduleWhere,
      parameterValues,
      stage,
      runSqlQuery,
    } = props;

    const timeColumnName = querySource.columns.find(column => column.sqlType === 'TIMESTAMP')?.name;
    const timeGranularity =
      parameterValues.granularity === 'auto'
        ? getAutoGranularity(
            where,
            timeColumnName || '__time',
            Math.floor(Math.max(stage.width - 80, 10) / MIN_SLICE_WIDTH),
          )
        : parameterValues.granularity;

    const { facetColumn, maxFacets, showOthers, measures, markType } = parameterValues;

    const dataQuery = useMemo(() => {
      return {
        querySource,
        timezone,
        where,
        moduleWhere,
        timeGranularity,
        measures,
        facetExpression: facetColumn?.expression,
        maxFacets,
        showOthers,
        oneExtra: markType !== 'bar',
      };
    }, [
      querySource,
      timezone,
      where,
      moduleWhere,
      timeGranularity,
      measures,
      facetColumn,
      maxFacets,
      showOthers,
      markType,
    ]);

    const [sourceDataState, queryManager] = useQueryManager({
      query: dataQuery,
      processQuery: async (
        {
          querySource,
          timezone,
          where,
          moduleWhere,
          timeGranularity,
          measures,
          facetExpression,
          maxFacets,
          showOthers,
          oneExtra,
        },
        signal,
      ) => {
        if (!timeColumnName) {
          throw new Error(`Must have a column of type TIMESTAMP for the time chart to work`);
        }

        const effectiveWhere = where.and(moduleWhere);
        const granularity = new Duration(timeGranularity);

        const detectedFacets: string[] | undefined = facetExpression
          ? (
              await runSqlQuery(
                {
                  query: querySource
                    .getInitQuery(effectiveWhere)
                    .addSelect(facetExpression.cast('VARCHAR').as(FACET_NAME), {
                      addToGroupBy: 'end',
                    })
                    .changeOrderByExpression(measures[0].expression.toOrderByExpression('DESC'))
                    .changeLimitValue(maxFacets + (showOthers ? 1 : 0)), // If we want to show others add 1 to check if we need to query for them
                  timezone,
                },
                signal,
              )
            ).getColumnByIndex(0)!
          : undefined;

        signal.throwIfAborted();

        if (detectedFacets?.length === 0) {
          // If detectedFacets is empty then there is no data at all and no need to do a larger query
          return {
            effectiveFacets: [],
            sourceData: [],
            measures,
            granularity,
          };
        }

        const facetsToQuery =
          showOthers && detectedFacets && maxFacets < detectedFacets.length
            ? detectedFacets.slice(0, maxFacets)
            : undefined;
        const effectiveFacets = facetsToQuery ? facetsToQuery.concat(OTHER_VALUE) : detectedFacets;

        let query = querySource
          .getInitQuery(overqueryWhere(effectiveWhere, timeColumnName, granularity, oneExtra))
          .applyIf(facetExpression && detectedFacets && !facetsToQuery, q =>
            q.addWhere(facetExpression!.cast('VARCHAR').in(detectedFacets!)),
          )
          .addSelect(
            smartTimeFloor(C(timeColumnName), timeGranularity, timezone.isUTC()).as(TIME_NAME),
            {
              addToGroupBy: 'end',
              addToOrderBy: 'end',
              direction: 'DESC',
            },
          )
          .applyIf(facetExpression, q => {
            if (!facetExpression) return q; // Should never get here, doing this to make peace between eslint and TS
            return q.addSelect(
              (facetsToQuery
                ? SqlCase.ifThenElse(
                    facetExpression.in(facetsToQuery),
                    facetExpression,
                    L(OTHER_VALUE),
                  )
                : facetExpression
              )
                .cast('VARCHAR')
                .as(FACET_NAME),
              { addToGroupBy: 'end' },
            );
          });

        // Add all measures to the query
        for (let i = 0; i < measures.length; i++) {
          query = query.addSelect(measures[i].expression.as(getMeasureName(i)));
        }

        query = query.changeLimitValue(
          10000 * (effectiveFacets ? Math.min(effectiveFacets.length, 10) : 1),
        );

        const result = await runSqlQuery(
          {
            query,
            timezone,
          },
          signal,
        );

        const dataset = result.toObjectArray().map(
          (b): RangeDatum => ({
            start: b[TIME_NAME].valueOf(),
            end: granularity.shift(b[TIME_NAME], Timezone.UTC, 1).valueOf(),
            measures: measures.map((_, i) => b[getMeasureName(i)]),
            facet: b[FACET_NAME],
          }),
        );

        return {
          effectiveFacets,
          sourceData: dataset,
          measures,
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
            facets={sourceData.effectiveFacets}
            facetColorizer={v => {
              if (!facetColumn) return '#000000';
              if (v === OTHER_VALUE) return OTHER_COLOR;
              return ColorAssigner.getColorForDimensionValue(facetColumn.name, v);
            }}
            granularity={sourceData.granularity}
            markType={parameterValues.markType}
            curveType={parameterValues.curveType}
            measures={measures}
            stage={stage}
            timezone={timezone}
            yAxisPosition="right"
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
