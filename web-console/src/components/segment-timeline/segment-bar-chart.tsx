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

import { C, F, L, N, sql, SqlExpression, SqlQuery } from 'druid-query-toolkit';
import { useMemo } from 'react';

import { END_OF_TIME_DATE, type Rule, RuleUtil, START_OF_TIME_DATE } from '../../druid-models';
import type { Capabilities } from '../../helpers';
import { useQueryManager } from '../../hooks';
import { Api } from '../../singletons';
import { Duration, filterMap, getApiArray, queryDruidSql, TZ_UTC } from '../../utils';
import { Loader } from '../loader/loader';

import type { IntervalRow } from './common';
import type { SegmentBarChartRenderProps } from './segment-bar-chart-render';
import { SegmentBarChartRender } from './segment-bar-chart-render';

import './segment-bar-chart.scss';

export interface SegmentBarChartProps
  extends Omit<
    SegmentBarChartRenderProps,
    'intervalRows' | 'datasourceRules' | 'datasourceRulesError'
  > {
  capabilities: Capabilities;
}

export const SegmentBarChart = function SegmentBarChart(props: SegmentBarChartProps) {
  const { capabilities, dateRange, shownDatasource, ...otherProps } = props;

  const intervalsQuery = useMemo(
    () => ({ capabilities, dateRange, shownDatasource: shownDatasource }),
    [capabilities, dateRange, shownDatasource],
  );

  const [intervalRowsState] = useQueryManager({
    query: intervalsQuery,
    processQuery: async ({ capabilities, dateRange, shownDatasource }, cancelToken) => {
      if (capabilities.hasSql()) {
        const query = SqlQuery.from(N('sys').table('segments'))
          .changeWhereExpression(
            SqlExpression.and(
              sql`"start" <= '${dateRange[1].toISOString()}' AND '${dateRange[0].toISOString()}' < "end"`,
              C('start').unequal(START_OF_TIME_DATE),
              C('end').unequal(END_OF_TIME_DATE),
              C('is_overshadowed').equal(0),
              shownDatasource ? C('datasource').equal(L(shownDatasource)) : undefined,
            ),
          )
          .addSelect(C('start'), { addToGroupBy: 'end' })
          .addSelect(C('end'), { addToGroupBy: 'end' })
          .addSelect(C('datasource'), { addToGroupBy: 'end' })
          .addSelect(C('is_realtime').as('realtime'), { addToGroupBy: 'end' })
          .addSelect(F.count().as('segments'))
          .addSelect(F.sum(C('size')).as('size'))
          .addSelect(F.sum(C('num_rows')).as('rows'))
          .toString();

        return (await queryDruidSql({ query }, cancelToken)).map(sr => {
          const start = new Date(sr.start);
          const end = new Date(sr.end);

          return {
            ...sr,
            start,
            end,
            realtime: Boolean(sr.realtime),
            originalTimeSpan: Duration.fromRange(start, end, TZ_UTC),
          } as IntervalRow;
        });
      } else {
        return filterMap(
          await getApiArray(
            `/druid/coordinator/v1/metadata/segments?includeOvershadowedStatus&includeRealtimeSegments&${
              shownDatasource ? `datasources=${Api.encodePath(shownDatasource)}` : ''
            }`,
            cancelToken,
          ),
          (segment: any) => {
            if (segment.overshadowed) return; // We have to include overshadowed segments to get the realtime segments in this API
            const [startStr, endStr] = segment.interval.split('/');
            if (startStr === START_OF_TIME_DATE && endStr === END_OF_TIME_DATE) return;
            const start = new Date(startStr);
            const end = new Date(endStr);
            if (!(start <= dateRange[1] && dateRange[0] < end)) return;

            return {
              start,
              end,
              datasource: segment.dataSource,
              realtime: Boolean(segment.realtime),
              originalTimeSpan: Duration.fromRange(start, end, TZ_UTC),
              segments: 1,
              size: segment.size,
              rows: segment.num_rows || 0, // segment.num_rows is really null on this API :-(
            } as IntervalRow;
          },
        );
      }
    },
  });

  const [allLoadRulesState] = useQueryManager({
    query: shownDatasource ? '' : undefined,
    processQuery: async (_, cancelToken) => {
      return (
        await Api.instance.get<Record<string, Rule[]>>('/druid/coordinator/v1/rules', {
          cancelToken,
        })
      ).data;
    },
  });

  const datasourceRules = useMemo(() => {
    const allLoadRules = allLoadRulesState.data;
    if (!allLoadRules || !shownDatasource) return;
    return {
      loadRules: (allLoadRules[shownDatasource] || []).toReversed(),
      defaultLoadRules: (allLoadRules[RuleUtil.DEFAULT_RULES_KEY] || []).toReversed(),
    };
  }, [allLoadRulesState.data, shownDatasource]);

  if (intervalRowsState.error) {
    return (
      <div className="empty-placeholder">
        <span className="error-text">{`Error when loading data: ${intervalRowsState.getErrorMessage()}`}</span>
      </div>
    );
  }

  const intervalRows = intervalRowsState.getSomeData();
  return (
    <>
      {intervalRows && (
        <SegmentBarChartRender
          intervalRows={intervalRows}
          datasourceRules={datasourceRules}
          datasourceRulesError={allLoadRulesState.getErrorMessage()}
          dateRange={dateRange}
          shownDatasource={shownDatasource}
          {...otherProps}
        />
      )}
      {intervalRowsState.loading && <Loader />}
    </>
  );
};
