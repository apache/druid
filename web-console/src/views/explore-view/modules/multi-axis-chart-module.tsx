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

import { C, F, L, SqlExpression, SqlQuery } from '@druid-toolkit/query';
import type { ECharts } from 'echarts';
import * as echarts from 'echarts';
import React, { useEffect, useMemo, useRef } from 'react';

import { useQueryManager } from '../../../hooks';
import { prettyFormatIsoDateTick, prettyFormatIsoDateWithMsIfNeeded } from '../../../utils';
import { highlightStore } from '../highlight-store/highlight-store';
import type { ExpressionMeta } from '../models';
import { ModuleRepository } from '../module-repository/module-repository';
import { DATE_FORMAT, getAutoGranularity, snapToGranularity } from '../utils';

import './record-table-module.scss';

interface MultiAxisChartParameterValues {
  timeGranularity: string;
  measures: ExpressionMeta[];
}

ModuleRepository.registerModule<MultiAxisChartParameterValues>({
  id: 'multi-axis-chart',
  title: 'Multi-axis chart',
  parameters: {
    timeGranularity: {
      type: 'option',
      options: ['auto', 'PT1M', 'PT5M', 'PT30M', 'PT1H', 'P1D'],
      optionLabels: {
        auto: 'Auto',
        PT1M: 'Minute',
        PT5M: '5 minutes',
        PT30M: '30 minutes',
        PT1H: 'Hour',
        PT6H: '6 hours',
        P1D: 'Day',
      },
      defaultValue: 'auto',
    },
    measures: {
      type: 'measures',
      label: 'Measures to show',
      transferGroup: 'show',
      defaultValue: querySource => querySource.getFirstAggregateMeasureArray(),
      nonEmpty: true,
      required: true,
    },
  },
  component: function MultiAxisChartModule(props) {
    const { querySource, where, setWhere, parameterValues, stage, runSqlQuery } = props;
    const chartRef = useRef<ECharts>();

    const timeGranularity =
      parameterValues.timeGranularity === 'auto'
        ? getAutoGranularity(where, '__time')
        : parameterValues.timeGranularity;

    const { measures } = parameterValues;

    const dataQuery = useMemo(() => {
      const source = querySource.query;

      return SqlQuery.from(source)
        .addWhere(where)
        .addSelect(F.timeFloor(C('__time'), L(timeGranularity)).as('time'), {
          addToGroupBy: 'end',
          addToOrderBy: 'end',
          direction: 'ASC',
        })
        .applyForEach(measures, (q, measure) => q.addSelect(measure.expression.as(measure.name)));
    }, [querySource, where, timeGranularity, measures]);

    const [dataState] = useQueryManager({
      query: dataQuery,
      processQuery: async (query: SqlQuery) => {
        return (await runSqlQuery(query)).toObjectArray();
      },
    });

    function setupChart(container: HTMLDivElement) {
      const myChart = echarts.init(container, 'dark');

      myChart.setOption({
        tooltip: {
          trigger: 'axis',
          axisPointer: {
            type: 'cross',
            label: {
              backgroundColor: '#6a7985',
              formatter({ value }: any) {
                return prettyFormatIsoDateWithMsIfNeeded(new Date(value).toISOString());
              },
            },
          },
        },
        legend: {
          data: [],
        },
        brush: {
          toolbox: ['lineX'],
          xAxisIndex: 0,
        },
        grid: {
          left: '3%',
          right: '4%',
          bottom: '3%',
          containLabel: true,
        },
        xAxis: [
          {
            type: 'time',
            boundaryGap: false,
            axisLabel: {
              formatter(value: any) {
                return prettyFormatIsoDateTick(new Date(value));
              },
            },
          },
        ],
        yAxis: [
          {
            type: 'value',
          },
        ],
      });

      // auto-enables the brush tool on load
      myChart.dispatchAction({
        type: 'takeGlobalCursor',
        key: 'brush',
        brushOption: {
          brushType: 'lineX',
        },
      });

      return myChart;
    }

    useEffect(() => {
      return () => {
        const myChart = chartRef.current;
        if (!myChart) return;
        myChart.dispose();
      };
    }, []);

    useEffect(() => {
      const myChart = chartRef.current;
      const data = dataState.data;
      if (!myChart || !data) return;

      myChart.setOption(
        {
          dataset: {
            dimensions: ['time'].concat(measures.map(m => m.name)),
            source: data,
          },
          grid: {
            right: measures.length * 40,
          },
          yAxis: measures.map(({ name }, i) => ({
            type: 'value',
            name: name,
            position: i === 0 ? 'left' : 'right',
            offset: i === 0 ? 0 : (i - 1) * 80,
            axisLine: {
              show: true,
            },
          })),
          series: measures.map(({ name }, i) => ({
            name: name,
            type: 'line',
            showSymbol: false,
            yAxisIndex: i,
            encode: {
              x: 'time',
              y: name,
              itemId: name,
            },
          })),
        },
        {
          replaceMerge: ['yAxis', 'series'],
        },
      );

      myChart.on('brush', (params: any) => {
        if (!params.areas.length) return;

        // this is only used for the label and the data saved in the highlight
        // the positioning is done with the true coordinates until the user
        // releases the mouse button (in the `brushend` event)
        const { start, end } = snapToGranularity(
          params.areas[0].coordRange[0],
          params.areas[0].coordRange[1],
          timeGranularity,
          undefined, // context.timezone,
        );

        const x0 = myChart.convertToPixel({ xAxisIndex: 0 }, params.areas[0].coordRange[0]);
        const x1 = myChart.convertToPixel({ xAxisIndex: 0 }, params.areas[0].coordRange[1]);

        highlightStore.getState().setHighlight({
          label: DATE_FORMAT.formatRange(start, end),
          x: x0 + (x1 - x0) / 2,
          y: 40,
          data: { start, end },
          onDrop: () => {
            highlightStore.getState().dropHighlight();
            myChart.dispatchAction({
              type: 'brush',
              command: 'clear',
              areas: [],
            });
          },
          onSave: () => {
            setWhere(
              where.changeClauseInWhere(
                SqlExpression.parse(
                  `TIME_IN_INTERVAL(${C('__time')}, '${start.toISOString()}/${end.toISOString()}')`,
                ),
              ),
            );
            highlightStore.getState().dropHighlight();
            myChart.dispatchAction({
              type: 'brush',
              command: 'clear',
              areas: [],
            });
          },
        });
      });

      // once the user is done selecting a range, this will snap the start and end
      myChart.on('brushend', () => {
        const highlight = highlightStore.getState().highlight;
        if (!highlight) return;

        // this is already snapped
        const { start, end } = highlight.data;

        const x0 = myChart.convertToPixel({ xAxisIndex: 0 }, start);
        const x1 = myChart.convertToPixel({ xAxisIndex: 0 }, end);

        // positions the bubble on the snapped start and end
        highlightStore.getState().updateHighlight({
          x: x0 + (x1 - x0) / 2,
        });

        // gives the chart the snapped range to highlight
        // (will replace the area the user just selected)
        myChart.dispatchAction({
          type: 'brush',
          areas: [
            {
              brushType: 'lineX',
              coordRange: [start, end],
              xAxisIndex: 0,
            },
          ],
        });
      });
    }, [dataState.data]);

    useEffect(() => {
      const myChart = chartRef.current;
      if (!myChart) return;
      myChart.resize();

      // if there is a highlight, update its x position
      // by calculating new pixel position from the highlight's data
      const highlight = highlightStore.getState().highlight;
      if (highlight) {
        const { start, end } = highlight.data;

        const x0 = myChart.convertToPixel({ xAxisIndex: 0 }, start);
        const x1 = myChart.convertToPixel({ xAxisIndex: 0 }, end);

        highlightStore.getState().updateHighlight({
          x: x0 + (x1 - x0) / 2,
        });
      }
    }, [stage]);

    return (
      <div
        className="bar-chart-module module"
        ref={container => {
          if (chartRef.current || !container) return;
          chartRef.current = setupChart(container);
        }}
      />
    );
  },
});
