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

import { C, F, L, SqlExpression } from '@druid-toolkit/query';
import { typedVisualModule } from '@druid-toolkit/visuals-core';
import * as echarts from 'echarts';

import { highlightStore } from '../highlight-store/highlight-store';
import { DATE_FORMAT, getAutoGranularity, getInitQuery, snapToGranularity } from '../utils';

export default typedVisualModule({
  parameters: {
    timeGranularity: {
      type: 'option',
      options: ['auto', 'PT1M', 'PT5M', 'PT30M', 'PT1H', 'P1D'],
      default: 'auto',
      control: {
        optionLabels: {
          auto: 'Auto',
          PT1M: 'Minute',
          PT5M: '5 minutes',
          PT30M: '30 minutes',
          PT1H: 'Hour',
          PT6H: '6 hours',
          P1D: 'Day',
        },
      },
    },
    metrics: {
      type: 'aggregates',
      default: [{ expression: SqlExpression.parse('COUNT(*)'), name: 'Count', sqlType: 'BIGINT' }],
      control: {
        label: 'Metrics to show',
        required: true,
        // transferGroup: 'show',
      },
    },
  },
  module: ({ container, host, updateWhere }) => {
    const myChart = echarts.init(container, 'dark');

    myChart.setOption({
      tooltip: {
        trigger: 'axis',
        axisPointer: {
          type: 'cross',
          label: {
            backgroundColor: '#6a7985',
          },
        },
      },
      legend: {
        data: [],
      },
      toolbox: {
        feature: {
          saveAsImage: {},
        },
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

    return {
      async update({ table, where, parameterValues, context }) {
        const { metrics } = parameterValues;

        const timeGranularity =
          parameterValues.timeGranularity === 'auto'
            ? getAutoGranularity(where, '__time')
            : parameterValues.timeGranularity;

        const dataset = (
          await host.sqlQuery(
            getInitQuery(table, where)
              .addSelect(F.timeFloor(C('__time'), L(timeGranularity)).as('time'), {
                addToGroupBy: 'end',
                addToOrderBy: 'end',
                direction: 'ASC',
              })
              .applyForEach(metrics, (q, metric) => q.addSelect(metric.expression.as(metric.name))),
          )
        ).toObjectArray();

        myChart.setOption(
          {
            dataset: {
              dimensions: ['time'].concat(metrics.map(m => m.name)),
              source: dataset,
            },
            grid: {
              right: metrics.length * 40,
            },
            yAxis: metrics.map(({ name }, i) => ({
              type: 'value',
              name: name,
              position: i === 0 ? 'left' : 'right',
              offset: i === 0 ? 0 : (i - 1) * 80,
              axisLine: {
                show: true,
              },
            })),
            series: metrics.map(({ name }, i) => ({
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
            context.timezone,
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
              updateWhere(
                where.changeClauseInWhere(
                  SqlExpression.parse(
                    `TIME_IN_INTERVAL(${C(
                      '__time',
                    )}, '${start.toISOString()}/${end.toISOString()}')`,
                  ),
                ) as SqlExpression,
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
      },

      resize() {
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
      },

      destroy() {
        myChart.dispose();
      },
    };
  },
});
