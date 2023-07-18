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

import { getInitQuery } from '../utils';

export default typedVisualModule({
  parameters: {
    timeGranularity: {
      type: 'option',
      options: ['PT1M', 'PT5M', 'PT30M', 'PT1H', 'P1D'],
      default: 'PT1H',
      control: {
        optionLabels: {
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
  module: ({ container, host }) => {
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

    const resizeHandler = () => {
      myChart.resize();
    };

    window.addEventListener('resize', resizeHandler);

    return {
      async update({ table, where, parameterValues }) {
        const { timeGranularity, metrics } = parameterValues;

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
      },
      destroy() {
        window.removeEventListener('resize', resizeHandler);
        myChart.dispose();
      },
    };
  },
});
