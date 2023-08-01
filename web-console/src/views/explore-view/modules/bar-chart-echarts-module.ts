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

import { C, SqlExpression } from '@druid-toolkit/query';
import { typedVisualModule } from '@druid-toolkit/visuals-core';
import * as echarts from 'echarts';

import { getInitQuery } from '../utils';

export default typedVisualModule({
  parameters: {
    splitColumn: {
      type: 'column',
      control: {
        label: 'Bar column',
        // transferGroup: 'show',
        required: true,
      },
    },
    metric: {
      type: 'aggregate',
      default: { expression: SqlExpression.parse('COUNT(*)'), name: 'Count', sqlType: 'BIGINT' },
      control: {
        label: 'Metric to show',
        // transferGroup: 'show-agg',
        required: true,
      },
    },
    metricToSort: {
      type: 'aggregate',
      control: {
        label: 'Metric to sort (default to shown)',
      },
    },
    limit: {
      type: 'number',
      default: 5,
      control: {
        label: 'Max bars to show',
        required: true,
      },
    },
  },
  module: ({ container, host, getLastUpdateEvent, updateWhere }) => {
    const { sqlQuery } = host;
    const myChart = echarts.init(container, 'dark');

    myChart.setOption({
      tooltip: {},
      dataset: {
        sourceHeader: false,
        dimensions: ['dim', 'met'],
        source: [],
      },
      xAxis: {
        type: 'category',
        axisLabel: { interval: 0, rotate: -30 },
      },
      yAxis: {},
      series: [
        {
          type: 'bar',
          encode: {
            x: 'dim',
            y: 'met',
          },
        },
      ],
    });

    const resizeHandler = () => {
      myChart.resize();
    };

    window.addEventListener('resize', resizeHandler);

    myChart.on('click', 'series', p => {
      const lastUpdateEvent = getLastUpdateEvent();
      if (!lastUpdateEvent?.parameterValues.splitColumn) return;

      updateWhere(
        lastUpdateEvent.where.toggleClauseInWhere(
          C(lastUpdateEvent.parameterValues.splitColumn.name).equal(p.name),
        ),
      );
    });

    return {
      async update({ table, where, parameterValues }) {
        const { splitColumn, metric, metricToSort, limit } = parameterValues;
        if (!splitColumn) return;

        const v = await sqlQuery(
          getInitQuery(table, where)
            .addSelect(splitColumn.expression.as('dim'), { addToGroupBy: 'end' })
            .addSelect(metric.expression.as('met'), {
              addToOrderBy: metricToSort ? undefined : 'end',
              direction: 'DESC',
            })
            .applyIf(metricToSort, q =>
              q.addOrderBy(metricToSort!.expression.toOrderByExpression('DESC')),
            )
            .changeLimitValue(limit),
        );
        myChart.setOption({
          dataset: {
            source: v.toObjectArray(),
          },
        });
      },
      destroy() {
        window.removeEventListener('resize', resizeHandler);
        myChart.dispose();
      },
    };
  },
});
