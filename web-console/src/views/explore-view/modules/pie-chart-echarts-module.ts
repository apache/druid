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
        label: 'Slice column',
        // transferGroup: 'show',
        required: true,
      },
    },
    metric: {
      type: 'aggregate',
      default: { expression: SqlExpression.parse('COUNT(*)'), name: 'Count', sqlType: 'BIGINT' },
      control: {
        // transferGroup: 'show',
        required: true,
      },
    },
    limit: {
      type: 'number',
      default: 5,
      control: {
        label: 'Max slices to show',
        required: true,
      },
    },
    showOthers: {
      type: 'boolean',
      default: true,
      control: { label: 'Show others' },
    },
  },
  module: ({ container, host, getLastUpdateEvent, updateWhere }) => {
    const myChart = echarts.init(container, 'dark');

    myChart.setOption({
      tooltip: {
        trigger: 'item',
      },
      legend: {
        orient: 'vertical',
        left: 'left',
      },
      series: [
        {
          type: 'pie',
          radius: '50%',
          data: [],
          emphasis: {
            itemStyle: {
              shadowBlur: 10,
              shadowOffsetX: 0,
              shadowColor: 'rgba(0, 0, 0, 0.5)',
            },
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
        const { splitColumn, metric, limit } = parameterValues;

        if (!splitColumn) return;

        const result = await host.sqlQuery(
          getInitQuery(table, where)
            .addSelect(splitColumn.expression.as('name'), { addToGroupBy: 'end' })
            .addSelect(metric.expression.as('value'), {
              addToOrderBy: 'end',
              direction: 'DESC',
            })
            .changeLimitValue(limit),
        );

        const data = result.toObjectArray();

        if (parameterValues.showOthers) {
          const others = await host.sqlQuery(
            getInitQuery(table, where)
              .addSelect(metric.expression.as('value'))
              .addWhere(C(splitColumn.name).notIn(result.getColumnByIndex(0)!)),
          );

          data.push({ name: 'Others', value: others.rows[0][0] });
        }

        myChart.setOption({
          series: [
            {
              name: metric.name,
              data,
            },
          ],
        });
      },
      destroy() {
        window.removeEventListener('resize', resizeHandler);
        myChart.dispose();
      },
    };
  },
});
