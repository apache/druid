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

import { highlightStore } from '../highlight-store/highlight-store';
import { getInitQuery } from '../utils';

/**
 * Returns the cartesian coordinates of a pie slice external centroid
 */
function getCentroid(chart: echarts.ECharts, dataIndex: number) {
  // see these underscores everywhere? that's because those are private properties
  // I have no real choice but to use them, because there is no public API for this (on pie charts)
  // #no_ragrets
  const layout = (chart as any)._chartsViews?.[0]?._data?._itemLayouts?.[dataIndex];

  if (!layout) return;

  const { cx, cy, startAngle, endAngle, r } = layout;
  const angle = (startAngle + endAngle) / 2;

  const x = cx + Math.cos(angle) * r;
  const y = cy + Math.sin(angle) * r;

  return { x, y };
}

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
  module: ({ container, host, updateWhere }) => {
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
          id: 'hello',
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

    return {
      async update({ table, where, parameterValues }) {
        const { splitColumn, metric, limit } = parameterValues;

        if (!splitColumn) return;

        myChart.off('click');

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
            getInitQuery(
              table,
              where.changeClauseInWhere(
                C(splitColumn.name).notIn(result.getColumnByIndex(0)!),
              ) as SqlExpression,
            ).addSelect(metric.expression.as('value')),
          );

          data.push({ name: 'Others', value: others.rows[0][0], __isOthers: true });
        }

        myChart.setOption({
          series: [
            {
              id: 'hello',
              data,
            },
          ],
        });

        myChart.on('click', 'series', p => {
          if (highlightStore.getState().highlight?.data.name === p.name) {
            highlightStore.getState().dropHighlight();
            return;
          }

          const centroid = getCentroid(myChart, p.dataIndex);

          if (!centroid) return;

          const { name, value, __isOthers } = p.data as any;

          highlightStore.getState().setHighlight({
            label: name + ': ' + value,
            x: centroid.x,
            y: centroid.y - 20,
            data: { name, value, dataIndex: p.dataIndex },
            onDrop: () => {
              highlightStore.getState().dropHighlight();
            },
            onSave: __isOthers
              ? undefined
              : () => {
                  updateWhere(where.toggleClauseInWhere(C(splitColumn.name).equal(name)));
                  highlightStore.getState().dropHighlight();
                },
          });
        });
      },

      resize() {
        myChart.resize();

        // if there is a highlight, update its x position
        // by calculating new pixel position from the highlight's data
        const highlight = highlightStore.getState().highlight;
        if (highlight) {
          const { dataIndex } = highlight.data;

          const centroid = getCentroid(myChart, dataIndex);

          if (!centroid) return;

          highlightStore.getState().updateHighlight({
            x: centroid.x,
            y: centroid.y - 20,
          });
        }
      },

      destroy() {
        myChart.dispose();
      },
    };
  },
});
