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

import { C, F, L, SqlCase, SqlExpression } from '@druid-toolkit/query';
import { typedVisualModule } from '@druid-toolkit/visuals-core';
import * as echarts from 'echarts';

import { highlightStore } from '../highlight-store/highlight-store';
import { DATE_FORMAT, getAutoGranularity, getInitQuery, snapToGranularity } from '../utils';

const TIME_NAME = '__t__';
const METRIC_NAME = '__met__';
const STACK_NAME = '__stack__';
const OTHERS_VALUE = 'Others';

function transformData(data: any[], vs: string[]): Record<string, number>[] {
  const zeroDatum = Object.fromEntries(vs.map(v => [v, 0]));

  let lastTime = -1;
  let lastDatum: Record<string, number> | undefined;
  const ret = [];
  for (const d of data) {
    const t = d[TIME_NAME];
    if (t.valueOf() !== lastTime) {
      if (lastDatum) ret.push(lastDatum);
      lastTime = t.valueOf();
      lastDatum = { ...zeroDatum, [TIME_NAME]: t };
    }
    lastDatum![d[STACK_NAME]] = d[METRIC_NAME];
  }
  if (lastDatum) ret.push(lastDatum);
  return ret;
}

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
    splitColumn: {
      type: 'column',
      control: {
        label: 'Stack by',
        // transferGroup: 'show',
      },
    },
    numberToStack: {
      type: 'number',
      default: 7,
      min: 2,
      control: {
        label: 'Max stacks',
        required: true,
        visible: ({ params }) => Boolean(params.splitColumn),
      },
    },
    showOthers: {
      type: 'boolean',
      default: true,
      control: {
        visible: ({ params }) => Boolean(params.splitColumn),
      },
    },
    metric: {
      type: 'aggregate',
      default: { expression: SqlExpression.parse('COUNT(*)'), name: 'Count', sqlType: 'BIGINT' },
      control: {
        label: 'Metric to show',
        required: true,
        // transferGroup: 'show-agg',
      },
    },
    snappyHighlight: {
      type: 'boolean',
      default: true,
      control: {
        label: 'Snap highlight to nearest dates',
      },
    },
  },
  module: ({ container, host, updateWhere }) => {
    const myChart = echarts.init(container, 'dark');

    myChart.setOption({
      dataset: {
        dimensions: [],
        source: [],
      },
      tooltip: {
        trigger: 'axis',
        transitionDuration: 0,
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
      series: [],
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
        const { splitColumn, metric, numberToStack, showOthers, snappyHighlight } = parameterValues;

        // this should probably be a parameter
        const timeColumnName = '__time';

        const timeGranularity =
          parameterValues.timeGranularity === 'auto'
            ? getAutoGranularity(where, timeColumnName)
            : parameterValues.timeGranularity;

        myChart.off('brush');
        myChart.off('brushend');

        const vs = splitColumn
          ? (
              await host.sqlQuery(
                getInitQuery(table, where)
                  .addSelect(splitColumn.expression.as('v'), { addToGroupBy: 'end' })
                  .changeOrderByExpression(metric.expression.toOrderByExpression('DESC'))
                  .changeLimitValue(numberToStack),
              )
            ).getColumnByIndex(0)!
          : undefined;

        const dataset = (
          await host.sqlQuery(
            getInitQuery(
              table,
              splitColumn && vs && !showOthers ? where.and(splitColumn.expression.in(vs)) : where,
            )
              .addSelect(F.timeFloor(C(timeColumnName), L(timeGranularity)).as(TIME_NAME), {
                addToGroupBy: 'end',
                addToOrderBy: 'end',
                direction: 'ASC',
              })
              .applyIf(splitColumn, q => {
                const splitEx = splitColumn!.expression;
                return q.addSelect(
                  (showOthers
                    ? SqlCase.ifThenElse(splitEx.in(vs!), splitEx, L(OTHERS_VALUE))
                    : splitEx
                  ).as(STACK_NAME),
                  { addToGroupBy: 'end' },
                );
              })
              .addSelect(metric.expression.as(METRIC_NAME)),
          )
        ).toObjectArray();

        const effectiveVs = vs && showOthers ? vs.concat(OTHERS_VALUE) : vs;
        const sourceData = effectiveVs ? transformData(dataset, effectiveVs) : dataset;

        myChart.on('brush', (params: any) => {
          if (!params.areas.length) return;

          // this is only used for the label and the data saved in the highlight
          // the positioning is done with the true coordinates until the user
          // releases the mouse button (in the `brushend` event)
          const { start, end } = snappyHighlight
            ? snapToGranularity(
                params.areas[0].coordRange[0],
                params.areas[0].coordRange[1],
                timeGranularity,
                context.timezone,
              )
            : { start: params.areas[0].coordRange[0], end: params.areas[0].coordRange[1] };

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
                      timeColumnName,
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

        const showSymbol = sourceData.length < 2;
        myChart.setOption(
          {
            dataset: {
              dimensions: [TIME_NAME].concat(effectiveVs || [METRIC_NAME]),
              source: sourceData,
            },
            animation: false,
            legend: effectiveVs
              ? {
                  data: effectiveVs,
                }
              : undefined,
            series: (effectiveVs || [METRIC_NAME]).map(v => {
              return {
                id: v,
                name: effectiveVs ? v : metric.name,
                type: 'line',
                stack: 'Total',
                showSymbol,
                lineStyle: v === OTHERS_VALUE ? { color: '#ccc' } : {},
                areaStyle: v === OTHERS_VALUE ? { color: '#ccc' } : {},
                emphasis: {
                  focus: 'series',
                },
                encode: {
                  x: TIME_NAME,
                  y: v,
                  itemId: v,
                },
              };
            }),
          },
          {
            replaceMerge: ['legend', 'series'],
          },
        );
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
