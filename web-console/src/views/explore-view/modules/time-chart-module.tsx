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
import { C, F, L, SqlCase } from 'druid-query-toolkit';
import type { ECharts } from 'echarts';
import * as echarts from 'echarts';
import { useEffect, useMemo, useRef } from 'react';

import { Loader } from '../../../components';
import { useQueryManager } from '../../../hooks';
import {
  Duration,
  formatInteger,
  formatNumber,
  prettyFormatIsoDateTick,
  prettyFormatIsoDateWithMsIfNeeded,
} from '../../../utils';
import { Issue } from '../components';
import { highlightStore } from '../highlight-store/highlight-store';
import type { ExpressionMeta } from '../models';
import { ModuleRepository } from '../module-repository/module-repository';
import { DATE_FORMAT, getAutoGranularity } from '../utils';

import './record-table-module.scss';

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

interface TimeChartParameterValues {
  timeGranularity: string;
  splitColumn?: ExpressionMeta;
  numberToStack: number;
  showOthers: boolean;
  measure: ExpressionMeta;
  snappyHighlight: boolean;
}

ModuleRepository.registerModule<TimeChartParameterValues>({
  id: 'time-chart',
  title: 'Time chart',
  icon: IconNames.TIMELINE_LINE_CHART,
  parameters: {
    timeGranularity: {
      type: 'option',
      options: ['auto', 'PT1M', 'PT5M', 'PT30M', 'PT1H', 'P1D'],
      defaultValue: 'auto',
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
    splitColumn: {
      type: 'expression',
      label: 'Stack by',
      transferGroup: 'show',
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
      defaultValue: querySource => querySource.getFirstAggregateMeasure(),
      required: true,
    },
    snappyHighlight: {
      type: 'boolean',
      label: 'Snap highlight to granularity',
      defaultValue: true,
      sticky: true,
    },
  },
  component: function TimeChartModule(props) {
    const { querySource, where, setWhere, parameterValues, stage, runSqlQuery } = props;
    const chartRef = useRef<ECharts>();

    const timeColumnName = querySource.columns.find(column => column.sqlType === 'TIMESTAMP')?.name;
    const timeGranularity =
      parameterValues.timeGranularity === 'auto'
        ? getAutoGranularity(where, timeColumnName || '__time')
        : parameterValues.timeGranularity;

    const { splitColumn, numberToStack, showOthers, measure, snappyHighlight } = parameterValues;

    const dataQuery = useMemo(() => {
      return {
        initQuery: querySource.getInitQuery(where),
        measure,
        splitExpression: splitColumn?.expression,
        numberToStack,
        showOthers,
      };
    }, [querySource, where, measure, splitColumn, numberToStack, showOthers]);

    const [sourceDataState, queryManager] = useQueryManager({
      query: dataQuery,
      processQuery: async (
        { initQuery, measure, splitExpression, numberToStack, showOthers },
        cancelToken,
      ) => {
        if (!timeColumnName) {
          throw new Error(`Must have a column of type TIMESTAMP for the time chart to work`);
        }

        const vs = splitExpression
          ? (
              await runSqlQuery(
                initQuery
                  .addSelect(splitExpression.as('v'), { addToGroupBy: 'end' })
                  .changeOrderByExpression(measure.expression.toOrderByExpression('DESC'))
                  .changeLimitValue(numberToStack),
                cancelToken,
              )
            ).getColumnByIndex(0)!
          : undefined;

        cancelToken.throwIfRequested();

        const dataset = (
          await runSqlQuery(
            initQuery
              .applyIf(splitExpression && vs && !showOthers, q =>
                q.addWhere(splitExpression!.in(vs!)),
              )
              .addSelect(F.timeFloor(C(timeColumnName), L(timeGranularity)).as(TIME_NAME), {
                addToGroupBy: 'end',
                addToOrderBy: 'end',
                direction: 'ASC',
              })
              .applyIf(splitExpression, q => {
                if (!splitExpression || !vs) return q; // Should never get here, doing this to make peace between eslint and TS
                return q.addSelect(
                  (showOthers
                    ? SqlCase.ifThenElse(splitExpression.in(vs), splitExpression, L(OTHERS_VALUE))
                    : splitExpression
                  ).as(STACK_NAME),
                  { addToGroupBy: 'end' },
                );
              })
              .addSelect(measure.expression.as(METRIC_NAME)),
            cancelToken,
          )
        ).toObjectArray();

        const effectiveVs = vs && showOthers ? vs.concat(OTHERS_VALUE) : vs;
        return {
          effectiveVs,
          sourceData: effectiveVs ? transformData(dataset, effectiveVs) : dataset,
          measure,
        };
      },
    });

    function setupChart(container: HTMLDivElement) {
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
              formatter(d: any) {
                if (d.axisDimension === 'x') {
                  return prettyFormatIsoDateWithMsIfNeeded(new Date(d.value).toISOString());
                } else {
                  return Math.abs(d.value) < 1 ? formatNumber(d.value) : formatInteger(d.value);
                }
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
      const data = sourceDataState.data;
      if (!myChart || !data) return;
      const { effectiveVs, sourceData, measure } = data;

      myChart.off('brush');
      myChart.off('brushend');

      myChart.on('brush', (params: any) => {
        if (!params.areas.length) return;

        // this is only used for the label and the data saved in the highlight
        // the positioning is done with the true coordinates until the user
        // releases the mouse button (in the `brushend` event)
        let start = params.areas[0].coordRange[0];
        let end = params.areas[0].coordRange[1];
        if (snappyHighlight) {
          const duration = new Duration(timeGranularity);
          start = duration.round(start, 'Etc/UTC');
          end = duration.round(end, 'Etc/UTC');
        }

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
            if (!timeColumnName) return;
            setWhere(
              where.changeClauseInWhere(
                F(
                  'TIME_IN_INTERVAL',
                  C(timeColumnName),
                  `${start.toISOString()}/${end.toISOString()}`,
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
              name: effectiveVs ? v : measure.name,
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
      // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [sourceDataState.data, snappyHighlight]);

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

    const errorMessage = sourceDataState.getErrorMessage();
    return (
      <div className="time-chart-module module">
        <div
          className="echart-container"
          ref={container => {
            if (chartRef.current || !container) return;
            chartRef.current = setupChart(container);
          }}
        />
        {errorMessage && <Issue issue={errorMessage} />}
        {sourceDataState.loading && (
          <Loader cancelText="Cancel query" onCancel={() => queryManager.cancelCurrent()} />
        )}
      </div>
    );
  },
});
