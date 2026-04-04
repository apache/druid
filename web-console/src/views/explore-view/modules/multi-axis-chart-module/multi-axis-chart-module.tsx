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

import { Button, Intent } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { Duration, Timezone } from 'chronoshift';
import { C, F, L } from 'druid-query-toolkit';
import type { ECharts } from 'echarts';
import * as echarts from 'echarts';
import { useEffect, useMemo, useRef, useState } from 'react';

import { Loader, PortalBubble, type PortalBubbleOpenOn } from '../../../../components';
import { useQueryManager } from '../../../../hooks';
import {
  formatInteger,
  formatIsoDateRange,
  formatNumber,
  getAutoGranularity,
  prettyFormatIsoDateWithMsIfNeeded,
  tickFormatWithTimezone,
} from '../../../../utils';
import { Issue } from '../../components';
import type { ExpressionMeta } from '../../models';
import { ModuleRepository } from '../../module-repository/module-repository';
import { updateFilterClause } from '../../utils';

interface MultiAxisChartHighlight extends PortalBubbleOpenOn {
  start: Date;
  end: Date;
}

interface MultiAxisChartParameterValues {
  measures: ExpressionMeta[];
  granularity: string;
}

ModuleRepository.registerModule<MultiAxisChartParameterValues>({
  id: 'multi-axis-chart',
  title: 'Multi-axis chart',
  icon: IconNames.SERIES_ADD,
  parameters: {
    measures: {
      type: 'measures',
      label: 'Measures to show',
      transferGroup: 'show',
      defaultValue: ({ querySource }) => querySource?.getFirstAggregateMeasureArray(),
      nonEmpty: true,
      required: true,
      important: true,
    },
    granularity: {
      type: 'option',
      options: ['auto', 'PT1M', 'PT5M', 'PT30M', 'PT1H', 'P1D'],
      optionLabels: g => (g === 'auto' ? 'Auto' : new Duration(g).getDescription(true)),
      defaultValue: 'auto',
    },
  },
  component: function MultiAxisChartModule(props) {
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
    const containerRef = useRef<HTMLDivElement>();
    const chartRef = useRef<ECharts>();
    const [highlight, setHighlight] = useState<MultiAxisChartHighlight | undefined>();

    const timeColumnName = querySource.columns.find(column => column.sqlType === 'TIMESTAMP')?.name;
    const timeGranularity =
      parameterValues.granularity === 'auto'
        ? getAutoGranularity(where, timeColumnName || '__time', 200)
        : parameterValues.granularity;

    const { measures } = parameterValues;

    const dataQuery = useMemo(() => {
      return {
        query: querySource
          .getInitQuery(where.and(moduleWhere))
          .addSelect(F.timeFloor(C(timeColumnName || '__time'), L(timeGranularity)).as('time'), {
            addToGroupBy: 'end',
            addToOrderBy: 'end',
            direction: 'ASC',
          })
          .applyForEach(measures, (q, measure) => q.addSelect(measure.expression.as(measure.name))),
        timezone,
      };
    }, [querySource, timezone, where, moduleWhere, timeColumnName, timeGranularity, measures]);

    const [sourceDataState, queryManager] = useQueryManager({
      query: dataQuery,
      processQuery: async (query, signal) => {
        if (!timeColumnName) {
          throw new Error(`Must have a column of type TIMESTAMP for the multi-axis chart to work`);
        }

        return (await runSqlQuery(query, signal)).toObjectArray();
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
                return tickFormatWithTimezone(new Date(value), timezone);
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
      const data = sourceDataState.data;
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

      myChart.off('brush');

      myChart.on('brush', (params: any) => {
        if (!params.areas.length) return;

        // this is only used for the label and the data saved in the highlight
        // the positioning is done with the true coordinates until the user
        // releases the mouse button (in the `brushend` event)
        const duration = new Duration(timeGranularity);
        const start = duration.round(params.areas[0].coordRange[0], Timezone.UTC);
        const end = duration.round(params.areas[0].coordRange[1], Timezone.UTC);

        const x0 = myChart.convertToPixel({ xAxisIndex: 0 }, params.areas[0].coordRange[0]);
        const x1 = myChart.convertToPixel({ xAxisIndex: 0 }, params.areas[0].coordRange[1]);

        setHighlight({
          title: formatIsoDateRange(start, end, Timezone.UTC),
          x: (x0 + x1) / 2,
          y: 50,
          start,
          end,
          text: (
            <div className="button-bar">
              <Button
                text="Zoom in"
                intent={Intent.PRIMARY}
                small
                onClick={() => {
                  if (!timeColumnName) return;
                  setWhere(
                    updateFilterClause(
                      where,
                      F(
                        'TIME_IN_INTERVAL',
                        C(timeColumnName),
                        `${start.toISOString()}/${end.toISOString()}`,
                      ),
                    ),
                  );
                  setHighlight(undefined);
                  myChart.dispatchAction({
                    type: 'brush',
                    command: 'clear',
                    areas: [],
                  });
                }}
              />
              <Button
                text="Close"
                small
                onClick={() => {
                  setHighlight(undefined);
                  myChart.dispatchAction({
                    type: 'brush',
                    command: 'clear',
                    areas: [],
                  });
                }}
              />
            </div>
          ),
        });
      });
      // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [sourceDataState.data]);

    useEffect(() => {
      const myChart = chartRef.current;
      if (!myChart) return;
      myChart.resize();

      // if there is a highlight, update its x position
      // by calculating new pixel position from the highlight's data
      if (highlight) {
        const { start, end } = highlight;

        const x0 = myChart.convertToPixel({ xAxisIndex: 0 }, start);
        const x1 = myChart.convertToPixel({ xAxisIndex: 0 }, end);

        setHighlight({
          ...highlight,
          x: (x0 + x1) / 2,
        });
      }
    }, [stage]);

    const errorMessage = sourceDataState.getErrorMessage();
    return (
      <div className="multi-axis-chart-module module">
        <div
          className="echart-container"
          ref={container => {
            if (chartRef.current || !container) return;
            containerRef.current = container;
            chartRef.current = setupChart(container);
          }}
        />
        {errorMessage && <Issue issue={errorMessage} />}
        {sourceDataState.loading && (
          <Loader cancelText="Cancel query" onCancel={() => queryManager.cancelCurrent()} />
        )}
        <PortalBubble
          className="module-bubble"
          openOn={highlight}
          offsetElement={containerRef.current}
        />
      </div>
    );
  },
});
