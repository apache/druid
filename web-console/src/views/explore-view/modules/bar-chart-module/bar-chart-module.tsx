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
import { F, L } from 'druid-query-toolkit';
import type { ECharts } from 'echarts';
import * as echarts from 'echarts';
import { useEffect, useMemo, useRef, useState } from 'react';

import { Loader, PortalBubble, type PortalBubbleOpenOn } from '../../../../components';
import { useQueryManager } from '../../../../hooks';
import { formatEmpty } from '../../../../utils';
import { Issue } from '../../components';
import type { ExpressionMeta } from '../../models';
import { ModuleRepository } from '../../module-repository/module-repository';
import { updateFilterClause } from '../../utils';

const OVERALL_LABEL = 'Overall';

interface BarChartHighlight extends PortalBubbleOpenOn {
  dim: number;
  met: number;
}

interface BarChartParameterValues {
  splitColumn: ExpressionMeta;
  timeBucket: string;
  measure: ExpressionMeta;
  measureToSort: ExpressionMeta;
  limit: number;
}

ModuleRepository.registerModule<BarChartParameterValues>({
  id: 'bar-chart',
  title: 'Bar chart',
  icon: IconNames.VERTICAL_BAR_CHART_DESC,
  parameters: {
    splitColumn: {
      type: 'expression',
      label: 'Bar column',
      transferGroup: 'show',
      required: true,
      important: true,
    },
    timeBucket: {
      type: 'option',
      label: 'Time bucket',
      options: ['PT1M', 'PT5M', 'PT1H', 'P1D', 'P1M'],
      optionLabels: {
        PT1M: '1 minute',
        PT5M: '5 minutes',
        PT1H: '1 hour',
        P1D: '1 day',
        P1M: '1 month',
      },
      defaultValue: 'PT1H',
      important: true,
      defined: ({ parameterValues, querySource }) =>
        parameterValues.splitColumn?.evaluateSqlType(querySource?.columns) === 'TIMESTAMP',
    },

    measure: {
      type: 'measure',
      label: 'Measure to show',
      transferGroup: 'show-agg',
      defaultValue: ({ querySource }) => querySource?.getFirstAggregateMeasure(),
      required: true,
      important: true,
    },
    measureToSort: {
      type: 'measure',
      label: 'Measure to sort',
      description: 'Default to shown measure',
    },
    limit: {
      type: 'number',
      label: 'Max bars to show',
      defaultValue: 5,
      required: true,
    },
  },
  component: function BarChartModule(props) {
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
    const [highlight, setHighlight] = useState<BarChartHighlight | undefined>();

    const { splitColumn, timeBucket, measure, measureToSort, limit } = parameterValues;

    const dataQuery = useMemo(() => {
      const splitExpression = splitColumn ? splitColumn.expression : L(OVERALL_LABEL);

      return {
        query: querySource
          .getInitQuery(where.and(moduleWhere))
          .addSelect(
            splitExpression.applyIf(timeBucket, ex => F.timeFloor(ex, timeBucket)).as('dim'),
            {
              addToGroupBy: 'end',
              addToOrderBy: !measureToSort && timeBucket ? 'end' : undefined,
              direction: 'ASC',
            },
          )
          .addSelect(measure.expression.as('met'), {
            addToOrderBy: !measureToSort && !timeBucket ? 'end' : undefined,
            direction: 'DESC',
          })
          .applyIf(measureToSort, q =>
            q.addOrderBy(measureToSort.expression.toOrderByExpression('DESC')),
          )
          .changeLimitValue(limit),
        timezone,
      };
    }, [
      querySource,
      timezone,
      where,
      moduleWhere,
      splitColumn,
      timeBucket,
      measure,
      measureToSort,
      limit,
    ]);

    const [sourceDataState, queryManager] = useQueryManager({
      query: dataQuery,
      processQuery: async (query, cancelToken) => {
        return (await runSqlQuery(query, cancelToken)).toObjectArray();
      },
    });

    function setupChart(container: HTMLDivElement) {
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

      myChart.off('click');

      myChart.setOption({
        dataset: {
          source: data,
        },
      });

      myChart.on('click', 'series', p => {
        const label = p.name;
        const { dim, met } = p.data as any;

        const [x, y] = myChart.convertToPixel({ seriesIndex: 0 }, [dim, met]);

        setHighlight({
          title: formatEmpty(label),
          x: x,
          y: y - 20,
          dim,
          met,
          text: (
            <div className="button-bar">
              {label !== OVERALL_LABEL && (
                <Button
                  text="Zoom in"
                  intent={Intent.PRIMARY}
                  small
                  onClick={() => {
                    if (splitColumn) {
                      setWhere(updateFilterClause(where, splitColumn.expression.equal(label)));
                    }
                    setHighlight(undefined);
                  }}
                />
              )}
              <Button
                text="Close"
                small
                onClick={() => {
                  setHighlight(undefined);
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
        const [x, y] = myChart.convertToPixel({ seriesIndex: 0 }, [highlight.dim, highlight.met]);

        setHighlight({
          ...highlight,
          x: x,
          y: y - 20,
        });
      }
    }, [stage]);

    const errorMessage = sourceDataState.getErrorMessage();
    return (
      <div className="bar-chart-module module">
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
