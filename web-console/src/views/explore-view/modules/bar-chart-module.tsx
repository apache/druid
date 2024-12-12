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
import { L } from 'druid-query-toolkit';
import type { ECharts } from 'echarts';
import * as echarts from 'echarts';
import { useEffect, useMemo, useRef } from 'react';

import { Loader } from '../../../components';
import { useQueryManager } from '../../../hooks';
import { formatEmpty } from '../../../utils';
import { Issue } from '../components';
import { highlightStore } from '../highlight-store/highlight-store';
import type { ExpressionMeta } from '../models';
import { ModuleRepository } from '../module-repository/module-repository';

import './record-table-module.scss';

const OVERALL_LABEL = 'Overall';

interface BarChartParameterValues {
  splitColumn: ExpressionMeta;
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
    },
    measure: {
      type: 'measure',
      label: 'Measure to show',
      transferGroup: 'show-agg',
      defaultValue: querySource => querySource.getFirstAggregateMeasure(),
      required: true,
    },
    measureToSort: {
      type: 'measure',
      label: 'Measure to sort (default to shown)',
    },
    limit: {
      type: 'number',
      label: 'Max bars to show',
      defaultValue: 5,
      required: true,
    },
  },
  component: function BarChartModule(props) {
    const { querySource, where, setWhere, parameterValues, stage, runSqlQuery } = props;
    const chartRef = useRef<ECharts>();

    const { splitColumn, measure, measureToSort, limit } = parameterValues;

    const dataQuery = useMemo(() => {
      const splitExpression = splitColumn ? splitColumn.expression : L(OVERALL_LABEL);

      return querySource
        .getInitQuery(where)
        .addSelect(splitExpression.as('dim'), { addToGroupBy: 'end' })
        .addSelect(measure.expression.as('met'), {
          addToOrderBy: measureToSort ? undefined : 'end',
          direction: 'DESC',
        })
        .applyIf(measureToSort, q =>
          q.addOrderBy(measureToSort.expression.toOrderByExpression('DESC')),
        )
        .changeLimitValue(limit);
    }, [querySource, where, splitColumn, measure, measureToSort, limit]);

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

        highlightStore.getState().setHighlight({
          label: formatEmpty(label),
          x,
          y: y - 20,
          data: [dim, met],
          onDrop: () => {
            highlightStore.getState().dropHighlight();
          },
          onSave:
            label !== OVERALL_LABEL
              ? () => {
                  if (splitColumn) {
                    setWhere(where.toggleClauseInWhere(splitColumn.expression.equal(label)));
                  }
                  highlightStore.getState().dropHighlight();
                }
              : undefined,
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
      const highlight = highlightStore.getState().highlight;
      if (highlight) {
        const [x, y] = myChart.convertToPixel({ seriesIndex: 0 }, highlight.data as number[]);

        highlightStore.getState().updateHighlight({
          x,
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
