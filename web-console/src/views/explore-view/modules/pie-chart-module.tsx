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
import { C, L } from 'druid-query-toolkit';
import type { ECharts } from 'echarts';
import * as echarts from 'echarts';
import { useEffect, useMemo, useRef } from 'react';

import { Loader } from '../../../components';
import { useQueryManager } from '../../../hooks';
import { formatEmpty, formatNumber } from '../../../utils';
import { Issue } from '../components';
import { highlightStore } from '../highlight-store/highlight-store';
import type { ExpressionMeta } from '../models';
import { ModuleRepository } from '../module-repository/module-repository';

import './record-table-module.scss';

const OVERALL_LABEL = 'Overall';

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

interface PieChartParameterValues {
  splitColumn: ExpressionMeta;
  measure: ExpressionMeta;
  limit: number;
  showOthers: boolean;
}

ModuleRepository.registerModule<PieChartParameterValues>({
  id: 'pie-chart',
  title: 'Pie chart',
  icon: IconNames.PIE_CHART,
  parameters: {
    splitColumn: {
      type: 'expression',
      label: 'Slice column',
      transferGroup: 'show',
      required: true,
    },
    measure: {
      type: 'measure',
      transferGroup: 'show',
      defaultValue: querySource => querySource.getFirstAggregateMeasure(),
      required: true,
    },
    limit: {
      type: 'number',
      label: 'Max slices to show',
      defaultValue: 5,
      required: true,
    },
    showOthers: {
      type: 'boolean',
      defaultValue: true,
      label: 'Show others',
    },
  },
  component: function PieChartModule(props) {
    const { querySource, where, setWhere, parameterValues, stage, runSqlQuery } = props;
    const chartRef = useRef<ECharts>();

    const { splitColumn, measure, limit, showOthers } = parameterValues;

    const dataQueries = useMemo(() => {
      const splitExpression = splitColumn ? splitColumn.expression : L(OVERALL_LABEL);

      return {
        mainQuery: querySource
          .getInitQuery(where)
          .addSelect(splitExpression.as('name'), { addToGroupBy: 'end' })
          .addSelect(measure.expression.as('value'), {
            addToOrderBy: 'end',
            direction: 'DESC',
          })
          .changeLimitValue(limit),
        splitExpression: splitColumn?.expression,
        othersPartialQuery: showOthers
          ? querySource.getInitQuery(where).addSelect(measure.expression.as('value'))
          : undefined,
      };
    }, [querySource, where, splitColumn, measure, limit, showOthers]);

    const [sourceDataState, queryManager] = useQueryManager({
      query: dataQueries,
      processQuery: async ({ mainQuery, splitExpression, othersPartialQuery }, cancelToken) => {
        const result = await runSqlQuery(mainQuery, cancelToken);
        const data = result.toObjectArray();

        if (splitExpression && othersPartialQuery) {
          const othersResult = await runSqlQuery(
            othersPartialQuery.addWhere(splitExpression.notIn(result.getColumnByIndex(0)!)),
          );
          data.push({ name: 'Others', value: othersResult.rows[0][0], __isOthers: true });
        }

        return data;
      },
    });

    function setupChart(container: HTMLDivElement) {
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
            label: {
              formatter: (params: any) => formatEmpty(params.name),
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
          label: formatEmpty(name) + ': ' + formatNumber(value),
          x: centroid.x,
          y: centroid.y - 20,
          data: { name, value, dataIndex: p.dataIndex },
          onDrop: () => {
            highlightStore.getState().dropHighlight();
          },
          onSave: __isOthers
            ? undefined
            : () => {
                setWhere(where.toggleClauseInWhere(C(splitColumn.name).equal(name)));
                highlightStore.getState().dropHighlight();
              },
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
        const { dataIndex } = highlight.data;

        const centroid = getCentroid(myChart, dataIndex);

        if (!centroid) return;

        highlightStore.getState().updateHighlight({
          x: centroid.x,
          y: centroid.y - 20,
        });
      }
    }, [stage]);

    const errorMessage = sourceDataState.getErrorMessage();
    return (
      <div className="pie-chart-module module">
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
