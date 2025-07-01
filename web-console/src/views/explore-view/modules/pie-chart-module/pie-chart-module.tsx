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
import { C, F, L } from 'druid-query-toolkit';
import type { ECharts } from 'echarts';
import * as echarts from 'echarts';
import { useEffect, useMemo, useRef, useState } from 'react';

import { Loader, PortalBubble, type PortalBubbleOpenOn } from '../../../../components';
import { useQueryManager } from '../../../../hooks';
import { ColorAssigner } from '../../../../singletons';
import { formatEmpty, formatNumber } from '../../../../utils';
import { Issue } from '../../components';
import type { ExpressionMeta } from '../../models';
import { ModuleRepository } from '../../module-repository/module-repository';
import { updateFilterClause } from '../../utils';

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

interface PieChartHighlight extends PortalBubbleOpenOn {
  name: string;
  dataIndex: number;
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
      important: true,
    },
    measure: {
      type: 'measure',
      transferGroup: 'show',
      defaultValue: ({ querySource }) => querySource?.getFirstAggregateMeasure(),
      required: true,
      important: true,
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
    const { querySource, where, setWhere, moduleWhere, parameterValues, stage, runSqlQuery } =
      props;
    const containerRef = useRef<HTMLDivElement>();
    const chartRef = useRef<ECharts>();
    const [highlight, setHighlight] = useState<PieChartHighlight | undefined>();

    const { splitColumn, measure, limit, showOthers } = parameterValues;

    const dataQueries = useMemo(() => {
      const splitExpression = splitColumn ? splitColumn.expression : L(OVERALL_LABEL);
      const effectiveWhere = where.and(moduleWhere);

      return {
        mainQuery: querySource
          .getInitQuery(effectiveWhere)
          .addSelect(F.cast(splitExpression, 'VARCHAR').as('name'), { addToGroupBy: 'end' })
          .addSelect(measure.expression.as('value'), {
            addToOrderBy: 'end',
            direction: 'DESC',
          })
          .changeLimitValue(limit + (showOthers ? 1 : 0)),
        limit,
        splitExpression: splitColumn?.expression,
        othersPartialQuery: showOthers
          ? querySource.getInitQuery(effectiveWhere).addSelect(measure.expression.as('value'))
          : undefined,
      };
    }, [querySource, where, moduleWhere, splitColumn, measure, limit, showOthers]);

    const [sourceDataState, queryManager] = useQueryManager({
      query: dataQueries,
      processQuery: async (
        { mainQuery, limit, splitExpression, othersPartialQuery },
        cancelToken,
      ) => {
        const result = await runSqlQuery({ query: mainQuery }, cancelToken);
        const data = result.toObjectArray();

        if (splitExpression && othersPartialQuery) {
          const pieValues = result.getColumnByIndex(0)!;

          if (pieValues.length > limit) {
            const othersResult = await runSqlQuery({
              query: othersPartialQuery.addWhere(splitExpression.notIn(pieValues.slice(0, limit))),
            });
            data.push({ name: 'Others', value: othersResult.rows[0][0], __isOthers: true });
          }
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

      const dataWithColors = data.map((item: any) => ({
        ...item,
        itemStyle: {
          color: splitColumn ? ColorAssigner.getColorForDimensionValue(splitColumn.name, item.name) : '#1890ff',
        },
      }));

      myChart.setOption({
        series: [
          {
            id: 'hello',
            data: dataWithColors,
          },
        ],
      });

      myChart.on('click', 'series', p => {
        if (highlight?.name === p.name) {
          setHighlight(undefined);
          return;
        }

        const centroid = getCentroid(myChart, p.dataIndex);
        if (!centroid) return;

        const { name, value, __isOthers } = p.data as any;

        setHighlight({
          title: formatEmpty(name),
          x: centroid.x,
          y: centroid.y - 20,
          name,
          dataIndex: p.dataIndex,
          text: (
            <>
              {formatNumber(value)}
              <div className="button-bar">
                {!__isOthers && (
                  <Button
                    text="Zoom in"
                    intent={Intent.PRIMARY}
                    small
                    onClick={() => {
                      setWhere(updateFilterClause(where, C(splitColumn.name).equal(name)));
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
            </>
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
        const { dataIndex } = highlight;

        const centroid = getCentroid(myChart, dataIndex);

        if (!centroid) return;

        setHighlight({
          ...highlight,
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
