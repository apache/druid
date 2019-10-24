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

import * as d3 from 'd3';
import { AxisScale } from 'd3';
import React, { useState } from 'react';

import { BarChartMargin, BarUnitData } from '../components/segment-timeline/segment-timeline';

import { BarGroup } from './bar-group';
import { ChartAxis } from './chart-axis';

import './stacked-bar-chart.scss';

interface StackedBarChartProps {
  svgWidth: number;
  svgHeight: number;
  margin: BarChartMargin;
  activeDataType?: string;
  dataToRender: BarUnitData[];
  changeActiveDatasource: (e: string) => void;
  formatTick: (e: number) => string;
  xScale: AxisScale<Date>;
  yScale: AxisScale<number>;
  barWidth: number;
}

export interface HoveredBarInfo {
  xCoordinate?: number;
  yCoordinate?: number;
  height?: number;
  width?: number;
  datasource?: string;
  xValue?: number;
  yValue?: number;
}

export function StackedBarChart(props: StackedBarChartProps) {
  const {
    activeDataType,
    svgWidth,
    svgHeight,
    formatTick,
    xScale,
    yScale,
    dataToRender,
    changeActiveDatasource,
    barWidth,
  } = props;
  const [hoverOn, setHoverOn] = useState();

  const width = props.svgWidth - props.margin.left - props.margin.right;
  const height = props.svgHeight - props.margin.bottom - props.margin.top;

  function renderBarChart() {
    return (
      <div className={'bar-chart-container'}>
        <svg
          width={width}
          height={height}
          viewBox={`0 0 ${svgWidth} ${svgHeight}`}
          preserveAspectRatio={'xMinYMin meet'}
          style={{ marginTop: '20px' }}
        >
          <ChartAxis
            className={'gridline-x'}
            transform={'translate(60, 0)'}
            scale={d3
              .axisLeft(yScale)
              .ticks(5)
              .tickSize(-width)
              .tickFormat(() => '')
              .tickSizeOuter(0)}
          />
          <ChartAxis
            className={'axis--x'}
            transform={`translate(65, ${height})`}
            scale={d3.axisBottom(xScale)}
          />
          <ChartAxis
            className={'axis--y'}
            transform={'translate(60, 0)'}
            scale={d3
              .axisLeft(yScale)
              .ticks(5)
              .tickFormat((e: number) => formatTick(e))}
          />
          <g className="bars-group" onMouseLeave={() => setHoverOn(undefined)}>
            <BarGroup
              dataToRender={dataToRender}
              changeActiveDatasource={changeActiveDatasource}
              formatTick={formatTick}
              xScale={xScale}
              yScale={yScale}
              onHoverBar={(e: HoveredBarInfo) => setHoverOn(e)}
              hoverOn={hoverOn}
              barWidth={barWidth}
            />
            {hoverOn && (
              <g
                className={'hovered-bar'}
                onClick={() => {
                  setHoverOn(undefined);
                  changeActiveDatasource(hoverOn.datasource as string);
                }}
              >
                <rect
                  x={hoverOn.xCoordinate}
                  y={hoverOn.yCoordinate}
                  width={barWidth}
                  height={hoverOn.height}
                />
              </g>
            )}
          </g>
        </svg>
      </div>
    );
  }

  return (
    <div className={'bar-chart'}>
      <div className={'bar-chart-tooltip'}>
        <div>Datasource: {hoverOn ? hoverOn.datasource : ''}</div>
        <div>Time: {hoverOn ? hoverOn.xValue : ''}</div>
        <div>
          {`${activeDataType === 'countData' ? 'Count:' : 'Size:'} ${
            hoverOn ? formatTick(hoverOn.yValue as number) : ''
          }`}
        </div>
      </div>
      {renderBarChart()}
    </div>
  );
}
