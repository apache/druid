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

import { axisBottom, axisLeft, AxisScale } from 'd3-axis';
import React, { useState } from 'react';

import { BarGroup } from './bar-group';
import { ChartAxis } from './chart-axis';

import './stacked-bar-chart.scss';

export interface BarUnitData {
  x: number;
  y: number;
  y0?: number;
  width: number;
  datasource: string;
  color: string;
  dailySize?: number;
}

export interface BarChartMargin {
  top: number;
  right: number;
  bottom: number;
  left: number;
}

export interface HoveredBarInfo {
  xCoordinate?: number;
  yCoordinate?: number;
  height?: number;
  width?: number;
  datasource?: string;
  xValue?: number;
  yValue?: number;
  dailySize?: number;
}

interface StackedBarChartProps {
  svgWidth: number;
  svgHeight: number;
  margin: BarChartMargin;
  activeDataType?: string;
  dataToRender: BarUnitData[];
  changeActiveDatasource: (e: string | null) => void;
  formatTick: (e: number) => string;
  xScale: AxisScale<Date>;
  yScale: AxisScale<number>;
  barWidth: number;
}

export const StackedBarChart = React.memo(function StackedBarChart(props: StackedBarChartProps) {
  const {
    activeDataType,
    svgWidth,
    svgHeight,
    margin,
    formatTick,
    xScale,
    yScale,
    dataToRender,
    changeActiveDatasource,
    barWidth,
  } = props;
  const [hoverOn, setHoverOn] = useState<HoveredBarInfo>();

  const width = svgWidth - margin.left - margin.right;
  const height = svgHeight - margin.top - margin.bottom;

  function renderBarChart() {
    return (
      <svg
        width={svgWidth}
        height={svgHeight}
        viewBox={`0 0 ${svgWidth} ${svgHeight}`}
        preserveAspectRatio="xMinYMin meet"
      >
        <g
          transform={`translate(${margin.left}, ${margin.top})`}
          onMouseLeave={() => setHoverOn(undefined)}
        >
          <ChartAxis
            className="gridline-x"
            transform="translate(0, 0)"
            scale={axisLeft(yScale)
              .ticks(5)
              .tickSize(-width)
              .tickFormat(() => '')
              .tickSizeOuter(0)}
          />
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
          <ChartAxis
            className="axis-x"
            transform={`translate(0, ${height})`}
            scale={axisBottom(xScale)}
          />
          <ChartAxis
            className="axis-y"
            scale={axisLeft(yScale)
              .ticks(5)
              .tickFormat((e: number) => formatTick(e))}
          />
          {hoverOn && (
            <g
              className="hovered-bar"
              onClick={() => {
                setHoverOn(undefined);
                changeActiveDatasource(hoverOn.datasource ?? null);
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
    );
  }

  return (
    <div className="stacked-bar-chart">
      {hoverOn && (
        <>
          <div className="bar-chart-tooltip">
            <div>Datasource: {hoverOn.datasource}</div>
            <div>Time: {hoverOn.xValue}</div>
            <div>
              {`${
                activeDataType === 'countData' ? 'Daily total count:' : 'Daily total size:'
              } ${formatTick(hoverOn.dailySize!)}`}
            </div>
            <div>
              {`${activeDataType === 'countData' ? 'Count:' : 'Size:'} ${formatTick(
                hoverOn.yValue!,
              )}`}
            </div>
          </div>
        </>
      )}
      {renderBarChart()}
    </div>
  );
});
