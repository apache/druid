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

import { AxisScale } from 'd3-axis';
import React from 'react';

import { BarUnitData } from '../components/segment-timeline/segment-timeline';

import { BarUnit } from './bar-unit';
import { HoveredBarInfo } from './stacked-bar-chart';

interface BarGroupProps {
  dataToRender: BarUnitData[];
  changeActiveDatasource: (dataSource: string) => void;
  formatTick: (e: number) => string;
  xScale: AxisScale<Date>;
  yScale: AxisScale<number>;
  barWidth: number;
  onHoverBar?: (e: any) => void;
  offHoverBar?: () => void;
  hoverOn?: HoveredBarInfo | null;
}

export class BarGroup extends React.Component<BarGroupProps> {
  shouldComponentUpdate(nextProps: BarGroupProps): boolean {
    return nextProps.hoverOn === this.props.hoverOn;
  }

  render(): JSX.Element[] | null {
    const {
      dataToRender,
      changeActiveDatasource,
      xScale,
      yScale,
      onHoverBar,
      barWidth,
    } = this.props;
    if (dataToRender === undefined) return null;

    return dataToRender.map((entry: BarUnitData, i: number) => {
      const y0 = yScale(entry.y0 || 0) || 0;
      const x = xScale(new Date(entry.x));
      const y = yScale((entry.y0 || 0) + entry.y) || 0;
      const height = y0 - y;
      const barInfo: HoveredBarInfo = {
        xCoordinate: x,
        yCoordinate: y,
        height,
        datasource: entry.datasource,
        xValue: entry.x,
        yValue: entry.y,
      };
      return (
        <BarUnit
          key={i}
          x={x}
          y={y}
          width={barWidth}
          height={height}
          style={{ fill: entry.color }}
          onClick={() => changeActiveDatasource(entry.datasource)}
          onHover={() => onHoverBar && onHoverBar(barInfo)}
        />
      );
    });
  }
}
