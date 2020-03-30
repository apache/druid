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

import React from 'react';

import './bar-unit.scss';

interface BarChartUnitProps {
  x: number | undefined;
  y: number;
  width: number;
  height: number;
  style?: any;
  onClick?: () => void;
  onHover?: () => void;
  offHover?: () => void;
}

export function BarUnit(props: BarChartUnitProps) {
  const { x, y, width, height, style, onClick, onHover, offHover } = props;
  return (
    <rect
      className={`bar-chart-unit`}
      x={x}
      y={y}
      width={width}
      height={height}
      style={style}
      onClick={onClick}
      onMouseOver={onHover}
      onMouseLeave={offHover}
    />
  );
}
