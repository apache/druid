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
import * as React from 'react';

interface ChartAxisProps extends React.Props<any> {
  transform: string;
  scale: any;
  className?: string;
}

interface ChartAxisState {}

export class ChartAxis extends React.Component<ChartAxisProps, ChartAxisState> {
  constructor(props: ChartAxisProps) {
    super(props);
    this.state = {};
  }

  render() {
    const { transform, scale, className } = this.props;
    return (
      <g
        className={`axis ${className}`}
        transform={transform}
        ref={node => d3.select(node).call(scale)}
      />
    );
  }
}
