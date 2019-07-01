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

import { render } from '@testing-library/react';
import React from 'react';

import { TableCell } from './table-cell';

describe('table cell', () => {
  it('matches snapshot null', () => {
    const tableCell = <TableCell value={null} unparseable={false} timestamp={false} />;

    const { container } = render(tableCell);
    expect(container.firstChild).toMatchSnapshot();
  });

  it('matches snapshot null timestamp', () => {
    const tableCell = <TableCell value={null} unparseable={false} timestamp />;

    const { container } = render(tableCell);
    expect(container.firstChild).toMatchSnapshot();
  });

  it('matches snapshot simple', () => {
    const tableCell = <TableCell value="Hello World" unparseable={false} timestamp={false} />;

    const { container } = render(tableCell);
    expect(container.firstChild).toMatchSnapshot();
  });

  it('matches snapshot array short', () => {
    const tableCell = <TableCell value={['a', 'b', 'c']} unparseable={false} timestamp={false} />;

    const { container } = render(tableCell);
    expect(container.firstChild).toMatchSnapshot();
  });

  it('matches snapshot array long', () => {
    const tableCell = (
      <TableCell
        value={Array.from(new Array(100)).map((_, i) => i)}
        unparseable={false}
        timestamp={false}
      />
    );

    const { container } = render(tableCell);
    expect(container.firstChild).toMatchSnapshot();
  });

  it('matches snapshot truncate', () => {
    const longString = new Array(100).join('test');
    const tableCell = <TableCell value={longString} unparseable={false} timestamp={false} />;

    const { container } = render(tableCell);
    expect(container.firstChild).toMatchSnapshot();
  });
});
