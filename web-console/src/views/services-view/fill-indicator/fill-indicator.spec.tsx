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

import { FillIndicator } from './fill-indicator';

describe('FillIndicator', () => {
  it('matches snapshot with equal values (normal capacity)', () => {
    const fillIndicator = <FillIndicator barValue={0.75} labelValue={0.75} />;
    const { container } = render(fillIndicator);
    expect(container.firstChild).toMatchSnapshot();
  });

  it('matches snapshot with different values (over-capacity)', () => {
    const fillIndicator = <FillIndicator barValue={0.6} labelValue={1.2} />;
    const { container } = render(fillIndicator);
    expect(container.firstChild).toMatchSnapshot();
  });

  it('matches snapshot with very small value showing ~0.0%', () => {
    const fillIndicator = <FillIndicator barValue={0.0001} labelValue={0.0001} />;
    const { container } = render(fillIndicator);
    expect(container.firstChild).toMatchSnapshot();
  });

  it('matches snapshot at full capacity', () => {
    const fillIndicator = <FillIndicator barValue={1} labelValue={1} />;
    const { container } = render(fillIndicator);
    expect(container.firstChild).toMatchSnapshot();
  });

  it('matches snapshot at zero capacity', () => {
    const fillIndicator = <FillIndicator barValue={0} labelValue={0} />;
    const { container } = render(fillIndicator);
    expect(container.firstChild).toMatchSnapshot();
  });
});
