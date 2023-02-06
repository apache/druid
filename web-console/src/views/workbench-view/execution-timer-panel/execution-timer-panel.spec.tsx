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

import { Execution } from '../../../druid-models';

import { ExecutionTimerPanel } from './execution-timer-panel';

describe('AnchoredQueryTimer', () => {
  const start = 1619201218452;

  beforeEach(() => {
    let nowCalls = 0;
    jest.spyOn(Date, 'now').mockImplementation(() => {
      return start + 1000 + 2000 * nowCalls++;
    });
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it('matches snapshot', () => {
    const { container } = render(
      <ExecutionTimerPanel
        execution={new Execution({ engine: 'sql-msq-task', id: 'xxx', startTime: new Date(start) })}
        onCancel={() => {}}
      />,
    );
    expect(container.firstChild).toMatchSnapshot();
  });
});
