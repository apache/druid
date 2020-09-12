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
import { mount } from 'enzyme';
import React from 'react';

import { QueryManager } from '../../utils';
import { Capabilities } from '../../utils/capabilities';

import { SegmentTimeline } from './segment-timeline';

describe('Segment Timeline', () => {
  it('matches snapshot', () => {
    const segmentTimeline = (
      <SegmentTimeline capabilities={Capabilities.FULL} chartHeight={100} chartWidth={100} />
    );
    const { container } = render(segmentTimeline);
    expect(container.firstChild).toMatchSnapshot();
  });

  it('queries 3 months of data by default', () => {
    const dataQueryManager = new MockDataQueryManager();
    const segmentTimeline = (
      <SegmentTimeline
        capabilities={Capabilities.FULL}
        chartHeight={100}
        chartWidth={100}
        dataQueryManager={dataQueryManager}
      />
    );
    render(segmentTimeline);

    // Ideally, the test should verify the rendered bar graph to see if the bars
    // cover the selected period. Since the unit test does not have a druid
    // instance to query from, just verify the query has the correct time span.
    expect(dataQueryManager.queryTimeSpan).toBe(3);
  });

  it('queries matching time span when new period is selected from dropdown', () => {
    const dataQueryManager = new MockDataQueryManager();
    const segmentTimeline = (
      <SegmentTimeline
        capabilities={Capabilities.FULL}
        chartHeight={100}
        chartWidth={100}
        dataQueryManager={dataQueryManager}
      />
    );
    const wrapper = mount(segmentTimeline);
    const selects = wrapper.find('select');
    expect(selects.length).toBe(2); // Datasource & Period
    const periodSelect = selects.at(1);
    const newTimeSpanMonths = 6;
    periodSelect.simulate('change', { target: { value: newTimeSpanMonths } });

    // Ideally, the test should verify the rendered bar graph to see if the bars
    // cover the selected period. Since the unit test does not have a druid
    // instance to query from, just verify the query has the correct time span.
    expect(dataQueryManager.queryTimeSpan).toBe(newTimeSpanMonths);
  });
});

/**
 * Mock the data query manager, since the unit test does not have a druid instance
 */
class MockDataQueryManager extends QueryManager<
  { capabilities: Capabilities; timeSpan: number },
  any
> {
  queryTimeSpan?: number;

  constructor() {
    super({
      processQuery: async ({ timeSpan }) => {
        this.queryTimeSpan = timeSpan;
      },
      debounceIdle: 0,
      debounceLoading: 0,
    });
  }
}
