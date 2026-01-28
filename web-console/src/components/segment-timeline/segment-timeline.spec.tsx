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

import { Capabilities } from '../../helpers';
import { QueryState } from '../../utils';

import { SegmentTimeline } from './segment-timeline';

jest.useFakeTimers('modern').setSystemTime(Date.parse('2024-11-19T12:34:56Z'));

jest.mock('../../hooks', () => {
  return {
    useQueryManager: (options: any) => {
      if (options.initQuery instanceof Capabilities) {
        // This is a query for data sources
        return [new QueryState({ data: ['ds1', 'ds2'] })];
      }

      if (options.query === null) {
        // This is a query for the data source time range
        return [
          new QueryState({
            data: [new Date('2024-11-01 00:00:00Z'), new Date('2024-11-18 00:00:00Z')],
          }),
        ];
      }

      return new QueryState({ error: new Error('not covered') });
    },
  };
});

describe('SegmentTimeline', () => {
  it('matches snapshot', () => {
    const segmentTimeline = (
      <SegmentTimeline capabilities={Capabilities.FULL} datasource={undefined} />
    );
    const { container } = render(segmentTimeline);
    expect(container.firstChild).toMatchSnapshot();
  });
});
