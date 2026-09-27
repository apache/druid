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
import { TableFilters } from '../../utils/table-filters';

import { ServicesView } from './services-view';

// Snapshot the table props and columns, not every rendered cell
jest.mock(
  'react-table',
  () => jest.requireActual('../../test-utils/stub-component').reactTableStub,
);

jest.useFakeTimers().setSystemTime(Date.parse('2024-06-08T12:34:56Z'));

jest.mock('../../utils', () => {
  const originalUtils = jest.requireActual('../../utils');

  class QueryManagerMock {
    private readonly onStateChange: any;

    constructor(opt: { onStateChange: any }) {
      this.onStateChange = opt.onStateChange;
    }

    public runQuery() {
      this.onStateChange(
        new QueryState({
          data: {
            services: [
              [
                {
                  service: 'localhost:8082',
                  service_type: 'broker',
                  tier: null,
                  host: 'localhost',
                  plaintext_port: 8082,
                  tls_port: -1,
                  curr_size: 0,
                  max_size: 0,
                  is_leader: 0,
                  start_time: 0,
                },
                {
                  service: 'localhost:8083',
                  service_type: 'historical',
                  tier: '_default_tier',
                  host: 'localhost',
                  plaintext_port: 8083,
                  tls_port: -1,
                  curr_size: 179744287,
                  max_size: BigInt(3000000000),
                  is_leader: 0,
                  segmentsToLoad: 0,
                  segmentsToDrop: 0,
                  segmentsToLoadSize: 0,
                  segmentsToDropSize: 0,
                  start_time: 0,
                },
              ],
            ],
            loadQueueInfo: {},
            workerInfo: {},
          },
        }) as any,
      );
    }

    public terminate() {}
  }

  return {
    ...originalUtils,
    QueryManager: QueryManagerMock,
  };
});

describe('ServicesView', () => {
  it('renders data', () => {
    const comp = (
      <ServicesView
        filters={TableFilters.empty()}
        onFiltersChange={() => {}}
        goToQuery={() => {}}
        capabilities={Capabilities.FULL}
      />
    );

    const { container } = render(comp);
    expect(container.firstChild).toMatchSnapshot();
  });
});
