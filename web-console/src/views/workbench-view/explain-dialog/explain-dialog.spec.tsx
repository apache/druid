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

import { shallow } from 'enzyme';
import React from 'react';

import { QueryExplanation, QueryState } from '../../../utils';

import { ExplainDialog } from './explain-dialog';

let explainState: QueryState<QueryExplanation[] | string> = QueryState.INIT;

jest.mock('../../../hooks', () => {
  return {
    useQueryManager: () => [explainState],
  };
});

describe('ExplainDialog', () => {
  function makeExplainDialog() {
    return (
      <ExplainDialog
        onOpenQuery={() => {}}
        queryWithContext={{ engine: 'sql-native', queryString: 'test', queryContext: {} }}
        onClose={() => {}}
        openQueryLabel="Open query"
      />
    );
  }

  it('matches snapshot on init', () => {
    expect(shallow(makeExplainDialog())).toMatchSnapshot();
  });

  it('matches snapshot on loading', () => {
    explainState = QueryState.LOADING;

    expect(shallow(makeExplainDialog())).toMatchSnapshot();
  });

  it('matches snapshot on error', () => {
    explainState = new QueryState({ error: new Error('test error') });

    expect(shallow(makeExplainDialog())).toMatchSnapshot();
  });

  it('matches snapshot on some data (one query)', () => {
    explainState = new QueryState({
      data: [
        {
          query: {
            queryType: 'topN',
            dataSource: {
              type: 'join',
              left: {
                type: 'table',
                name: 'wikipedia',
              },
              right: {
                type: 'query',
                query: {
                  queryType: 'groupBy',
                  dataSource: {
                    type: 'table',
                    name: 'wikipedia',
                  },
                  intervals: {
                    type: 'intervals',
                    intervals: ['-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z'],
                  },
                  virtualColumns: [],
                  filter: {
                    type: 'selector',
                    dimension: 'channel',
                    value: '#en.wikipedia',
                    extractionFn: null,
                  },
                  granularity: {
                    type: 'all',
                  },
                  dimensions: [
                    {
                      type: 'default',
                      dimension: 'channel',
                      outputName: 'd0',
                      outputType: 'STRING',
                    },
                  ],
                  aggregations: [],
                  postAggregations: [],
                  having: null,
                  limitSpec: {
                    type: 'NoopLimitSpec',
                  },
                  context: {},
                  descending: false,
                },
              },
              rightPrefix: 'j0.',
              condition: '("channel" == "j0.d0")',
              joinType: 'LEFT',
              leftFilter: null,
            },
            virtualColumns: [],
            dimension: {
              type: 'default',
              dimension: 'channel',
              outputName: 'd0',
              outputType: 'STRING',
            },
            metric: {
              type: 'dimension',
              previousStop: null,
              ordering: {
                type: 'lexicographic',
              },
            },
            threshold: 101,
            intervals: {
              type: 'intervals',
              intervals: ['-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z'],
            },
            filter: null,
            granularity: {
              type: 'all',
            },
            aggregations: [
              {
                type: 'count',
                name: 'a0',
              },
            ],
            postAggregations: [],
            context: {},
            descending: false,
          },
          signature: [
            {
              name: 'd0',
              type: 'STRING',
            },
            {
              name: 'a0',
              type: 'LONG',
            },
          ],
        },
      ],
    });

    expect(shallow(makeExplainDialog())).toMatchSnapshot();
  });

  it('matches snapshot on some data (many queries)', () => {
    explainState = new QueryState({
      data: [
        {
          query: {
            queryType: 'scan',
            dataSource: {
              type: 'table',
              name: 'wikipedia',
            },
            intervals: {
              type: 'intervals',
              intervals: ['-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z'],
            },
            virtualColumns: [],
            resultFormat: 'compactedList',
            batchSize: 20480,
            limit: 101,
            filter: null,
            columns: ['channel'],
            legacy: false,
            context: {},
            descending: false,
            granularity: {
              type: 'all',
            },
          },
          signature: [
            {
              name: 'channel',
              type: 'STRING',
            },
          ],
        },
        {
          query: {
            queryType: 'scan',
            dataSource: {
              type: 'table',
              name: 'wikipedia',
            },
            intervals: {
              type: 'intervals',
              intervals: ['-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z'],
            },
            virtualColumns: [],
            resultFormat: 'compactedList',
            batchSize: 20480,
            limit: 101,
            filter: {
              type: 'selector',
              dimension: 'channel',
              value: '#en.wikipedia',
              extractionFn: null,
            },
            columns: ['channel'],
            legacy: false,
            context: {},
            descending: false,
            granularity: {
              type: 'all',
            },
          },
          signature: [
            {
              name: 'channel',
              type: 'STRING',
            },
          ],
        },
      ],
    });

    expect(shallow(makeExplainDialog())).toMatchSnapshot();
  });
});
