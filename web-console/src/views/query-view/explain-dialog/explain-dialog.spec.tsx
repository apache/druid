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

import {
  BasicQueryExplanation,
  parseQueryPlan,
  QueryState,
  SemiJoinQueryExplanation,
} from '../../../utils';

import { ExplainDialog } from './explain-dialog';

let explainState: QueryState<BasicQueryExplanation | SemiJoinQueryExplanation | string> =
  QueryState.INIT;

jest.mock('../../../hooks', () => {
  return {
    useQueryManager: () => [explainState],
  };
});

describe('ExplainDialog', () => {
  function makeExplainDialog() {
    return (
      <ExplainDialog
        setQueryString={() => {}}
        queryWithContext={{ queryString: 'test', queryContext: {}, wrapQueryLimit: undefined }}
        onClose={() => {}}
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

  it('matches snapshot on some data', () => {
    explainState = new QueryState({
      data: parseQueryPlan(
        `DruidQueryRel(query=[{"queryType":"topN","dataSource":{"type":"table","name":"kttm-multi-day"},"virtualColumns":[],"dimension":{"type":"default","dimension":"browser","outputName":"d0","outputType":"STRING"},"metric":{"type":"numeric","metric":"a0"},"threshold":101,"intervals":{"type":"intervals","intervals":["-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z"]},"filter":null,"granularity":{"type":"all"},"aggregations":[{"type":"count","name":"a0"}],"postAggregations":[],"context":{"sqlOuterLimit":101,"sqlQueryId":"5905fe8d-9a91-41e0-8f3a-d7a8ac21dce6"},"descending":false}], signature=[{d0:STRING, a0:LONG}])`,
      ),
    });

    expect(shallow(makeExplainDialog())).toMatchSnapshot();
  });
});
