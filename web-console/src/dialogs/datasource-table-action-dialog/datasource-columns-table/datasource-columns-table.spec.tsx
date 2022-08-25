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

import { QueryState } from '../../../utils';

import { DatasourceColumnsTable, DatasourceColumnsTableRow } from './datasource-columns-table';

let columnsState: QueryState<DatasourceColumnsTableRow[]> = QueryState.INIT;
jest.mock('../../../hooks', () => {
  return {
    useQueryManager: () => [columnsState],
  };
});

describe('DatasourceColumnsTable', () => {
  function makeDatasourceColumnsTable() {
    return <DatasourceColumnsTable datasource="test" />;
  }

  it('matches snapshot on init', () => {
    expect(shallow(makeDatasourceColumnsTable())).toMatchSnapshot();
  });

  it('matches snapshot on loading', () => {
    columnsState = QueryState.LOADING;

    expect(shallow(makeDatasourceColumnsTable())).toMatchSnapshot();
  });

  it('matches snapshot on error', () => {
    columnsState = new QueryState({ error: new Error('test error') });

    expect(shallow(makeDatasourceColumnsTable())).toMatchSnapshot();
  });

  it('matches snapshot on no data', () => {
    columnsState = new QueryState({
      data: [],
    });

    expect(shallow(makeDatasourceColumnsTable())).toMatchSnapshot();
  });

  it('matches snapshot on some data', () => {
    columnsState = new QueryState({
      data: [
        {
          COLUMN_NAME: 'channel',
          DATA_TYPE: 'VARCHAR',
        },
        {
          COLUMN_NAME: 'page',
          DATA_TYPE: 'VARCHAR',
        },
      ],
    });

    expect(shallow(makeDatasourceColumnsTable())).toMatchSnapshot();
  });
});
