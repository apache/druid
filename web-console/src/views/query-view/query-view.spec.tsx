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

import { QueryView } from './query-view';

describe('QueryView', () => {
  it('matches snapshot', () => {
    const sqlView = shallow(<QueryView initQuery="test" />);
    expect(sqlView).toMatchSnapshot();
  });

  it('matches snapshot with query', () => {
    const sqlView = shallow(<QueryView initQuery="SELECT +3" />);
    expect(sqlView).toMatchSnapshot();
  });

  it('.formatStr', () => {
    expect(QueryView.formatStr(null, 'csv')).toEqual('"null"');
    expect(QueryView.formatStr('hello\nworld', 'csv')).toEqual('"hello world"');
    expect(QueryView.formatStr(123, 'csv')).toEqual('"123"');
    expect(QueryView.formatStr(new Date('2021-01-02T03:04:05.678Z'), 'csv')).toEqual(
      '"2021-01-02T03:04:05.678Z"',
    );
  });
});
