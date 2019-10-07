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

import { QueryExtraInfo } from './query-extra-info';

describe('query extra info', () => {
  it('matches snapshot', () => {
    const queryExtraInfo = (
      <QueryExtraInfo
        queryExtraInfo={{
          queryId: 'e3ee781b-c0b6-4385-9d99-a8a1994bebac',
          startTime: new Date('1986-04-26T01:23:40+03:00'),
          endTime: new Date('1986-04-26T01:23:48+03:00'),
          numResults: 1000,
          wrapQueryLimit: 1000,
        }}
        onDownload={() => {}}
      />
    );

    const { container } = render(queryExtraInfo);
    expect(container.firstChild).toMatchSnapshot();
  });
});
