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
import { parseSqlQuery } from 'druid-query-toolkit';
import React from 'react';

import { StringMenuItems } from './string-menu-items';

describe('string menu', () => {
  it('matches snapshot when menu is opened for column not inside group by', () => {
    const stringMenu = (
      <StringMenuItems
        table={'table'}
        schema={'schema'}
        columnName={'cityName'}
        parsedQuery={parseSqlQuery(`SELECT channel, count(*) as cnt FROM wikipedia GROUP BY 1`)}
        onQueryChange={() => {}}
      />
    );

    const { container } = render(stringMenu);
    expect(container).toMatchSnapshot();
  });

  it('matches snapshot when menu is opened for column inside group by', () => {
    const stringMenu = (
      <StringMenuItems
        table={'table'}
        schema={'schema'}
        columnName={'channel'}
        parsedQuery={parseSqlQuery(`SELECT channel, count(*) as cnt FROM wikipedia GROUP BY 1`)}
        onQueryChange={() => {}}
      />
    );

    const { container } = render(stringMenu);
    expect(container).toMatchSnapshot();
  });
});
