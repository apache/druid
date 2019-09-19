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
import { sqlParserFactory } from 'druid-query-toolkit';
import React from 'react';

import { ColumnMetadata } from '../../../utils/column-metadata';

import { ColumnTree } from './column-tree';

describe('column tree', () => {
  const parser = sqlParserFactory(['COUNT']);

  it('matches snapshot', () => {
    const columnTree = (
      <ColumnTree
        getParsedQuery={() => {
          return parser(`SELECT channel, count(*) as cnt FROM wikipedia GROUP BY 1`);
        }}
        defaultSchema="druid"
        defaultTable="wikipedia"
        columnMetadataLoading={false}
        columnMetadata={
          [
            {
              TABLE_SCHEMA: 'druid',
              TABLE_NAME: 'wikipedia',
              COLUMN_NAME: '__time',
              DATA_TYPE: 'TIMESTAMP',
            },
            {
              TABLE_SCHEMA: 'druid',
              TABLE_NAME: 'wikipedia',
              COLUMN_NAME: 'added',
              DATA_TYPE: 'BIGINT',
            },
            {
              TABLE_SCHEMA: 'druid',
              TABLE_NAME: 'wikipedia',
              COLUMN_NAME: 'addedBy10',
              DATA_TYPE: 'FLOAT',
            },
            {
              TABLE_SCHEMA: 'sys',
              TABLE_NAME: 'tasks',
              COLUMN_NAME: 'error_msg',
              DATA_TYPE: 'VARCHAR',
            },
          ] as ColumnMetadata[]
        }
        onQueryStringChange={() => {}}
      />
    );

    const { container } = render(columnTree);
    expect(container.firstChild).toMatchSnapshot();
  });
});
