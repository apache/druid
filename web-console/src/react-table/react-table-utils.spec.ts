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

import {
  sqlQueryCustomTableFilter,
  stringToTableFilters,
  tableFiltersToString,
} from './react-table-utils';

describe('react-table-utils', () => {
  describe('sqlQueryCustomTableFilter', () => {
    it('works with contains', () => {
      expect(
        String(
          sqlQueryCustomTableFilter({
            id: 'datasource',
            value: `Hello`,
          }),
        ),
      ).toEqual(`LOWER("datasource") LIKE '%hello%'`);
    });

    it('works with exact', () => {
      expect(
        String(
          sqlQueryCustomTableFilter({
            id: 'datasource',
            value: `=Hello`,
          }),
        ),
      ).toEqual(`"datasource" = 'Hello'`);
    });

    it('works with less than or equal', () => {
      expect(
        String(
          sqlQueryCustomTableFilter({
            id: 'datasource',
            value: `<=Hello`,
          }),
        ),
      ).toEqual(`"datasource" <= 'Hello'`);
    });
  });

  it('tableFiltersToString', () => {
    expect(
      tableFiltersToString([
        { id: 'x', value: '~y' },
        { id: 'z', value: '=w&' },
      ]),
    ).toEqual('x~y&z=w%26');
  });

  it('stringToTableFilters', () => {
    expect(stringToTableFilters(undefined)).toEqual([]);
    expect(stringToTableFilters('')).toEqual([]);
    expect(stringToTableFilters('x~y')).toEqual([{ id: 'x', value: '~y' }]);
    expect(stringToTableFilters('x~y&z=w%26')).toEqual([
      { id: 'x', value: '~y' },
      { id: 'z', value: '=w&' },
    ]);
  });
});
