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

import { TableFilter } from './table-filter';

describe('TableFilter', () => {
  describe('sqlQueryCustomTableFilter', () => {
    it('works with contains', () => {
      expect(String(new TableFilter('datasource', '~', 'Hello').toSqlExpression())).toEqual(
        `LOWER("datasource") LIKE '%hello%'`,
      );
    });

    it('works with exact', () => {
      expect(String(new TableFilter('datasource', '=', 'Hello').toSqlExpression())).toEqual(
        `"datasource" = 'Hello'`,
      );
    });

    it('works with less than or equal', () => {
      expect(String(new TableFilter('datasource', '<=', 'Hello').toSqlExpression())).toEqual(
        `"datasource" <= 'Hello'`,
      );
    });
  });

  describe('toFilter', () => {
    it('converts to react-table Filter format', () => {
      const filter = new TableFilter('datasource', '=', 'test');
      expect(filter.toFilter()).toEqual({ id: 'datasource', value: '=test' });
    });
  });

  describe('fromFilter', () => {
    it('converts from react-table Filter format', () => {
      const filter = TableFilter.fromFilter({ id: 'datasource', value: '=test' });
      expect(filter.key).toBe('datasource');
      expect(filter.mode).toBe('=');
      expect(filter.value).toBe('test');
    });

    it('defaults to contains mode when no mode specified', () => {
      const filter = TableFilter.fromFilter({ id: 'datasource', value: 'test' });
      expect(filter.key).toBe('datasource');
      expect(filter.mode).toBe('~');
      expect(filter.value).toBe('test');
    });
  });

  describe('matches', () => {
    it('works with equals', () => {
      const filter = new TableFilter('status', '=', 'active');
      expect(filter.matches('active')).toBe(true);
      expect(filter.matches('inactive')).toBe(false);
    });

    it('works with contains', () => {
      const filter = new TableFilter('name', '~', 'test');
      expect(filter.matches('testing')).toBe(true);
      expect(filter.matches('TEST')).toBe(true);
      expect(filter.matches('notest')).toBe(true);
      expect(filter.matches('other')).toBe(false);
    });

    it('works with comparisons', () => {
      const filter = new TableFilter('count', '<', '10');
      expect(filter.matches('05')).toBe(true); // String comparison: '05' < '10'
      expect(filter.matches('2')).toBe(false); // String comparison: '2' > '10'
    });
  });

  describe('equals', () => {
    it('compares two filters', () => {
      const filter1 = new TableFilter('x', '=', 'y');
      const filter2 = new TableFilter('x', '=', 'y');
      const filter3 = new TableFilter('x', '=', 'z');
      expect(filter1.equals(filter2)).toBe(true);
      expect(filter1.equals(filter3)).toBe(false);
    });
  });
});
