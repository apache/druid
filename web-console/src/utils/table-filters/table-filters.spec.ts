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
import { TableFilters } from './table-filters';

describe('TableFilters', () => {
  describe('toString', () => {
    it('converts filters to string', () => {
      const filters = new TableFilters([
        new TableFilter('x', '~', 'y'),
        new TableFilter('z', '=', 'w&%/'),
      ]);
      expect(filters.toString()).toEqual('x~y&z=w%26%25%2F');
    });
  });

  describe('fromString', () => {
    it('handles empty strings', () => {
      expect(TableFilters.fromString(undefined).isEmpty()).toBe(true);
      expect(TableFilters.fromString('').isEmpty()).toBe(true);
    });

    it('parses single filter', () => {
      const filters = TableFilters.fromString('x~y');
      expect(filters.size()).toBe(1);
      const filterArray = filters.toArray();
      expect(filterArray[0].key).toBe('x');
      expect(filterArray[0].mode).toBe('~');
      expect(filterArray[0].value).toBe('y');
    });

    it('parses multiple filters with encoded characters', () => {
      const filters = TableFilters.fromString('x~y&z=w%26%25%2F');
      expect(filters.size()).toBe(2);
      const filterArray = filters.toArray();
      expect(filterArray[0].key).toBe('x');
      expect(filterArray[0].mode).toBe('~');
      expect(filterArray[0].value).toBe('y');
      expect(filterArray[1].key).toBe('z');
      expect(filterArray[1].mode).toBe('=');
      expect(filterArray[1].value).toBe('w&%/');
    });

    it('parses comparison filters', () => {
      const filters = TableFilters.fromString('x<3&y<=3');
      expect(filters.size()).toBe(2);
      const filterArray = filters.toArray();
      expect(filterArray[0].key).toBe('x');
      expect(filterArray[0].mode).toBe('<');
      expect(filterArray[0].value).toBe('3');
      expect(filterArray[1].key).toBe('y');
      expect(filterArray[1].mode).toBe('<=');
      expect(filterArray[1].value).toBe('3');
    });
  });

  describe('eq', () => {
    it('creates filters from key-value pairs', () => {
      const filters = TableFilters.eq({ datasource: 'test', type: 'index' });
      expect(filters.size()).toBe(2);
      const filterArray = filters.toArray();
      expect(filterArray[0].key).toBe('datasource');
      expect(filterArray[0].mode).toBe('=');
      expect(filterArray[0].value).toBe('test');
      expect(filterArray[1].key).toBe('type');
      expect(filterArray[1].mode).toBe('=');
      expect(filterArray[1].value).toBe('index');
    });
  });

  describe('empty', () => {
    it('creates an empty TableFilters', () => {
      const filters = TableFilters.empty();
      expect(filters.isEmpty()).toBe(true);
      expect(filters.size()).toBe(0);
    });
  });
});
