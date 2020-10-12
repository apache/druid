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
  alphanumericCompare,
  formatBytes,
  formatBytesCompact,
  formatInteger,
  formatMegabytes,
  formatPercent,
  sortWithPrefixSuffix,
  sqlQueryCustomTableFilter,
  swapElements,
} from './general';

describe('general', () => {
  describe('sortWithPrefixSuffix', () => {
    it('works in simple case', () => {
      expect(
        sortWithPrefixSuffix(
          'abcdefgh'.split('').reverse(),
          'gef'.split(''),
          'ba'.split(''),
          alphanumericCompare,
        ).join(''),
      ).toEqual('gefcdhba');
    });

    it('dedupes', () => {
      expect(
        sortWithPrefixSuffix(
          'abcdefgh'.split('').reverse(),
          'gefgef'.split(''),
          'baba'.split(''),
          alphanumericCompare,
        ).join(''),
      ).toEqual('gefcdhba');
    });
  });

  describe('sqlQueryCustomTableFilter', () => {
    it('works', () => {
      expect(
        sqlQueryCustomTableFilter({
          id: 'datasource',
          value: `hello`,
        }),
      ).toMatchInlineSnapshot(`"LOWER(\\"datasource\\") LIKE LOWER('%hello%')"`);

      expect(
        sqlQueryCustomTableFilter({
          id: 'datasource',
          value: `"hello"`,
        }),
      ).toMatchInlineSnapshot(`"\\"datasource\\" = 'hello'"`);
    });
  });

  describe('swapElements', () => {
    const array = ['a', 'b', 'c', 'd', 'e'];

    it('works when nothing changes', () => {
      expect(swapElements(array, 0, 0)).toEqual(['a', 'b', 'c', 'd', 'e']);
    });

    it('works upward', () => {
      expect(swapElements(array, 2, 1)).toEqual(['a', 'c', 'b', 'd', 'e']);
      expect(swapElements(array, 2, 0)).toEqual(['c', 'b', 'a', 'd', 'e']);
    });

    it('works downward', () => {
      expect(swapElements(array, 2, 3)).toEqual(['a', 'b', 'd', 'c', 'e']);
      expect(swapElements(array, 2, 4)).toEqual(['a', 'b', 'e', 'd', 'c']);
    });
  });

  describe('formatInteger', () => {
    it('works', () => {
      expect(formatInteger(10000)).toEqual('10,000');
    });
  });

  describe('formatBytes', () => {
    it('works', () => {
      expect(formatBytes(10000)).toEqual('10.00 KB');
    });
  });

  describe('formatBytesCompact', () => {
    it('works', () => {
      expect(formatBytesCompact(10000)).toEqual('10.00KB');
    });
  });

  describe('formatMegabytes', () => {
    it('works', () => {
      expect(formatMegabytes(30000000)).toEqual('28.6');
    });
  });

  describe('formatPercent', () => {
    it('works', () => {
      expect(formatPercent(2 / 3)).toEqual('66.67%');
    });
  });
});
