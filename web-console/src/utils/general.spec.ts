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
  arrangeWithPrefixSuffix,
  caseInsensitiveEquals,
  formatBytes,
  formatBytesCompact,
  formatInteger,
  formatMegabytes,
  formatMillions,
  formatNumber,
  formatPercent,
  hashJoaat,
  moveElement,
  moveToIndex,
  offsetToRowColumn,
  OVERLAY_OPEN_SELECTOR,
  parseCsvLine,
  swapElements,
} from './general';

describe('general', () => {
  describe('arrangeWithPrefixSuffix', () => {
    it('works in simple case', () => {
      expect(
        arrangeWithPrefixSuffix(
          'abcdefgh'.split('').reverse(),
          'gef'.split(''),
          'ba'.split(''),
        ).join(''),
      ).toEqual('gefhdcba');
    });

    it('dedupes', () => {
      expect(
        arrangeWithPrefixSuffix(
          'abcdefgh'.split('').reverse(),
          'gefgef'.split(''),
          'baba'.split(''),
        ).join(''),
      ).toEqual('gefhdcba');
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

  describe('moveElement', () => {
    it('moves items in an array', () => {
      expect(moveElement(['a', 'b', 'c'], 2, 0)).toEqual(['c', 'a', 'b']);
      expect(moveElement(['a', 'b', 'c'], 1, 1)).toEqual(['a', 'b', 'c']);
      expect(moveElement(['F', 'B', 'W', 'B'], 2, 1)).toEqual(['F', 'W', 'B', 'B']);
      expect(moveElement([1, 2, 3], 2, 1)).toEqual([1, 3, 2]);
    });
  });

  describe('moveToIndex', () => {
    it('works', () => {
      expect(moveToIndex(['a', 'b', 'c', 'd', 'e'], x => ['e', 'c'].indexOf(x))).toEqual([
        'e',
        'c',
        'a',
        'b',
        'd',
      ]);
    });
  });

  describe('formatNumber', () => {
    it('works', () => {
      expect(formatNumber(null as any)).toEqual('0');
      expect(formatNumber(0)).toEqual('0');
      expect(formatNumber(5)).toEqual('5');
      expect(formatNumber(5.1)).toEqual('5.1');
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

  describe('formatMillions', () => {
    it('works', () => {
      expect(formatMillions(1e6)).toEqual('1.000 M');
      expect(formatMillions(1e6 + 1)).toEqual('1.000 M');
      expect(formatMillions(1234567)).toEqual('1.235 M');
      expect(formatMillions(345.2)).toEqual('345');
    });
  });

  describe('parseCsvLine', () => {
    it('works in general', () => {
      expect(parseCsvLine(`Hello,,"",world,123,Hi "you","Quote, ""escapes"", work"\r\n`)).toEqual([
        `Hello`,
        ``,
        ``,
        `world`,
        `123`,
        `Hi "you"`,
        `Quote, "escapes", work`,
      ]);
    });

    it('works in empty case', () => {
      expect(parseCsvLine(``)).toEqual([``]);
    });

    it('works in trivial case', () => {
      expect(parseCsvLine(`Hello`)).toEqual([`Hello`]);
    });

    it('only parses first line', () => {
      expect(parseCsvLine(`Hi,there\na,b\nx,y\n`)).toEqual([`Hi`, `there`]);
    });
  });

  describe('hashJoaat', () => {
    it('works', () => {
      expect(hashJoaat('a')).toEqual(0xca2e9442);
      expect(hashJoaat('The quick brown fox jumps over the lazy dog')).toEqual(0x7647f758);
    });
  });

  describe('offsetToRowColumn', () => {
    it('works', () => {
      const str = 'Hello\nThis is a test\nstring.';
      expect(offsetToRowColumn(str, -6)).toBeUndefined();
      expect(offsetToRowColumn(str, 666)).toBeUndefined();
      expect(offsetToRowColumn(str, 3)).toEqual({
        row: 0,
        column: 3,
      });
      expect(offsetToRowColumn(str, 5)).toEqual({
        row: 0,
        column: 5,
      });
      expect(offsetToRowColumn(str, 24)).toEqual({
        row: 2,
        column: 3,
      });
      expect(offsetToRowColumn(str, str.length)).toEqual({
        row: 2,
        column: 7,
      });
    });
  });

  describe('caseInsensitiveEquals', () => {
    it('works', () => {
      expect(caseInsensitiveEquals(undefined, undefined)).toEqual(true);
      expect(caseInsensitiveEquals(undefined, 'x')).toEqual(false);
      expect(caseInsensitiveEquals('x', undefined)).toEqual(false);
      expect(caseInsensitiveEquals('x', 'X')).toEqual(true);
      expect(caseInsensitiveEquals(undefined, '')).toEqual(false);
    });
  });

  describe('OVERLAY_OPEN_SELECTOR', () => {
    it('is what it is', () => {
      expect(OVERLAY_OPEN_SELECTOR).toEqual('.bp5-portal .bp5-overlay-open');
    });
  });
});
