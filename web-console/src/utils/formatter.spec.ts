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

import { JSON_STRING_FORMATTER } from './formatter';

describe('Formatter', () => {
  describe('JSON_STRING_FORMATTER', () => {
    it('has a working stringify', () => {
      expect(
        new Array(38).fill(0).map((_, i) => {
          return JSON_STRING_FORMATTER.stringify(
            i + ' : `' + String.fromCharCode(i) + '` : `' + String.fromCharCode(i) + '`',
          );
        }),
      ).toEqual([
        '0 : `\\u0000` : `\\u0000`',
        '1 : `\\u0001` : `\\u0001`',
        '2 : `\\u0002` : `\\u0002`',
        '3 : `\\u0003` : `\\u0003`',
        '4 : `\\u0004` : `\\u0004`',
        '5 : `\\u0005` : `\\u0005`',
        '6 : `\\u0006` : `\\u0006`',
        '7 : `\\u0007` : `\\u0007`',
        '8 : `\\b` : `\\b`',
        '9 : `\\t` : `\\t`',
        '10 : `\\n` : `\\n`',
        '11 : `\\u000b` : `\\u000b`',
        '12 : `\\f` : `\\f`',
        '13 : `\\r` : `\\r`',
        '14 : `\\u000e` : `\\u000e`',
        '15 : `\\u000f` : `\\u000f`',
        '16 : `\\u0010` : `\\u0010`',
        '17 : `\\u0011` : `\\u0011`',
        '18 : `\\u0012` : `\\u0012`',
        '19 : `\\u0013` : `\\u0013`',
        '20 : `\\u0014` : `\\u0014`',
        '21 : `\\u0015` : `\\u0015`',
        '22 : `\\u0016` : `\\u0016`',
        '23 : `\\u0017` : `\\u0017`',
        '24 : `\\u0018` : `\\u0018`',
        '25 : `\\u0019` : `\\u0019`',
        '26 : `\\u001a` : `\\u001a`',
        '27 : `\\u001b` : `\\u001b`',
        '28 : `\\u001c` : `\\u001c`',
        '29 : `\\u001d` : `\\u001d`',
        '30 : `\\u001e` : `\\u001e`',
        '31 : `\\u001f` : `\\u001f`',
        '32 : ` ` : ` `',
        '33 : `!` : `!`',
        '34 : `\\"` : `\\"`',
        '35 : `#` : `#`',
        '36 : `$` : `$`',
        '37 : `%` : `%`',
      ]);

      expect(JSON_STRING_FORMATTER.stringify(`hello "world"`)).toEqual(`hello \\"world\\"`);
    });

    it('has a working parse', () => {
      expect(JSON_STRING_FORMATTER.parse(`h\u0065llo\t"world"\\`)).toEqual(`hello\t"world"\\`);
    });

    it('parses back and forth', () => {
      new Array(38).fill(0).forEach((_, i) => {
        const str = i + ' : `' + String.fromCharCode(i) + '` : `' + String.fromCharCode(i) + '`';
        expect(JSON_STRING_FORMATTER.parse(JSON_STRING_FORMATTER.stringify(str))).toEqual(str);
      });
    });
  });
});
