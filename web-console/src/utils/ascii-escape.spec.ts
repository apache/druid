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

import { escapeAsciiControlCharacters, unescapeAsciiControlCharacters } from './ascii-escape';

describe('ascii-escape', () => {
  it('escapeAsciiControlCharacters', () => {
    expect(
      new Array(38).fill(0).map((_, i) => {
        return escapeAsciiControlCharacters(
          i + ' : `' + String.fromCharCode(i) + '` : `' + String.fromCharCode(i) + '`',
        );
      }),
    ).toEqual([
      '0 : `\\x00` : `\\x00`',
      '1 : `\\x01` : `\\x01`',
      '2 : `\\x02` : `\\x02`',
      '3 : `\\x03` : `\\x03`',
      '4 : `\\x04` : `\\x04`',
      '5 : `\\x05` : `\\x05`',
      '6 : `\\x06` : `\\x06`',
      '7 : `\\x07` : `\\x07`',
      '8 : `\\x08` : `\\x08`',
      '9 : `\\x09` : `\\x09`',
      '10 : `\\x0a` : `\\x0a`',
      '11 : `\\x0b` : `\\x0b`',
      '12 : `\\x0c` : `\\x0c`',
      '13 : `\\x0d` : `\\x0d`',
      '14 : `\\x0e` : `\\x0e`',
      '15 : `\\x0f` : `\\x0f`',
      '16 : `\\x10` : `\\x10`',
      '17 : `\\x11` : `\\x11`',
      '18 : `\\x12` : `\\x12`',
      '19 : `\\x13` : `\\x13`',
      '20 : `\\x14` : `\\x14`',
      '21 : `\\x15` : `\\x15`',
      '22 : `\\x16` : `\\x16`',
      '23 : `\\x17` : `\\x17`',
      '24 : `\\x18` : `\\x18`',
      '25 : `\\x19` : `\\x19`',
      '26 : `\\x1a` : `\\x1a`',
      '27 : `\\x1b` : `\\x1b`',
      '28 : `\\x1c` : `\\x1c`',
      '29 : `\\x1d` : `\\x1d`',
      '30 : `\\x1e` : `\\x1e`',
      '31 : `\\x1f` : `\\x1f`',
      '32 : ` ` : ` `',
      '33 : `!` : `!`',
      '34 : `"` : `"`',
      '35 : `#` : `#`',
      '36 : `$` : `$`',
      '37 : `%` : `%`',
    ]);
  });

  it('unescapeAsciiControlCharacters', () => {
    new Array(38).fill(0).forEach((_, i) => {
      const str = i + ' : `' + String.fromCharCode(i) + '` : `' + String.fromCharCode(i) + '`';
      return expect(unescapeAsciiControlCharacters(escapeAsciiControlCharacters(str))).toEqual(str);
    });
  });
});
