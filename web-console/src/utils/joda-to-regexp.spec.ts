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

import { jodaFormatToRegExp } from './joda-to-regexp';

describe('jodaFormatToRegExp', () => {
  it('works for common formats', () => {
    expect(jodaFormatToRegExp('d/M/yyyy').toString()).toMatchInlineSnapshot(
      `"/^(?:3[0-1]|[12][0-9]|[1-9])\\\\/(?:1[0-2]|[1-9])\\\\/[0-9]{4}$/i"`,
    );

    expect(jodaFormatToRegExp('MM/dd/YYYY').toString()).toMatchInlineSnapshot(
      `"/^(?:1[0-2]|0[1-9])\\\\/(?:3[0-1]|[12][0-9]|0[1-9])\\\\/[0-9]{4}$/i"`,
    );

    expect(jodaFormatToRegExp('M/d/YY').toString()).toMatchInlineSnapshot(
      `"/^(?:1[0-2]|[1-9])\\\\/(?:3[0-1]|[12][0-9]|[1-9])\\\\/[0-9]{2}$/i"`,
    );

    expect(jodaFormatToRegExp('d-M-yyyy hh:mm:ss a').toString()).toMatchInlineSnapshot(
      `"/^(?:3[0-1]|[12][0-9]|[1-9])-(?:1[0-2]|[1-9])-[0-9]{4} (?:1[0-2]|0[1-9]):[0-5][0-9]:[0-5][0-9] [ap]m$/i"`,
    );

    expect(jodaFormatToRegExp('MM/dd/YYYY hh:mm:ss a').toString()).toMatchInlineSnapshot(
      `"/^(?:1[0-2]|0[1-9])\\\\/(?:3[0-1]|[12][0-9]|0[1-9])\\\\/[0-9]{4} (?:1[0-2]|0[1-9]):[0-5][0-9]:[0-5][0-9] [ap]m$/i"`,
    );

    expect(jodaFormatToRegExp('YYYY-MM-dd HH:mm:ss').toString()).toMatchInlineSnapshot(
      `"/^[0-9]{4}-(?:1[0-2]|0[1-9])-(?:3[0-1]|[12][0-9]|0[1-9]) (?:2[0-3]|1[0-9]|0[0-9]):[0-5][0-9]:[0-5][0-9]$/i"`,
    );

    expect(jodaFormatToRegExp('YYYY-MM-dd HH:mm:ss.S').toString()).toMatchInlineSnapshot(
      `"/^[0-9]{4}-(?:1[0-2]|0[1-9])-(?:3[0-1]|[12][0-9]|0[1-9]) (?:2[0-3]|1[0-9]|0[0-9]):[0-5][0-9]:[0-5][0-9].[0-9]{1,3}$/i"`,
    );
  });

  it('matches dates when needed', () => {
    expect(jodaFormatToRegExp('d-M-yyyy hh:mm:ss a').test('26-4-1986 01:23:40 am')).toEqual(true);
    expect(jodaFormatToRegExp('YYYY-MM-dd HH:mm:ss.S').test('26-4-1986 01:23:40 am')).toEqual(
      false,
    );
  });
});
