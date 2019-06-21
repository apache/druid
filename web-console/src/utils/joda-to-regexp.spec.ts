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
    expect(jodaFormatToRegExp('d/M/yyyy').toString()).toMatchSnapshot();
    expect(jodaFormatToRegExp('MM/dd/YYYY').toString()).toMatchSnapshot();
    expect(jodaFormatToRegExp('M/d/YY').toString()).toMatchSnapshot();
    expect(jodaFormatToRegExp('d-M-yyyy hh:mm:ss a').toString()).toMatchSnapshot();
    expect(jodaFormatToRegExp('MM/dd/YYYY hh:mm:ss a').toString()).toMatchSnapshot();
    expect(jodaFormatToRegExp('YYYY-MM-dd HH:mm:ss').toString()).toMatchSnapshot();
    expect(jodaFormatToRegExp('YYYY-MM-dd HH:mm:ss.S').toString()).toMatchSnapshot();
  });

  it('matches dates when needed', () => {
    expect(jodaFormatToRegExp('d-M-yyyy hh:mm:ss a').test('26-4-1986 01:23:40 am')).toEqual(true);
    expect(jodaFormatToRegExp('YYYY-MM-dd HH:mm:ss.S').test('26-4-1986 01:23:40 am')).toEqual(
      false,
    );
  });
});
