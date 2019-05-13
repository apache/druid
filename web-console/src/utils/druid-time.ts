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

export type DruidTimestampFormat = 'iso' | 'millis' | 'posix' | 'auto' | 'd/M/yyyy' | 'dd-M-yyyy hh:mm:ss a' |
  'MM/dd/YYYY' | 'M/d/YY' | 'MM/dd/YYYY hh:mm:ss a' | 'YYYY-MM-dd HH:mm:ss' | 'YYYY-MM-dd HH:mm:ss.S';

export const TIMESTAMP_FORMAT_VALUES: DruidTimestampFormat[] = [
  'iso', 'millis', 'posix', 'MM/dd/YYYY hh:mm:ss a', 'MM/dd/YYYY', 'M/d/YY', 'd/M/yyyy',
  'YYYY-MM-dd HH:mm:ss', 'YYYY-MM-dd HH:mm:ss.S'
];

const EXAMPLE_DATE_ISO = '2015-10-29T23:00:00.000Z';
const EXAMPLE_DATE_VALUE = Date.parse(EXAMPLE_DATE_ISO);
const MIN_MILLIS = 3.15576e11; // 3 years in millis, so Tue Jan 01 1980
const MAX_MILLIS = EXAMPLE_DATE_VALUE * 10;
const MIN_POSIX = MIN_MILLIS / 1000;
const MAX_POSIX = MAX_MILLIS / 1000;

// copied from http://goo.gl/0ejHHW with small tweak to make dddd not pass on its own
// tslint:disable-next-line:max-line-length
export const ISO_MATCHER = new RegExp(/^([\+-]?\d{4}(?!\d{2}\b))((-?)((0[1-9]|1[0-2])(\3([12]\d|0[1-9]|3[01]))?|W([0-4]\d|5[0-2])(-?[1-7])?|(00[1-9]|0[1-9]\d|[12]\d{2}|3([0-5]\d|6[1-6])))(T((([01]\d|2[0-3])((:?)[0-5]\d)?|24:?00)([\.,]\d+(?!:))?)?(\17[0-5]\d([\.,]\d+)?)?([zZ]|([\+-])([01]\d|2[0-3]):?([0-5]\d)?)?)?)$/);
export const JODA_TO_REGEXP_LOOKUP: Record<string, RegExp> = {
  'd/M/yyyy': /^[12]?\d\/1?\d\/\d\d\d\d$/,
  'MM/dd/YYYY': /^\d\d\/\d\d\/\d\d\d\d$/,
  'M/d/YY': /^1?\d\/[12]?\d\/\d\d$/,
  'd-M-yyyy hh:mm:ss a': /^[12]?\d-1?\d-\d\d\d\d \d\d:\d\d:\d\d [ap]m$/i,
  'MM/dd/YYYY hh:mm:ss a' : /^\d\d\/\d\d\/\d\d\d\d \d\d:\d\d:\d\d [ap]m$/i,
  'YYYY-MM-dd HH:mm:ss' : /^\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d$/,
  'YYYY-MM-dd HH:mm:ss.S': /^\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d\.\d\d\d$/
};

export function timeFormatMatches(format: DruidTimestampFormat, value: string | number): boolean {
  if (format === 'iso') {
    return ISO_MATCHER.test(String(value));
  }

  if (format === 'millis') {
    const absValue = Math.abs(Number(value));
    return MIN_MILLIS < absValue && absValue < MAX_MILLIS;
  }

  if (format === 'posix') {
    const absValue = Math.abs(Number(value));
    return MIN_POSIX < absValue && absValue < MAX_POSIX;
  }

  const formatRegexp = JODA_TO_REGEXP_LOOKUP[format];
  if (!formatRegexp) throw new Error(`unknown Druid format ${format}`);

  return formatRegexp.test(String(value));
}

export function possibleDruidFormatForValues(values: any[]): DruidTimestampFormat | null {
  return TIMESTAMP_FORMAT_VALUES.filter(format => {
    return values.every(value => timeFormatMatches(format, value));
  })[0] || null;
}
