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

export const NUMERIC_TIME_FORMATS: string[] = ['posix', 'millis', 'micro', 'nano'];
export const BASIC_TIME_FORMATS: string[] = ['auto', 'iso'].concat(NUMERIC_TIME_FORMATS);

export const DATE_ONLY_TIME_FORMATS: string[] = [
  'dd/MM/yyyy',
  'MM/dd/yyyy',
  'd/M/yy',
  'M/d/yy',
  'd/M/yyyy',
  'M/d/yyyy',
];

export const DATETIME_TIME_FORMATS: string[] = [
  'd/M/yyyy H:mm:ss',
  'M/d/yyyy H:mm:ss',
  'MM/dd/yyyy hh:mm:ss a',
  'yyyy-MM-dd HH:mm:ss',
  'yyyy-MM-dd HH:mm:ss.S',
];

export const OTHER_TIME_FORMATS: string[] = ['MMM dd HH:mm:ss'];

const ALL_FORMAT_VALUES: string[] = BASIC_TIME_FORMATS.concat(
  DATE_ONLY_TIME_FORMATS,
  DATETIME_TIME_FORMATS,
  OTHER_TIME_FORMATS,
);

const MIN_POSIX = 3.15576e8; // 3 years in posix, so Tue Jan 01 1980
const MIN_MILLIS = MIN_POSIX * 1000;
const MIN_MICRO = MIN_MILLIS * 1000;
const MIN_NANO = MIN_MICRO * 1000;
const MAX_NANO = MIN_NANO * 1000;

// tslint:disable-next-line:max-line-length
export const AUTO_MATCHER = /^([\+-]?\d{4}(?!\d{2}\b))((-?)((0[1-9]|1[0-2])(\3([12]\d|0[1-9]|3[01]))?|W([0-4]\d|5[0-2])(-?[1-7])?|(00[1-9]|0[1-9]\d|[12]\d{2}|3([0-5]\d|6[1-6])))( ((([01]\d|2[0-3])((:?)[0-5]\d)?|24:?00)([\.,]\d+(?!:))?)?(\17[0-5]\d([\.,]\d+)?)?([zZ]|([\+-])([01]\d|2[0-3]):?([0-5]\d)?)?)?)$/;

// tslint:disable-next-line:max-line-length
export const ISO_MATCHER = /^([\+-]?\d{4}(?!\d{2}\b))((-?)((0[1-9]|1[0-2])(\3([12]\d|0[1-9]|3[01]))?|W([0-4]\d|5[0-2])(-?[1-7])?|(00[1-9]|0[1-9]\d|[12]\d{2}|3([0-5]\d|6[1-6])))(T((([01]\d|2[0-3])((:?)[0-5]\d)?|24:?00)([\.,]\d+(?!:))?)?(\17[0-5]\d([\.,]\d+)?)?([zZ]|([\+-])([01]\d|2[0-3]):?([0-5]\d)?)?)?)$/;

// Note: AUTO and ISO are basically the same except ISO has a space as a separator instead of the T

export function timeFormatMatches(format: string, value: string | number): boolean {
  const absValue = Math.abs(Number(value));
  switch (format) {
    case 'auto':
      return AUTO_MATCHER.test(String(value));

    case 'iso':
      return ISO_MATCHER.test(String(value));

    case 'posix':
      return MIN_POSIX < absValue && absValue < MIN_MILLIS;

    case 'millis':
      return MIN_MILLIS < absValue && absValue < MIN_MICRO;

    case 'micro':
      return MIN_MICRO < absValue && absValue < MIN_NANO;

    case 'nano':
      return MIN_NANO < absValue && absValue < MAX_NANO;

    default:
      return jodaFormatToRegExp(format).test(String(value));
  }
}

export function possibleDruidFormatForValues(values: any[]): string | null {
  return (
    ALL_FORMAT_VALUES.filter(format => {
      return values.every(value => timeFormatMatches(format, value));
    })[0] || null
  );
}
