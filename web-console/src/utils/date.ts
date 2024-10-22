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

import type { DateRange, NonNullDateRange } from '@blueprintjs/datetime';

const CURRENT_YEAR = new Date().getUTCFullYear();

export function isNonNullRange(range: DateRange): range is NonNullDateRange {
  return range[0] != null && range[1] != null;
}

export function dateToIsoDateString(date: Date): string {
  return date.toISOString().slice(0, 10);
}
export function prettyFormatIsoDateWithMsIfNeeded(isoDate: string | Date): string {
  return (typeof isoDate === 'string' ? isoDate : isoDate.toISOString())
    .replace('T', ' ')
    .replace('Z', '')
    .replace('.000', '');
}

export function prettyFormatIsoDate(isoDate: string | Date): string {
  return prettyFormatIsoDateWithMsIfNeeded(isoDate).replace(/\.\d\d\d/, '');
}

export function prettyFormatIsoDateTick(date: Date): string {
  // s like 2016-06-27T19:00:00.000Z
  let s = date.toISOString();
  if (!s.endsWith('.000Z')) {
    return s.slice(19, 23); // => ".001"
  }
  s = s.slice(0, 19); // s like 2016-06-27T19:00:00

  if (!s.endsWith(':00')) {
    return s.slice(11); // => 00:00:01
  }
  s = s.slice(0, 16); // s like 2016-06-27T19:00

  if (!s.endsWith('T00:00')) {
    return s.slice(11); // => 00:00
  }

  return s.slice(0, 10); // s like 2016-06-27
}

export function utcToLocalDate(utcDate: Date): Date {
  // Function removes the local timezone of the date and displays it in UTC
  return new Date(utcDate.getTime() + utcDate.getTimezoneOffset() * 60000);
}

export function localToUtcDate(localDate: Date): Date {
  // Function removes the local timezone of the date and displays it in UTC
  return new Date(localDate.getTime() - localDate.getTimezoneOffset() * 60000);
}

export function utcToLocalDateRange([start, end]: DateRange): DateRange {
  return [start ? utcToLocalDate(start) : null, end ? utcToLocalDate(end) : null];
}

export function localToUtcDateRange([start, end]: DateRange): DateRange {
  return [start ? localToUtcDate(start) : null, end ? localToUtcDate(end) : null];
}

export function intervalToLocalDateRange(interval: string): DateRange {
  const dates = interval.split('/');
  if (dates.length !== 2) return [null, null];

  const startDate = Date.parse(dates[0]) ? new Date(dates[0]) : null;
  const endDate = Date.parse(dates[1]) ? new Date(dates[1]) : null;

  // Must check if the start and end dates are within range
  return [
    startDate && startDate.getFullYear() < CURRENT_YEAR - 20 ? null : startDate,
    endDate && endDate.getFullYear() > CURRENT_YEAR ? null : endDate,
  ];
}

export function localDateRangeToInterval(localRange: DateRange): string {
  // This function takes in the dates selected from datepicker in local time, and displays them in UTC
  // Shall Blueprint make any changes to the way dates are selected, this function will have to be reworked
  const [localStartDate, localEndDate] = localRange;
  return `${localStartDate ? localToUtcDate(localStartDate).toISOString().slice(0, 19) : ''}/${
    localEndDate ? localToUtcDate(localEndDate).toISOString().slice(0, 19) : ''
  }`;
}

export function maxDate(a: Date, b: Date): Date {
  return a > b ? a : b;
}

export function minDate(a: Date, b: Date): Date {
  return a < b ? a : b;
}
