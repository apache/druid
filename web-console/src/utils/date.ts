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
import { fromDate, toTimeZone } from '@internationalized/date';
import type { Timezone } from 'chronoshift';
import dayjs from 'dayjs';

import type { WebConsoleConfig } from '../druid-models/web-console-config/web-console-config';

import { localStorageGetJson, LocalStorageKeys } from './local-storage-keys';

const CURRENT_YEAR = new Date().getUTCFullYear();
export const DATE_FORMAT = 'YYYY-MM-DDTHH:mm:ss.SSSZ';

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

export function toIsoStringInTimezone(date: Date, timezone: Timezone): string {
  if (timezone.isUTC()) return date.toISOString();
  const zonedDate = toTimeZone(fromDate(date, 'Etc/UTC'), timezone.toString());
  return zonedDate.toString().replace(/[+-]\d\d:\d\d\[.+$/, '');
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

export function formatDate(value: string) {
  const webConsoleConfig: WebConsoleConfig | undefined = localStorageGetJson(
    LocalStorageKeys.WEB_CONSOLE_CONFIGS,
  );
  const showLocalTime = webConsoleConfig?.showLocalTime;
  return showLocalTime ? dayjs(value).format(DATE_FORMAT) : dayjs(value).toISOString();
}

/**
 * Parses an ISO 8601 date string into a Date object.
 * Accepts flexible formats including:
 * - Year only: "2016"
 * - Year-month: "2016-06"
 * - Date only: "2016-06-20"
 * - Date with hour: "2016-06-20 21" or "2016-06-20T21"
 * - Date with hour-minute: "2016-06-20 21:31" or "2016-06-20T21:31"
 * - Date with hour-minute-second: "2016-06-20 21:31:02" or "2016-06-20T21:31:02"
 * - Full datetime: "2016-06-20T21:31:02.123" or "2016-06-20 21:31:02.123"
 * - Optional trailing "Z": "2016-06-20T21:31:02Z" (the Z is ignored, date is always parsed as UTC)
 *
 * Missing components default to: month=1, day=1, hour=0, minute=0, second=0, millisecond=0
 *
 * @param dateString - The ISO date string to parse
 * @returns A Date object in UTC
 * @throws Error if the date string is invalid or components are out of range
 */
export function parseIsoDate(dateString: string): Date {
  // Match ISO 8601 date format with optional date and time components and optional trailing Z
  // Format: YYYY[-MM[-DD[[T| ]HH[:mm[:ss[.SSS]]]]]][Z]
  const isoRegex =
    /^(\d{4})(?:-(\d{2})(?:-(\d{2})(?:[T ](\d{2})(?::(\d{2})(?::(\d{2})(?:\.(\d{1,3}))?)?)?)?)?)?Z?$/;
  const match = isoRegex.exec(dateString.trim());

  if (!match) {
    throw new Error(
      `Invalid date format: expected ISO 8601 format`,
    );
  }

  const year = parseInt(match[1], 10);
  const month = match[2] ? parseInt(match[2], 10) : 1;
  const day = match[3] ? parseInt(match[3], 10) : 1;
  const hour = match[4] ? parseInt(match[4], 10) : 0;
  const minute = match[5] ? parseInt(match[5], 10) : 0;
  const second = match[6] ? parseInt(match[6], 10) : 0;
  const millisecond = match[7] ? parseInt(match[7].padEnd(3, '0'), 10) : 0;

  // Validate year
  if (year < 1000 || year > 3999) {
    throw new Error(`Invalid year: must be between 1000 and 3999, got ${year}`);
  }

  // Validate month
  if (month < 1 || month > 12) {
    throw new Error(`Invalid month: must be between 1 and 12, got ${month}`);
  }

  // Validate day
  if (day < 1 || day > 31) {
    throw new Error(`Invalid day: must be between 1 and 31, got ${day}`);
  }

  // Validate time components
  if (hour > 23) {
    throw new Error(`Invalid hour: must be between 0 and 23, got ${hour}`);
  }
  if (minute > 59) {
    throw new Error(`Invalid minute: must be between 0 and 59, got ${minute}`);
  }
  if (second > 59) {
    throw new Error(`Invalid second: must be between 0 and 59, got ${second}`);
  }

  // Create UTC date
  const value = Date.UTC(year, month - 1, day, hour, minute, second, millisecond);
  if (isNaN(value)) {
    throw new Error(`Invalid date: the date components do not form a valid date`);
  }

  return new Date(value);
}
