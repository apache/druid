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

import { fromDate, toTimeZone } from '@internationalized/date';
import type { Timezone } from 'chronoshift';
import { day, Duration, hour, minute, month, second, week, year } from 'chronoshift';
import { bisector, tickStep } from 'd3-array';
import { utcFormat } from 'd3-time-format';

// ---------------------------------------
// Tick formatting

const formatMillisecond = utcFormat('.%L');
const formatSecond = utcFormat(':%S');
const formatMinute = utcFormat('%I:%M');
const formatHour = utcFormat('%I %p');
const formatDay = utcFormat('%a %d');
const formatWeek = utcFormat('%b %d');
const formatMonth = utcFormat('%B');
const formatYear = utcFormat('%Y');

/*
 * Format ticks with the timezone provided
 * Originally based on https://github.com/d3/d3-scale/blob/v3.3.0/src/time.js#L29
 */
export function tickFormatWithTimezone(date: Date, timezone: Timezone): string {
  if (second.floor(date, timezone) < date) return formatMillisecond(date);
  if (minute.floor(date, timezone) < date) return formatSecond(date);

  const offsetDate = offsetDateToTimezone(date, timezone);
  if (hour.floor(date, timezone) < date) return formatMinute(offsetDate);
  if (day.floor(date, timezone) < date) return formatHour(offsetDate);
  if (month.floor(date, timezone) < date) {
    return week.floor(date, timezone) < date ? formatDay(offsetDate) : formatWeek(offsetDate);
  }
  if (year.floor(date, timezone) < date) return formatMonth(offsetDate);
  return formatYear(offsetDate);
}

function offsetDateToTimezone(date: Date, timezone: Timezone): Date {
  const zonedDate = toTimeZone(fromDate(date, 'Etc/UTC'), timezone.toString());
  return new Date(date.valueOf() + zonedDate.offset);
}

// ---------------------------------------
// Tick calculation

/*
 * Calculates the ticks in a manner identical to D3's default tick calculator but in a timezone aware manner
 * Originally based on https://github.com/d3/d3-time/blob/v3.1.0/src/ticks.js#L35
 */
export function timezoneAwareTicks(
  start: Date,
  end: Date,
  count: number,
  timezone: Timezone,
): Date[] {
  if (end < start) throw new Error('start must come before end end');
  const interval = tickInterval(start, end, count);
  if (!interval) return [];
  return interval.materialize(start, end, timezone);
}

const durationSecond = new Duration('PT1S');
const durationMinute = new Duration('PT1M');
const durationHour = new Duration('PT1H');
const durationDay = new Duration('P1D');
const durationWeek = new Duration('P1W');
const durationMonth = new Duration('P1M');
const durationYear = new Duration('P1Y');
const tickIntervals: [Duration, number, number][] = [
  [durationSecond, 1, second.canonicalLength],
  [durationSecond, 5, 5 * second.canonicalLength],
  [durationSecond, 15, 15 * second.canonicalLength],
  [durationSecond, 30, 30 * second.canonicalLength],
  [durationMinute, 1, minute.canonicalLength],
  [durationMinute, 5, 5 * minute.canonicalLength],
  [durationMinute, 15, 15 * minute.canonicalLength],
  [durationMinute, 30, 30 * minute.canonicalLength],
  [durationHour, 1, hour.canonicalLength],
  [durationHour, 3, 3 * hour.canonicalLength],
  [durationHour, 6, 6 * hour.canonicalLength],
  [durationHour, 12, 12 * hour.canonicalLength],
  [durationDay, 1, day.canonicalLength],
  [durationDay, 2, 2 * day.canonicalLength],
  [durationWeek, 1, week.canonicalLength],
  [durationMonth, 1, month.canonicalLength],
  [durationMonth, 3, 3 * month.canonicalLength],
  [durationYear, 1, year.canonicalLength],
];

/*
 * Calculates the distance between adjacent ticks
 * Originally based on https://github.com/d3/d3-time/blob/v3.1.0/src/ticks.js#L43
 * It is basically the same function except it returns a Duration which is timezone aware
 */
function tickInterval(start: Date, end: Date, count: number): Duration {
  const target = Math.abs(end.valueOf() - start.valueOf()) / count;
  const i = bisector(([, , step]) => step).right(tickIntervals, target);
  if (i === tickIntervals.length)
    return durationYear.multiply(
      tickStep(start.valueOf() / year.canonicalLength, end.valueOf() / year.canonicalLength, count),
    );
  // Original line uses milliseconds: if (i === 0) return millisecond.every(Math.max(tickStep(start, end, count), 1));
  if (i === 0)
    return durationSecond.multiply(
      Math.max(tickStep(start.valueOf() / 1000, end.valueOf() / 1000, count), 1),
    );
  const [t, step] =
    tickIntervals[target / tickIntervals[i - 1][2] < tickIntervals[i][2] / target ? i - 1 : i];
  return t.multiply(step);
}
