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

import { fromDate, startOfWeek } from '@internationalized/date';

export type AlignFn = (dt: Date, tz: string) => Date;

export type ShiftFn = (dt: Date, tz: string, step: number) => Date;

export type RoundFn = (dt: Date, roundTo: number, tz: string) => Date;

export interface TimeShifterNoCeil {
  canonicalLength: number;
  siblings?: number;
  floor: AlignFn;
  round: RoundFn;
  shift: ShiftFn;
}

export interface TimeShifter extends TimeShifterNoCeil {
  ceil: AlignFn;
}

function isUTC(tz: string): boolean {
  return tz === 'Etc/UTC';
}

function adjustDay(day: number): number {
  return (day + 6) % 7;
}

function floorTo(n: number, roundTo: number): number {
  return Math.floor(n / roundTo) * roundTo;
}

function timeShifterFiller(tm: TimeShifterNoCeil): TimeShifter {
  const { floor, shift } = tm;
  return {
    ...tm,
    ceil: (dt: Date, tz: string) => {
      const floored = floor(dt, tz);
      if (floored.valueOf() === dt.valueOf()) return dt; // Just like ceil(3) is 3 and not 4
      return shift(floored, tz, 1);
    },
  };
}

export const second = timeShifterFiller({
  canonicalLength: 1000,
  siblings: 60,
  floor: (dt, _tz) => {
    // Seconds do not actually need a timezone because all timezones align on seconds... for now...
    dt = new Date(dt.valueOf());
    dt.setUTCMilliseconds(0);
    return dt;
  },
  round: (dt, roundTo, _tz) => {
    const cur = dt.getUTCSeconds();
    const adj = floorTo(cur, roundTo);
    if (cur !== adj) dt.setUTCSeconds(adj);
    return dt;
  },
  shift: (dt, _tz, step) => {
    dt = new Date(dt.valueOf());
    dt.setUTCSeconds(dt.getUTCSeconds() + step);
    return dt;
  },
});

export const minute = timeShifterFiller({
  canonicalLength: 60000,
  siblings: 60,
  floor: (dt, _tz) => {
    // Minutes do not actually need a timezone because all timezones align on minutes... for now...
    dt = new Date(dt.valueOf());
    dt.setUTCSeconds(0, 0);
    return dt;
  },
  round: (dt, roundTo, _tz) => {
    const cur = dt.getUTCMinutes();
    const adj = floorTo(cur, roundTo);
    if (cur !== adj) dt.setUTCMinutes(adj);
    return dt;
  },
  shift: (dt, _tz, step) => {
    dt = new Date(dt.valueOf());
    dt.setUTCMinutes(dt.getUTCMinutes() + step);
    return dt;
  },
});

// Movement by hour is tz independent because in every timezone an hour is 60 min
function hourMove(dt: Date, _tz: string, step: number) {
  dt = new Date(dt.valueOf());
  dt.setUTCHours(dt.getUTCHours() + step);
  return dt;
}

export const hour = timeShifterFiller({
  canonicalLength: 3600000,
  siblings: 24,
  floor: (dt, tz) => {
    if (isUTC(tz)) {
      dt = new Date(dt.valueOf());
      dt.setUTCMinutes(0, 0, 0);
      return dt;
    } else {
      return fromDate(dt, tz).set({ second: 0, minute: 0, millisecond: 0 }).toDate();
    }
  },
  round: (dt, roundTo, tz) => {
    if (isUTC(tz)) {
      const cur = dt.getUTCHours();
      const adj = floorTo(cur, roundTo);
      if (cur !== adj) dt.setUTCHours(adj);
    } else {
      const cur = fromDate(dt, tz).hour;
      const adj = floorTo(cur, roundTo);
      if (cur !== adj) return hourMove(dt, tz, adj - cur);
    }
    return dt;
  },
  shift: hourMove,
});

export const day = timeShifterFiller({
  canonicalLength: 24 * 3600000,
  floor: (dt, tz) => {
    if (isUTC(tz)) {
      dt = new Date(dt.valueOf());
      dt.setUTCHours(0, 0, 0, 0);
      return dt;
    } else {
      return fromDate(dt, tz).set({ hour: 0, second: 0, minute: 0, millisecond: 0 }).toDate();
    }
  },
  shift: (dt, tz, step) => {
    if (isUTC(tz)) {
      dt = new Date(dt.valueOf());
      dt.setUTCDate(dt.getUTCDate() + step);
      return dt;
    } else {
      return fromDate(dt, tz).add({ days: step }).toDate();
    }
  },
  round: () => {
    throw new Error('missing day round');
  },
});

export const week = timeShifterFiller({
  canonicalLength: 7 * 24 * 3600000,
  floor: (dt, tz) => {
    if (isUTC(tz)) {
      dt = new Date(dt.valueOf());
      dt.setUTCHours(0, 0, 0, 0);
      dt.setUTCDate(dt.getUTCDate() - adjustDay(dt.getUTCDay()));
    } else {
      const zd = fromDate(dt, tz);
      return startOfWeek(
        zd.set({ hour: 0, second: 0, minute: 0, millisecond: 0 }),
        'fr-FR', // We want the week to start on Monday
      ).toDate();
    }
    return dt;
  },
  shift: (dt, tz, step) => {
    if (isUTC(tz)) {
      dt = new Date(dt.valueOf());
      dt.setUTCDate(dt.getUTCDate() + step * 7);
      return dt;
    } else {
      return fromDate(dt, tz).add({ weeks: step }).toDate();
    }
  },
  round: () => {
    throw new Error('missing week round');
  },
});

function monthShift(dt: Date, tz: string, step: number) {
  if (isUTC(tz)) {
    dt = new Date(dt.valueOf());
    dt.setUTCMonth(dt.getUTCMonth() + step);
    return dt;
  } else {
    return fromDate(dt, tz).add({ months: step }).toDate();
  }
}

export const month = timeShifterFiller({
  canonicalLength: 30 * 24 * 3600000,
  siblings: 12,
  floor: (dt, tz) => {
    if (isUTC(tz)) {
      dt = new Date(dt.valueOf());
      dt.setUTCHours(0, 0, 0, 0);
      dt.setUTCDate(1);
      return dt;
    } else {
      return fromDate(dt, tz)
        .set({ day: 1, hour: 0, second: 0, minute: 0, millisecond: 0 })
        .toDate();
    }
  },
  round: (dt, roundTo, tz) => {
    if (isUTC(tz)) {
      const cur = dt.getUTCMonth();
      const adj = floorTo(cur, roundTo);
      if (cur !== adj) dt.setUTCMonth(adj);
    } else {
      const cur = fromDate(dt, tz).month - 1; // Needs to be zero indexed
      const adj = floorTo(cur, roundTo);
      if (cur !== adj) return monthShift(dt, tz, adj - cur);
    }
    return dt;
  },
  shift: monthShift,
});

function yearShift(dt: Date, tz: string, step: number) {
  if (isUTC(tz)) {
    dt = new Date(dt.valueOf());
    dt.setUTCFullYear(dt.getUTCFullYear() + step);
    return dt;
  } else {
    return fromDate(dt, tz).add({ years: step }).toDate();
  }
}

export const year = timeShifterFiller({
  canonicalLength: 365 * 24 * 3600000,
  siblings: 1000,
  floor: (dt, tz) => {
    if (isUTC(tz)) {
      dt = new Date(dt.valueOf());
      dt.setUTCHours(0, 0, 0, 0);
      dt.setUTCMonth(0, 1);
      return dt;
    } else {
      return fromDate(dt, tz)
        .set({ month: 1, day: 1, hour: 0, second: 0, minute: 0, millisecond: 0 })
        .toDate();
    }
  },
  round: (dt, roundTo, tz) => {
    if (isUTC(tz)) {
      const cur = dt.getUTCFullYear();
      const adj = floorTo(cur, roundTo);
      if (cur !== adj) dt.setUTCFullYear(adj);
    } else {
      const cur = fromDate(dt, tz).year;
      const adj = floorTo(cur, roundTo);
      if (cur !== adj) return yearShift(dt, tz, adj - cur);
    }
    return dt;
  },
  shift: yearShift,
});

export interface Shifters {
  second: TimeShifter;
  minute: TimeShifter;
  hour: TimeShifter;
  day: TimeShifter;
  week: TimeShifter;
  month: TimeShifter;
  year: TimeShifter;

  [key: string]: TimeShifter;
}

export const shifters: Shifters = {
  second,
  minute,
  hour,
  day,
  week,
  month,
  year,
};
