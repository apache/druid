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

import {
  dateToIsoDateString,
  intervalToLocalDateRange,
  localDateRangeToInterval,
  localToUtcDate,
  parseIsoDate,
  utcToLocalDate,
} from './date';

describe('date', () => {
  describe('dateToIsoDateString', () => {
    it('works', () => {
      expect(dateToIsoDateString(new Date('2021-02-03T12:00:00Z'))).toEqual('2021-02-03');
    });
  });

  describe('utcToLocalDate / localToUtcDate', () => {
    it('works', () => {
      const date = new Date('2021-02-03T12:00:00Z');

      expect(localToUtcDate(utcToLocalDate(date))).toEqual(date);
      expect(utcToLocalDate(localToUtcDate(date))).toEqual(date);
    });
  });

  describe('intervalToLocalDateRange / localDateRangeToInterval', () => {
    it('works with full interval', () => {
      const interval = '2021-02-03T12:00:00/2021-03-03T12:00:00';

      expect(localDateRangeToInterval(intervalToLocalDateRange(interval))).toEqual(interval);
    });

    it('works with start only', () => {
      const interval = '2021-02-03T12:00:00/';

      expect(localDateRangeToInterval(intervalToLocalDateRange(interval))).toEqual(interval);
    });

    it('works with end only', () => {
      const interval = '/2021-02-03T12:00:00';

      expect(localDateRangeToInterval(intervalToLocalDateRange(interval))).toEqual(interval);
    });
  });

  describe('parseIsoDate', () => {
    it('works with year only', () => {
      const result = parseIsoDate('2016');
      expect(result).toEqual(new Date(Date.UTC(2016, 0, 1, 0, 0, 0, 0)));
    });

    it('works with year-month', () => {
      const result = parseIsoDate('2016-06');
      expect(result).toEqual(new Date(Date.UTC(2016, 5, 1, 0, 0, 0, 0)));
    });

    it('works with date only', () => {
      const result = parseIsoDate('2016-06-20');
      expect(result).toEqual(new Date(Date.UTC(2016, 5, 20, 0, 0, 0, 0)));
    });

    it('works with date and hour using T separator', () => {
      const result = parseIsoDate('2016-06-20T21');
      expect(result).toEqual(new Date(Date.UTC(2016, 5, 20, 21, 0, 0, 0)));
    });

    it('works with date and hour using space separator', () => {
      const result = parseIsoDate('2016-06-20 21');
      expect(result).toEqual(new Date(Date.UTC(2016, 5, 20, 21, 0, 0, 0)));
    });

    it('works with date, hour, and minute using T separator', () => {
      const result = parseIsoDate('2016-06-20T21:31');
      expect(result).toEqual(new Date(Date.UTC(2016, 5, 20, 21, 31, 0, 0)));
    });

    it('works with date, hour, and minute using space separator', () => {
      const result = parseIsoDate('2016-06-20 21:31');
      expect(result).toEqual(new Date(Date.UTC(2016, 5, 20, 21, 31, 0, 0)));
    });

    it('works with datetime without milliseconds using T separator', () => {
      const result = parseIsoDate('2016-06-20T21:31:02');
      expect(result).toEqual(new Date(Date.UTC(2016, 5, 20, 21, 31, 2, 0)));
    });

    it('works with datetime without milliseconds using space separator', () => {
      const result = parseIsoDate('2016-06-20 21:31:02');
      expect(result).toEqual(new Date(Date.UTC(2016, 5, 20, 21, 31, 2, 0)));
    });

    it('works with full datetime with milliseconds using T separator', () => {
      const result = parseIsoDate('2016-06-20T21:31:02.123');
      expect(result).toEqual(new Date(Date.UTC(2016, 5, 20, 21, 31, 2, 123)));
    });

    it('works with full datetime with milliseconds using space separator', () => {
      const result = parseIsoDate('2016-06-20 21:31:02.123');
      expect(result).toEqual(new Date(Date.UTC(2016, 5, 20, 21, 31, 2, 123)));
    });

    it('works with single digit milliseconds', () => {
      const result = parseIsoDate('2016-06-20T21:31:02.1');
      expect(result).toEqual(new Date(Date.UTC(2016, 5, 20, 21, 31, 2, 100)));
    });

    it('works with two digit milliseconds', () => {
      const result = parseIsoDate('2016-06-20T21:31:02.12');
      expect(result).toEqual(new Date(Date.UTC(2016, 5, 20, 21, 31, 2, 120)));
    });

    it('works with whitespace trimming', () => {
      const result = parseIsoDate('  2016-06-20T21:31:02  ');
      expect(result).toEqual(new Date(Date.UTC(2016, 5, 20, 21, 31, 2, 0)));
    });

    it('works with trailing Z', () => {
      const result = parseIsoDate('2016-06-20T21:31:02Z');
      expect(result).toEqual(new Date(Date.UTC(2016, 5, 20, 21, 31, 2, 0)));
    });

    it('works with trailing Z and milliseconds', () => {
      const result = parseIsoDate('2016-06-20T21:31:02.123Z');
      expect(result).toEqual(new Date(Date.UTC(2016, 5, 20, 21, 31, 2, 123)));
    });

    it('works with date only and trailing Z', () => {
      const result = parseIsoDate('2016-06-20Z');
      expect(result).toEqual(new Date(Date.UTC(2016, 5, 20, 0, 0, 0, 0)));
    });

    it('throws error for nonsense format with multiple T separators', () => {
      expect(() => parseIsoDate('2016T06T20T21T31T02T000')).toThrow(
        'Invalid date format: expected ISO 8601 format',
      );
    });

    it('throws error for invalid year below range', () => {
      expect(() => parseIsoDate('0999-06-20')).toThrow(
        'Invalid year: must be between 1000 and 3999',
      );
    });

    it('throws error for invalid year above range', () => {
      expect(() => parseIsoDate('4000-06-20')).toThrow(
        'Invalid year: must be between 1000 and 3999',
      );
    });

    it('throws error for invalid month below range', () => {
      expect(() => parseIsoDate('2016-00-20')).toThrow('Invalid month: must be between 1 and 12');
    });

    it('throws error for invalid month above range', () => {
      expect(() => parseIsoDate('2016-13-20')).toThrow('Invalid month: must be between 1 and 12');
    });

    it('throws error for invalid day below range', () => {
      expect(() => parseIsoDate('2016-06-00')).toThrow('Invalid day: must be between 1 and 31');
    });

    it('throws error for invalid day above range', () => {
      expect(() => parseIsoDate('2016-06-32')).toThrow('Invalid day: must be between 1 and 31');
    });

    it('throws error for invalid hour', () => {
      expect(() => parseIsoDate('2016-06-20 25:00:00')).toThrow(
        'Invalid hour: must be between 0 and 23',
      );
    });

    it('throws error for invalid minute', () => {
      expect(() => parseIsoDate('2016-06-20 21:60:00')).toThrow(
        'Invalid minute: must be between 0 and 59',
      );
    });

    it('throws error for invalid second', () => {
      expect(() => parseIsoDate('2016-06-20 21:31:60')).toThrow(
        'Invalid second: must be between 0 and 59',
      );
    });

    it('throws error for slash separators', () => {
      expect(() => parseIsoDate('2016/06/20')).toThrow('Invalid date format');
    });

    it('throws error for completely invalid string', () => {
      expect(() => parseIsoDate('not-a-date')).toThrow('Invalid date format');
    });

    it('throws error for empty string', () => {
      expect(() => parseIsoDate('')).toThrow('Invalid date format');
    });
  });
});
