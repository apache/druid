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
  ceilToUtcDay,
  dateToIsoDateString,
  intervalToLocalDateRange,
  localDateRangeToInterval,
  localToUtcDate,
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

  describe('ceilToUtcDay', () => {
    it('works', () => {
      expect(ceilToUtcDay(new Date('2021-02-03T12:03:02.001Z'))).toEqual(
        new Date('2021-02-04T00:00:00Z'),
      );
    });
  });
});
