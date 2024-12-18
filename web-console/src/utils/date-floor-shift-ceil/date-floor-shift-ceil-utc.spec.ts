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

import { shifters } from './date-floor-shift-ceil';

function pairwise<T>(array: T[], callback: (t1: T, t2: T) => void) {
  for (let i = 0; i < array.length - 1; i++) {
    callback(array[i], array[i + 1]);
  }
}

describe('floor, shift, ceil (UTC)', () => {
  const tz = 'Etc/UTC';

  it('moves seconds', () => {
    const dates: Date[] = [
      new Date('2012-11-04T00:00:00Z'),
      new Date('2012-11-04T00:00:03Z'),
      new Date('2012-11-04T00:00:06Z'),
      new Date('2012-11-04T00:00:09Z'),
      new Date('2012-11-04T00:00:12Z'),
    ];
    pairwise(dates, (d1, d2) => expect(shifters.second.shift(d1, tz, 3)).toEqual(d2));
  });

  it('rounds minutes', () => {
    expect(shifters.minute.round(new Date('2012-11-04T00:29:00Z'), 15, tz)).toEqual(
      new Date('2012-11-04T00:15:00Z'),
    );

    expect(shifters.minute.round(new Date('2012-11-04T00:29:00Z'), 4, tz)).toEqual(
      new Date('2012-11-04T00:28:00Z'),
    );
  });

  it('moves minutes', () => {
    const dates: Date[] = [
      new Date('2012-11-04T00:00:00Z'),
      new Date('2012-11-04T00:03:00Z'),
      new Date('2012-11-04T00:06:00Z'),
      new Date('2012-11-04T00:09:00Z'),
      new Date('2012-11-04T00:12:00Z'),
    ];
    pairwise(dates, (d1, d2) => expect(shifters.minute.shift(d1, tz, 3)).toEqual(d2));
  });

  it('floors hour correctly', () => {
    expect(shifters.hour.floor(new Date('2012-11-04T00:30:00Z'), tz)).toEqual(
      new Date('2012-11-04T00:00:00Z'),
    );

    expect(shifters.hour.floor(new Date('2012-11-04T01:30:00Z'), tz)).toEqual(
      new Date('2012-11-04T01:00:00Z'),
    );

    expect(shifters.hour.floor(new Date('2012-11-04T01:30:00Z'), tz)).toEqual(
      new Date('2012-11-04T01:00:00Z'),
    );

    expect(shifters.hour.floor(new Date('2012-11-04T02:30:00Z'), tz)).toEqual(
      new Date('2012-11-04T02:00:00Z'),
    );

    expect(shifters.hour.floor(new Date('2012-11-04T03:30:00Z'), tz)).toEqual(
      new Date('2012-11-04T03:00:00Z'),
    );
  });

  it('moves hour', () => {
    const dates: Date[] = [
      new Date('2012-11-04T00:00:00Z'),
      new Date('2012-11-04T01:00:00Z'),
      new Date('2012-11-04T02:00:00Z'),
      new Date('2012-11-04T03:00:00Z'),
    ];
    pairwise(dates, (d1, d2) => expect(shifters.hour.shift(d1, tz, 1)).toEqual(d2));
  });

  it('moves day', () => {
    const dates: Date[] = [
      new Date('2012-11-03T00:00:00Z'),
      new Date('2012-11-04T00:00:00Z'),
      new Date('2012-11-05T00:00:00Z'),
      new Date('2012-11-06T00:00:00Z'),
    ];
    pairwise(dates, (d1, d2) => expect(shifters.day.shift(d1, tz, 1)).toEqual(d2));
  });

  it('ceils day', () => {
    let d1 = new Date('2014-12-11T22:11:57.469Z');
    let d2 = new Date('2014-12-12T00:00:00.000Z');
    expect(shifters.day.ceil(d1, tz)).toEqual(d2);

    d1 = new Date('2014-12-08T00:00:00.000Z');
    d2 = new Date('2014-12-08T00:00:00.000Z');
    expect(shifters.day.ceil(d1, tz)).toEqual(d2);
  });

  it('moves week', () => {
    const dates: Date[] = [
      new Date('2012-10-29T00:00:00Z'),
      new Date('2012-11-05T00:00:00Z'),
      new Date('2012-11-12T00:00:00Z'),
      new Date('2012-11-19T00:00:00Z'),
    ];
    pairwise(dates, (d1, d2) => expect(shifters.week.shift(d1, tz, 1)).toEqual(d2));
  });

  it('floors week correctly', () => {
    let d1 = new Date('2014-12-11T22:11:57.469Z');
    let d2 = new Date('2014-12-08T00:00:00.000Z');
    expect(shifters.week.floor(d1, tz)).toEqual(d2);

    d1 = new Date('2014-12-07T12:11:57.469Z');
    d2 = new Date('2014-12-01T00:00:00.000Z');
    expect(shifters.week.floor(d1, tz)).toEqual(d2);
  });

  it('ceils week correctly', () => {
    let d1 = new Date('2014-12-11T22:11:57.469Z');
    let d2 = new Date('2014-12-15T00:00:00.000Z');
    expect(shifters.week.ceil(d1, tz)).toEqual(d2);

    d1 = new Date('2014-12-07T12:11:57.469Z');
    d2 = new Date('2014-12-08T00:00:00.000Z');
    expect(shifters.week.ceil(d1, tz)).toEqual(d2);
  });

  it('moves month', () => {
    const dates: Date[] = [
      new Date('2012-11-01T00:00:00Z'),
      new Date('2012-12-01T00:00:00Z'),
      new Date('2013-01-01T00:00:00Z'),
      new Date('2013-02-01T00:00:00Z'),
    ];
    pairwise(dates, (d1, d2) => expect(shifters.month.shift(d1, tz, 1)).toEqual(d2));
  });

  it('shifts month on the 31st', () => {
    const d1 = new Date('2016-03-31T00:00:00.000Z');
    const d2 = new Date('2016-05-01T00:00:00.000Z');
    expect(shifters.month.shift(d1, tz, 1)).toEqual(d2);
  });

  it('moves year', () => {
    const dates: Date[] = [
      new Date('2010-01-01T00:00:00Z'),
      new Date('2011-01-01T00:00:00Z'),
      new Date('2012-01-01T00:00:00Z'),
      new Date('2013-01-01T00:00:00Z'),
    ];
    pairwise(dates, (d1, d2) => expect(shifters.year.shift(d1, tz, 1)).toEqual(d2));
  });
});
