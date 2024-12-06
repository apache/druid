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

import { second, shifters } from '../date-floor-shift-ceil/date-floor-shift-ceil';
import { capitalizeFirst, pluralIfNeeded } from '../general';

export const TZ_UTC = 'Etc/UTC';

export type DurationSpan = 'year' | 'month' | 'week' | 'day' | 'hour' | 'minute' | 'second';

const SPANS_WITH_WEEK: DurationSpan[] = [
  'year',
  'month',
  'week',
  'day',
  'hour',
  'minute',
  'second',
];
const SPANS_WITHOUT_WEEK: DurationSpan[] = ['year', 'month', 'day', 'hour', 'minute', 'second'];
const SPANS_WITHOUT_WEEK_OR_MONTH: DurationSpan[] = ['year', 'day', 'hour', 'minute', 'second'];
const SPANS_UP_TO_DAY: DurationSpan[] = ['day', 'hour', 'minute', 'second'];

export type DurationValue = Partial<Record<DurationSpan, number>>;

const periodWeekRegExp = /^P(\d+)W$/;
const periodRegExp = /^P(?:(\d+)Y)?(?:(\d+)M)?(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?$/;
//                     P   (year ) (month   ) (day     )    T(hour    ) (minute  ) (second  )

function getSpansFromString(durationStr: string): DurationValue {
  const spans: DurationValue = {};
  let matches: RegExpExecArray | null;
  if ((matches = periodWeekRegExp.exec(durationStr))) {
    spans.week = Number(matches[1]);
    if (!spans.week) throw new Error('Duration can not have empty weeks');
  } else if ((matches = periodRegExp.exec(durationStr))) {
    const nums = matches.map(Number);
    for (let i = 0; i < SPANS_WITHOUT_WEEK.length; i++) {
      const span = SPANS_WITHOUT_WEEK[i];
      const value = nums[i + 1];
      if (value) spans[span] = value;
    }
  } else {
    throw new Error("Can not parse duration '" + durationStr + "'");
  }
  return spans;
}

function getSpansFromStartEnd(start: Date, end: Date, timezone: string): DurationValue {
  start = second.floor(start, timezone);
  end = second.floor(end, timezone);
  if (end <= start) throw new Error('start must come before end');

  const spans: DurationValue = {};
  let iterator: Date = start;
  for (let i = 0; i < SPANS_WITHOUT_WEEK.length; i++) {
    const span = SPANS_WITHOUT_WEEK[i];
    let spanCount = 0;

    // Shortcut
    const length = end.valueOf() - iterator.valueOf();
    const canonicalLength: number = shifters[span].canonicalLength;
    if (length < canonicalLength / 4) continue;
    const numberToFit = Math.min(0, Math.floor(length / canonicalLength) - 1);
    let iteratorMove: Date;
    if (numberToFit > 0) {
      // try to skip by numberToFit
      iteratorMove = shifters[span].shift(iterator, timezone, numberToFit);
      if (iteratorMove <= end) {
        spanCount += numberToFit;
        iterator = iteratorMove;
      }
    }

    while (true) {
      iteratorMove = shifters[span].shift(iterator, timezone, 1);
      if (iteratorMove <= end) {
        iterator = iteratorMove;
        spanCount++;
      } else {
        break;
      }
    }

    if (spanCount) {
      spans[span] = spanCount;
    }
  }
  return spans;
}

function removeZeros(spans: DurationValue): DurationValue {
  const newSpans: DurationValue = {};
  for (let i = 0; i < SPANS_WITH_WEEK.length; i++) {
    const span = SPANS_WITH_WEEK[i];
    if (Number(spans[span]) > 0) {
      newSpans[span] = spans[span];
    }
  }
  return newSpans;
}

function fitIntoSpans(length: number, spansToCheck: DurationSpan[]): DurationValue {
  const spans: DurationValue = {};

  let lengthLeft = length;
  for (let i = 0; i < spansToCheck.length; i++) {
    const span = spansToCheck[i];
    const spanLength = shifters[span].canonicalLength;
    const count = Math.floor(lengthLeft / spanLength);

    if (count) {
      lengthLeft -= spanLength * count;
      spans[span] = count;
    }
  }

  return spans;
}

/**
 * Represents an ISO duration like P1DT3H
 */
export class Duration {
  public readonly singleSpan?: DurationSpan;
  public readonly spans: Readonly<DurationValue>;

  static fromCanonicalLength(length: number, skipMonths = false): Duration {
    if (length <= 0) throw new Error('length must be positive');
    let spans = fitIntoSpans(length, skipMonths ? SPANS_WITHOUT_WEEK_OR_MONTH : SPANS_WITHOUT_WEEK);

    if (
      length % shifters['week'].canonicalLength === 0 && // Weeks fits
      (Object.keys(spans).length > 1 || // We already have a more complex span
        spans['day']) // or... we only have days and it might be simpler to express as weeks
    ) {
      spans = { week: length / shifters['week'].canonicalLength };
    }

    return new Duration(spans);
  }

  static fromCanonicalLengthUpToDays(length: number): Duration {
    if (length <= 0) throw new Error('length must be positive');
    return new Duration(fitIntoSpans(length, SPANS_UP_TO_DAY));
  }

  static fromRange(start: Date, end: Date, timezone: string): Duration {
    return new Duration(getSpansFromStartEnd(start, end, timezone));
  }

  static pickSmallestGranularityThatFits(
    granularities: Duration[],
    span: number,
    maxEntities: number,
  ): Duration {
    for (const granularity of granularities) {
      if (span / granularity.getCanonicalLength() < maxEntities) return granularity;
    }
    return granularities[granularities.length - 1];
  }

  constructor(spans: DurationValue | string) {
    const effectiveSpans: DurationValue =
      typeof spans === 'string' ? getSpansFromString(spans) : removeZeros(spans);

    const usedSpans = Object.keys(effectiveSpans) as DurationSpan[];
    if (!usedSpans.length) throw new Error('Duration can not be empty');
    if (usedSpans.length === 1) {
      this.singleSpan = usedSpans[0];
    } else if (effectiveSpans.week) {
      throw new Error("Can not mix 'week' and other spans");
    }
    this.spans = effectiveSpans;
  }

  public toString() {
    const strArr: string[] = ['P'];
    const spans = this.spans;
    if (spans.week) {
      strArr.push(String(spans.week), 'W');
    } else {
      let addedT = false;
      for (let i = 0; i < SPANS_WITHOUT_WEEK.length; i++) {
        const span = SPANS_WITHOUT_WEEK[i];
        const value = spans[span];
        if (!value) continue;
        if (!addedT && i >= 3) {
          strArr.push('T');
          addedT = true;
        }
        strArr.push(String(value), span[0].toUpperCase());
      }
    }
    return strArr.join('');
  }

  public add(duration: Duration): Duration {
    return Duration.fromCanonicalLength(this.getCanonicalLength() + duration.getCanonicalLength());
  }

  public subtract(duration: Duration): Duration {
    const newCanonicalDuration = this.getCanonicalLength() - duration.getCanonicalLength();
    if (newCanonicalDuration < 0) throw new Error('A duration can not be negative.');
    return Duration.fromCanonicalLength(newCanonicalDuration);
  }

  public multiply(multiplier: number): Duration {
    if (multiplier <= 0) throw new Error('Multiplier must be positive non-zero');
    if (multiplier === 1) return this;
    const newCanonicalDuration = this.getCanonicalLength() * multiplier;
    return Duration.fromCanonicalLength(newCanonicalDuration);
  }

  public valueOf() {
    return this.spans;
  }

  public equals(other: Duration | undefined): boolean {
    return other instanceof Duration && this.toString() === other.toString();
  }

  public isSimple(): boolean {
    const { singleSpan } = this;
    if (!singleSpan) return false;
    return this.spans[singleSpan] === 1;
  }

  public isFloorable(): boolean {
    const { singleSpan } = this;
    if (!singleSpan) return false;
    const span = Number(this.spans[singleSpan]);
    if (span === 1) return true;
    const { siblings } = shifters[singleSpan];
    if (!siblings) return false;
    return siblings % span === 0;
  }

  /**
   * Floors the date according to this duration.
   * @param date The date to floor
   * @param timezone The timezone within which to floor
   */
  public floor(date: Date, timezone: string): Date {
    const { singleSpan } = this;
    if (!singleSpan) throw new Error('Can not floor on a complex duration');
    const span = this.spans[singleSpan]!;
    const mover = shifters[singleSpan];
    let dt = mover.floor(date, timezone);
    if (span !== 1) {
      if (!mover.siblings) {
        throw new Error(`Can not floor on a ${singleSpan} duration that is not 1`);
      }
      if (mover.siblings % span !== 0) {
        throw new Error(
          `Can not floor on a ${singleSpan} duration that does not divide into ${mover.siblings}`,
        );
      }
      dt = mover.round(dt, span, timezone);
    }
    return dt;
  }

  /**
   * Moves the given date by 'step' times of the duration
   * Negative step value will move back in time.
   * @param date The date to move
   * @param timezone The timezone within which to make the move
   * @param step The number of times to step by the duration
   */
  public shift(date: Date, timezone: string, step = 1): Date {
    const spans = this.spans;
    for (const span of SPANS_WITH_WEEK) {
      const value = spans[span];
      if (value) date = shifters[span].shift(date, timezone, step * value);
    }
    return date;
  }

  public ceil(date: Date, timezone: string): Date {
    const floored = this.floor(date, timezone);
    if (floored.valueOf() === date.valueOf()) return date; // Just like ceil(3) is 3 and not 4
    return this.shift(floored, timezone, 1);
  }

  public round(date: Date, timezone: string): Date {
    const floorDate = this.floor(date, timezone);
    const ceilDate = this.ceil(date, timezone);
    const distanceToFloor = Math.abs(date.valueOf() - floorDate.valueOf());
    const distanceToCeil = Math.abs(date.valueOf() - ceilDate.valueOf());
    return distanceToFloor < distanceToCeil ? floorDate : ceilDate;
  }

  /**
   * Materializes all the values of this duration form start to end
   * @param start The date to start on
   * @param end The date to start on
   * @param timezone The timezone within which to materialize
   * @param step The number of times to step by the duration
   */
  public materialize(start: Date, end: Date, timezone: string, step = 1): Date[] {
    const values: Date[] = [];
    let iter = this.floor(start, timezone);
    while (iter <= end) {
      values.push(iter);
      iter = this.shift(iter, timezone, step);
    }
    return values;
  }

  /**
   * Checks to see if date is aligned to this duration within the timezone (floors to itself)
   * @param date The date to check
   * @param timezone The timezone within which to make the check
   */
  public isAligned(date: Date, timezone: string): boolean {
    return this.floor(date, timezone).valueOf() === date.valueOf();
  }

  /**
   * Check to see if this duration can be divided by the given duration
   * @param smaller The smaller duration to divide by
   */
  public dividesBy(smaller: Duration): boolean {
    const myCanonicalLength = this.getCanonicalLength();
    const smallerCanonicalLength = smaller.getCanonicalLength();
    return (
      myCanonicalLength % smallerCanonicalLength === 0 &&
      this.isFloorable() &&
      smaller.isFloorable()
    );
  }

  public getCanonicalLength(): number {
    const spans = this.spans;
    let length = 0;
    for (const span of SPANS_WITH_WEEK) {
      const value = spans[span];
      if (value) length += value * shifters[span].canonicalLength;
    }
    return length;
  }

  public getDescription(capitalize?: boolean): string {
    const spans = this.spans;
    const description: string[] = [];
    for (const span of SPANS_WITH_WEEK) {
      const value = spans[span];
      const spanTitle = capitalize ? capitalizeFirst(span) : span;
      if (value) {
        if (value === 1 && this.singleSpan) {
          description.push(spanTitle);
        } else {
          description.push(pluralIfNeeded(value, spanTitle));
        }
      }
    }
    return description.join(', ');
  }

  public getSingleSpan(): string | undefined {
    return this.singleSpan;
  }

  public getSingleSpanValue(): number | undefined {
    if (!this.singleSpan) return;
    return this.spans[this.singleSpan];
  }

  public limitToDays(): Duration {
    return Duration.fromCanonicalLengthUpToDays(this.getCanonicalLength());
  }
}
