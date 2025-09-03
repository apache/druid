/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.java.util.common;

import com.google.common.collect.ImmutableList;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.guava.Comparators;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import javax.annotation.Nullable;

public final class Intervals
{
  public static final Interval ETERNITY = utc(JodaUtils.MIN_INSTANT, JodaUtils.MAX_INSTANT);
  public static final ImmutableList<Interval> ONLY_ETERNITY = ImmutableList.of(ETERNITY);
  private static final DateTimeFormatter FAST_ISO_UTC_FORMATTER =
      ISODateTimeFormat.dateTime().withChronology(ISOChronology.getInstanceUTC());

  public static Interval utc(long startInstant, long endInstant)
  {
    return new Interval(startInstant, endInstant, ISOChronology.getInstanceUTC());
  }

  public static Interval of(String interval)
  {
    try {
      return new Interval(interval, ISOChronology.getInstanceUTC());
    }
    catch (IllegalArgumentException e) {
      throw InvalidInput.exception(e, "Invalid interval[%s]: [%s]", interval, e.getMessage());
    }
  }

  public static Interval of(String format, Object... formatArgs)
  {
    return of(StringUtils.format(format, formatArgs));
  }

  /**
   * @return Null if cannot parse with optimized strategy, else return the Interval.
   */
  @Nullable
  private static Interval tryOptimizedIntervalDeserialization(final String intervalText)
  {
    final int slashIndex = intervalText.indexOf('/');
    if (slashIndex <= 0 || slashIndex >= intervalText.length() - 1) {
      return null;
    }
    try {
      final String startStr = intervalText.substring(0, slashIndex);
      final long startMillis = FAST_ISO_UTC_FORMATTER.parseMillis(startStr);

      final String endStr = intervalText.substring(slashIndex + 1);
      final long endMillis = FAST_ISO_UTC_FORMATTER.parseMillis(endStr);
      return Intervals.utc(startMillis, endMillis);
    }
    catch (IllegalArgumentException e) {
      return null;
    }
  }

  /**
   * Parses a Joda-Time {@link Interval} from its string representation.
   *
   * Tries a fast path for ISO-8601 UTC, slash-delimited intervals (e.g.
   * "2022-09-16T00:00:00.000Z/2022-09-17T00:00:00.000Z"); otherwise falls back to the
   * general interval parser. The returned interval uses ISO chronology in UTC.
   */
  public static Interval deserialize(String string)
  {
    final Interval interval = tryOptimizedIntervalDeserialization(string);
    if (interval == null) {
      return Intervals.of(string);
    } else {
      return interval;
    }
  }

  /**
   * Returns true if the provided interval has endpoints that can be compared against other DateTimes using their
   * string representations.
   *
   * See also {@link DateTimes#canCompareAsString(DateTime)}.
   */
  public static boolean canCompareEndpointsAsStrings(final Interval interval)
  {
    return DateTimes.canCompareAsString(interval.getStart()) && DateTimes.canCompareAsString(interval.getEnd());
  }

  /**
   * Returns true if the provided interval contains all time.
   */
  public static boolean isEternity(final Interval interval)
  {
    return ETERNITY.equals(interval);
  }

  /**
   * Finds an interval from the given set of sortedIntervals which overlaps with
   * the searchInterval. If multiple candidate intervals overlap with the
   * searchInterval, the "smallest" interval based on the
   * {@link Comparators#intervalsByStartThenEnd()} is returned.
   *
   * @param searchInterval  Interval which should overlap with the result
   * @param sortedIntervals Candidate overlapping intervals, sorted in ascending
   *                        order, using {@link Comparators#intervalsByStartThenEnd()}.
   * @return The first overlapping interval, if one exists, otherwise null.
   */
  @Nullable
  public static Interval findOverlappingInterval(Interval searchInterval, Interval[] sortedIntervals)
  {
    for (Interval interval : sortedIntervals) {
      if (interval.overlaps(searchInterval)) {
        return interval;
      } else if (interval.getStart().isAfter(searchInterval.getEnd())) {
        // Intervals after this cannot have an overlap
        return null;
      }
    }

    return null;
  }

  private Intervals()
  {
  }
}
