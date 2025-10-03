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
   * A performance-optimized method for parsing a Joda-Time {@link Interval} from a string.
   * This method is significantly faster than the standard {@link Intervals#of(String)} for the following
   * group of offsets:
   * <ol>
   *   <li>"2022-01-01T00:00:00.000Z/2022-01-02T00:00:00.000Z"</li>
   *   <li>"2022-01-01T00:00:00.000+05:30/2022-01-01T01:00:00.000+05:30"</li>
   *   <li>"2022-01-01T00:00:00.000+0530/2022-01-01T01:00:00.000+0530"</li>
   * </ol>
   * <p>
   * If the input string does not match the format, it will fall back to the more flexible but
   * slower {@link Intervals#of(String)} parser. If you are dealing with any Intervals format examples below,
   * consider using {@link Intervals#of(String)} instead:
   * <ol>
   *   <li>"2022-01-01T00:00:00Z/2022-01-02T00:00:00Z" (without millis)</li>
   *   <li>"2022-01-01/2022-01-02" (Date only)</li>
   *   <li>"2022-01-01T12:00:00.000Z/PT6H" (Periods in start / end)</li>
   * </ol>
   *
   * Currently, this method is only used in {@link org.apache.druid.timeline.SegmentId}.
   */
  public static Interval fromString(String string)
  {
    if (canDeserializeIntervalOptimallyFromString(string)) {
      Interval interval = tryOptimizedIntervalDeserialization(string);

      if (interval != null) {
        return interval;
      }
    }

    return Intervals.of(string);
  }

  private static boolean canDeserializeIntervalOptimallyFromString(String intervalText)
  {
    // Optimized version does not deal well with Periods.
    if (intervalText.contains("P")) {
      return false;
    }

    final int slashIndex = intervalText.indexOf('/');
    return (slashIndex > 0 && slashIndex < intervalText.length() - 1);
  }

  /**
   * @return null if the input format cannot be parsed with optimized strategy, else return the Interval.
   */
  @Nullable
  private static Interval tryOptimizedIntervalDeserialization(final String intervalText)
  {
    final int slashIndex = intervalText.indexOf('/');
    final String startStr = intervalText.substring(0, slashIndex);
    final String endStr = intervalText.substring(slashIndex + 1);

    try {
      final long startMillis = FAST_ISO_UTC_FORMATTER.parseMillis(startStr);
      final long endMillis = FAST_ISO_UTC_FORMATTER.parseMillis(endStr);
      return Intervals.utc(startMillis, endMillis);
    }
    catch (IllegalArgumentException e) {
      return null;
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
