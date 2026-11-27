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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import io.netty.util.SuppressForbidden;
import org.apache.druid.java.util.common.guava.Comparators;
import org.joda.time.Chronology;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.Months;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Pattern;

public final class DateTimes
{
  public static final DateTime EPOCH = utc(0);
  public static final DateTime MAX = utc(JodaUtils.MAX_INSTANT);
  public static final DateTime MIN = utc(JodaUtils.MIN_INSTANT);
  // The following two DateTime objects are utilities that can be used for accurately comparing date strings
  public static final DateTime COMPARE_DATE_AS_STRING_MIN = of("0000-01-01");
  public static final DateTime COMPARE_DATE_AS_STRING_MAX = of("10000-01-01").minus(1);

  public static final UtcFormatter ISO_DATE_TIME = wrapFormatter(ISODateTimeFormat.dateTime());
  public static final UtcFormatter ISO_DATE_OPTIONAL_TIME = wrapFormatter(ISODateTimeFormat.dateOptionalTimeParser());
  public static final UtcFormatter ISO_DATE_OR_TIME_WITH_OFFSET = wrapFormatter(
      ISODateTimeFormat.dateTimeParser().withOffsetParsed()
  );

  private static final Set<String> AVAILABLE_TIMEZONE_IDS = ImmutableSet.copyOf(TimeZone.getAvailableIDs());

  /**
   * This pattern aims to match strings, produced by {@link DateTime#toString()}. It's not rigorous: it could accept
   * some strings that couldn't be obtained by calling toString() on any {@link DateTime} object, and also it could
   * not match some valid DateTime string. Use for heuristic purposes only.
   */
  public static final Pattern COMMON_DATE_TIME_PATTERN = Pattern.compile(
      //year    month     day        hour       minute     second       millis  time zone
      "[0-9]{4}-[01][0-9]-[0-3][0-9]T[0-2][0-9]:[0-5][0-9]:[0-5][0-9]\\.[0-9]{3}(Z|[+\\-][0-9]{2}(:[0-9]{2}))"
  );

  public static DateTimeZone inferTzFromString(String tzId)
  {
    return inferTzFromString(tzId, true);
  }

  /**
   * @return The dateTimezone for the provided {@param tzId}. If {@param fallback} is true, the default timezone
   * will be returned if the tzId does not match a supported timezone.
   *
   * @throws IllegalArgumentException if {@param fallback} is false and the provided tzId doesn't match a supported
   *                                  timezone.
   */
  @SuppressForbidden(reason = "DateTimeZone#forID")
  public static DateTimeZone inferTzFromString(String tzId, boolean fallback) throws IllegalArgumentException
  {
    try {
      return DateTimeZone.forID(tzId);
    }
    catch (IllegalArgumentException e) {
      // also support Java timezone strings
      if (!fallback && !AVAILABLE_TIMEZONE_IDS.contains(tzId)) {
        throw e;
      }
      return DateTimeZone.forTimeZone(TimeZone.getTimeZone(tzId));
    }
  }

  /**
   * Simple wrapper class to enforce UTC Chronology in formatter. Specifically, it will use
   * {@link DateTimeFormatter#withChronology(Chronology)} to set the chronology to
   * {@link ISOChronology#getInstanceUTC()} on the wrapped {@link DateTimeFormatter}.
   */
  public static class UtcFormatter
  {
    private final DateTimeFormatter innerFormatter;

    private UtcFormatter(final DateTimeFormatter innerFormatter)
    {
      this.innerFormatter = innerFormatter.withChronology(ISOChronology.getInstanceUTC());
    }

    @SuppressForbidden(reason = "DateTimeFormatter#parseDateTime")
    public DateTime parse(final String instant)
    {
      return innerFormatter.parseDateTime(instant);
    }

    public String print(final DateTime instant)
    {
      return innerFormatter.print(instant);
    }
  }

  /**
   * Creates a {@link UtcFormatter} that wraps around a {@link DateTimeFormatter}.
   *
   * @param formatter inner {@link DateTimeFormatter} used to parse {@link String}
   */
  public static UtcFormatter wrapFormatter(final DateTimeFormatter formatter)
  {
    return new UtcFormatter(formatter);
  }

  public static DateTime utc(long instant)
  {
    return new DateTime(instant, ISOChronology.getInstanceUTC());
  }

  public static DateTime of(String instant)
  {
    if (instant != null
        && instant.length() == 24
        && instant.charAt(4) == '-'
        && instant.charAt(7) == '-'
        && instant.charAt(10) == 'T'
        && instant.charAt(13) == ':'
        && instant.charAt(16) == ':'
        && instant.charAt(19) == '.'
        && instant.charAt(23) == 'Z') {
      final char c0 = instant.charAt(0);
      final char c1 = instant.charAt(1);
      final char c2 = instant.charAt(2);
      final char c3 = instant.charAt(3);
      final char c5 = instant.charAt(5);
      final char c6 = instant.charAt(6);
      final char c8 = instant.charAt(8);
      final char c9 = instant.charAt(9);
      final char c11 = instant.charAt(11);
      final char c12 = instant.charAt(12);
      final char c14 = instant.charAt(14);
      final char c15 = instant.charAt(15);
      final char c17 = instant.charAt(17);
      final char c18 = instant.charAt(18);
      final char c20 = instant.charAt(20);
      final char c21 = instant.charAt(21);
      final char c22 = instant.charAt(22);
      if (c0 >= '0' && c0 <= '9'
          && c1 >= '0' && c1 <= '9'
          && c2 >= '0' && c2 <= '9'
          && c3 >= '0' && c3 <= '9'
          && c5 >= '0' && c5 <= '9'
          && c6 >= '0' && c6 <= '9'
          && c8 >= '0' && c8 <= '9'
          && c9 >= '0' && c9 <= '9'
          && c11 >= '0' && c11 <= '9'
          && c12 >= '0' && c12 <= '9'
          && c14 >= '0' && c14 <= '9'
          && c15 >= '0' && c15 <= '9'
          && c17 >= '0' && c17 <= '9'
          && c18 >= '0' && c18 <= '9'
          && c20 >= '0' && c20 <= '9'
          && c21 >= '0' && c21 <= '9'
          && c22 >= '0' && c22 <= '9') {
        final int year = (c0 - '0') * 1000 + (c1 - '0') * 100 + (c2 - '0') * 10 + (c3 - '0');
        final int month = (c5 - '0') * 10 + (c6 - '0');
        final int day = (c8 - '0') * 10 + (c9 - '0');
        final int hour = (c11 - '0') * 10 + (c12 - '0');
        final int minute = (c14 - '0') * 10 + (c15 - '0');
        final int second = (c17 - '0') * 10 + (c18 - '0');
        final int millis = (c20 - '0') * 100 + (c21 - '0') * 10 + (c22 - '0');
        try {
          return new DateTime(year, month, day, hour, minute, second, millis, ISOChronology.getInstanceUTC());
        }
        catch (IllegalArgumentException e) {
          // Out-of-range field (e.g. month 13): fall through below.
        }
      }
    }

    try {
      return new DateTime(instant, ISOChronology.getInstanceUTC());
    }
    catch (IllegalArgumentException ex) {
      try {
        return new DateTime(Long.valueOf(instant), ISOChronology.getInstanceUTC());
      }
      catch (IllegalArgumentException ex2) {
        throw ex;
      }
    }
  }
  
  public static DateTime of(
      int year,
      int monthOfYear,
      int dayOfMonth,
      int hourOfDay,
      int minuteOfHour
  )
  {
    return new DateTime(year, monthOfYear, dayOfMonth, hourOfDay, minuteOfHour, ISOChronology.getInstanceUTC());
  }

  public static DateTime nowUtc()
  {
    return DateTime.now(ISOChronology.getInstanceUTC());
  }

  public static DateTime max(DateTime dt1, DateTime dt2)
  {
    return dt1.compareTo(dt2) >= 0 ? dt1 : dt2;
  }

  public static DateTime min(DateTime dt1, DateTime dt2)
  {
    return dt1.compareTo(dt2) < 0 ? dt1 : dt2;
  }

  public static int subMonths(long timestamp1, long timestamp2, DateTimeZone timeZone)
  {
    DateTime time1 = new DateTime(timestamp1, timeZone);
    DateTime time2 = new DateTime(timestamp2, timeZone);

    return Months.monthsBetween(time1, time2).getMonths();
  }

  /**
   * Returns true if the provided DateTime can be compared against other DateTimes using its string representation.
   * Useful when generating SQL queries to the metadata store, or any other situation where time comparisons on
   * string representations might be useful.
   *
   * Conditions: the datetime must be between years 0 and 9999 (inclusive) and must be in the ISO UTC chronology.
   *
   * See also {@link Intervals#canCompareEndpointsAsStrings(Interval)}.
   */
  public static boolean canCompareAsString(final DateTime dateTime)
  {
    return dateTime.getMillis() >= COMPARE_DATE_AS_STRING_MIN.getMillis()
           && dateTime.getMillis() <= COMPARE_DATE_AS_STRING_MAX.getMillis()
           && ISOChronology.getInstanceUTC().equals(dateTime.getChronology());
  }

  /**
   * Returns the earlier of the two given dates. When passed a null and a non-null
   * date, this method simply returns the non-null value.
   */
  public static DateTime earlierOf(DateTime a, DateTime b)
  {
    // Put nulls last to select the smaller non-null value
    if (Objects.compare(a, b, Ordering.natural().nullsLast()) < 0) {
      return a;
    } else {
      return b;
    }
  }

  /**
   * Returns the later of the two given dates. When passed a null and a non-null
   * date, this method simply returns the non-null value.
   */
  public static DateTime laterOf(DateTime a, DateTime b)
  {
    // Put nulls first to select the bigger non-null value
    if (Objects.compare(a, b, Comparators.naturalNullsFirst()) > 0) {
      return a;
    } else {
      return b;
    }
  }

  private DateTimes()
  {
  }
}
