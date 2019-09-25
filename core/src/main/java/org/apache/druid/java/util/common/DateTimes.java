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

import io.netty.util.SuppressForbidden;
import org.joda.time.Chronology;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Months;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.util.TimeZone;
import java.util.regex.Pattern;

public final class DateTimes
{
  public static final DateTime EPOCH = utc(0);
  public static final DateTime MAX = utc(JodaUtils.MAX_INSTANT);
  public static final DateTime MIN = utc(JodaUtils.MIN_INSTANT);

  public static final UtcFormatter ISO_DATE_TIME = wrapFormatter(ISODateTimeFormat.dateTime());
  public static final UtcFormatter ISO_DATE_OPTIONAL_TIME = wrapFormatter(ISODateTimeFormat.dateOptionalTimeParser());
  public static final UtcFormatter ISO_DATE_OR_TIME_WITH_OFFSET = wrapFormatter(
      ISODateTimeFormat.dateTimeParser().withOffsetParsed()
  );

  /**
   * This pattern aims to match strings, produced by {@link DateTime#toString()}. It's not rigorous: it could accept
   * some strings that couldn't be obtained by calling toString() on any {@link DateTime} object, and also it could
   * not match some valid DateTime string. Use for heuristic purposes only.
   */
  public static final Pattern COMMON_DATE_TIME_PATTERN = Pattern.compile(
      //year    month     day        hour       minute     second       millis  time zone
      "[0-9]{4}-[01][0-9]-[0-3][0-9]T[0-2][0-9]:[0-5][0-9]:[0-5][0-9]\\.[0-9]{3}(Z|[+\\-][0-9]{2}(:[0-9]{2}))"
  );

  @SuppressForbidden(reason = "DateTimeZone#forID")
  public static DateTimeZone inferTzFromString(String tzId)
  {
    try {
      return DateTimeZone.forID(tzId);
    }
    catch (IllegalArgumentException e) {
      // also support Java timezone strings
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

  private DateTimes()
  {
  }
}
