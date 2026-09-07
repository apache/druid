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

import org.joda.time.Interval;
import org.joda.time.chrono.ISOChronology;

import java.util.Objects;

public final class FastIntervalStringFormatter
{
  private static final int MILLIS_PER_SECOND = 1000;
  private static final int SECONDS_PER_MINUTE = 60;
  private static final int MINUTES_PER_HOUR = 60;
  private static final int HOURS_PER_DAY = 24;
  private static final int SECONDS_PER_HOUR = SECONDS_PER_MINUTE * MINUTES_PER_HOUR;
  private static final int SECONDS_PER_DAY = SECONDS_PER_HOUR * HOURS_PER_DAY;
  private static final int MILLIS_PER_DAY = SECONDS_PER_DAY * MILLIS_PER_SECOND;
  // Year starts with march. March has 31, April has 30 ....(31, 30, 31, 30, 31) = 153 days. After that, the pattern repeats.
  private static final int DAYS_IN_FIVE_MONTHS = 153;
  private static final int DAYS_IN_YEAR = 365;

  private static final String ETERNITY_STRING = Intervals.ETERNITY.toString();

  private static final int ERA = 5;
  // 2000-03-01T00:00:00.000Z
  private static final long ERA_START_MILLIS = 951868800000L;
  // 2400-02-29T23:59:59.999Z
  private static final long ERA_END_MILLIS = 13574649599999L;
  // Used to shift to 0000-03-01
  private static final int RESET_OFFSET = 719468;
  private static final int DAYS_PER_ERA = 146097;
  private static final int YEARS_PER_ERA = 400;
  private static final int ERA_DAYS_OFFSET = ERA * DAYS_PER_ERA;
  // Used to get our target year interval (2000-2400)
  private static final int ERA_OFFSET = ERA * YEARS_PER_ERA;

  /**
   * Tries to use optimised format if the interval is within [2000-03-01T00:00:00.000Z, 2400-02-29T23:59:59.999Z].
   * Will fall back to {@link Interval#toString()} if the interval is not within the range or not in UTC timezone.
   *
   * @param interval The interval to convert to a string.
   * @return A string representation of the interval.
   */
  public static String format(Interval interval)
  {
    if (Intervals.isEternity(interval)) {
      return ETERNITY_STRING;
    }
    if (!Objects.equals(interval.getChronology(), ISOChronology.getInstanceUTC()) || interval.getStartMillis() < ERA_START_MILLIS || interval.getEndMillis() > ERA_END_MILLIS) {
      return interval.toString();
    }
    StringBuilder sb = new StringBuilder(49);
    formatIntervalFromMillis(sb, interval.getStartMillis());
    sb.append("/");
    formatIntervalFromMillis(sb, interval.getEndMillis());
    return sb.toString();
  }

  private static void formatIntervalFromMillis(StringBuilder sb, long millis)
  {
    formatDateFromMillis(sb, millis);
    formatTimeFromMillis(sb, millis);
  }

  /**
   * Uses a variation of <a href="https://howardhinnant.github.io/date_algorithms.html#civil_from_days">Howard Hinnant's civil_from_days algorithm</a>
   * to convert the milliseconds since epoch to a string representation of the date.
   * <p>
   * <p>
   * Assumptions/Variation:
   *
   * <ul>
   * <li>We fix era to be 5 since we know the dates will be within [2000-03-01T00:00:00.000Z, 2400-02-29T23:59:59.999Z]</li>
   * <li>We omit any negative dates checks.</li>
   * </ul>
   *
   * <p>
   * <p>
   * An Era is a 400-year cycle. Dates repeat every 400 years. We shift the days to be since 0000-03-01 to simplify calculations.
   * We then calculate the internal year, month, and day of the year and shift it back to a civil calendar date.
   *
   */
  private static void formatDateFromMillis(StringBuilder sb, long millis)
  {

    final int daysSinceEpoch = (int) (millis / MILLIS_PER_DAY);
    // Shift days to be since 0000-03-01
    int z = daysSinceEpoch + RESET_OFFSET;
    int dayOfEra = z - ERA_DAYS_OFFSET;
    int yearOfEra = (dayOfEra - dayOfEra / 1460 + dayOfEra / 36524 - dayOfEra / 146096) / DAYS_IN_YEAR;
    int year = (yearOfEra + ERA_OFFSET);
    int dayOfYear = dayOfEra - (DAYS_IN_YEAR * yearOfEra + yearOfEra / 4 - yearOfEra / 100);
    int mp = (5 * dayOfYear + 2) / DAYS_IN_FIVE_MONTHS;
    int day = dayOfYear - (DAYS_IN_FIVE_MONTHS * mp + 2) / 5 + 1;
    int month = mp + (mp < 10 ? 3 : -9);
    year += (month <= 2) ? 1 : 0;
    sb.append(year);
    sb.append('-');
    appendPadded(sb, month, false);
    sb.append('-');
    appendPadded(sb, day, false);
  }

  private static void formatTimeFromMillis(StringBuilder sb, long millis)
  {
    final int millisOfDay = (int) (millis % MILLIS_PER_DAY);

    // Time within the day
    int totalSeconds = millisOfDay / MILLIS_PER_SECOND;
    int milli = millisOfDay % MILLIS_PER_SECOND;
    int hour = totalSeconds / SECONDS_PER_HOUR;
    totalSeconds -= hour * SECONDS_PER_HOUR;
    int minute = totalSeconds / SECONDS_PER_MINUTE;
    int second = totalSeconds - minute * SECONDS_PER_MINUTE;

    sb.append('T');
    appendPadded(sb, hour, false);
    sb.append(':');
    appendPadded(sb, minute, false);
    sb.append(':');
    appendPadded(sb, second, false);
    sb.append('.');
    appendPadded(sb, milli, true);
    sb.append('Z');
  }

  /**
   * Appends the value to the StringBuilder, padded with zeros to the left. Only supports length 2 or 3.
   */
  private static void appendPadded(StringBuilder sb, int value, boolean isWidth3)
  {
    if (value < 10) {
      sb.append('0');
    }
    if (isWidth3 && value < 100) {
      sb.append('0');
    }
    sb.append(value);
  }

  private FastIntervalStringFormatter()
  {
  }
}
