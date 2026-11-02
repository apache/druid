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

package org.apache.druid.jdbc;

import java.sql.Timestamp;
import java.text.ParseException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;

/**
 * Utility class for parsing timestamp, date, and time strings into {@link Timestamp}.
 */
public class DruidTimestampParser
{
  /**
   * Flexible formatter that accepts ISO8601 and SQL formatted dates, with and without timezones.
   */
  private static final DateTimeFormatter TIMESTAMP_FORMATTER =
      new DateTimeFormatterBuilder()
          .parseCaseInsensitive()
          .optionalStart()
          .append(DateTimeFormatter.ISO_LOCAL_DATE)
          .optionalStart().appendLiteral('T').optionalEnd()
          .optionalStart().appendLiteral(' ').optionalEnd()
          .optionalEnd()
          .optionalStart()
          .append(DateTimeFormatter.ISO_LOCAL_TIME)
          .optionalEnd()
          .optionalStart()
          .appendOffsetId()
          .optionalEnd()
          .toFormatter(Locale.ENGLISH);

  private DruidTimestampParser()
  {
    // No instantiation.
  }

  /**
   * Parse a timestamp, date, or time string into a {@link Timestamp}.
   *
   * <p>Supported formats:</p>
   * <ul>
   *   <li>ISO 8601 instant with offset or Z suffix (e.g. "2025-01-15T12:30:45.000Z", "2025-01-15T12:30:45+05:30")</li>
   *   <li>ISO local date-time with T separator (e.g. "2025-01-15T12:30:45") -- interpreted as UTC</li>
   *   <li>Date-time with space separator (e.g. "2025-06-01 10:30:45.123") -- interpreted as UTC</li>
   *   <li>Bare date (e.g. "2025-01-15") -- interpreted as midnight UTC</li>
   *   <li>Bare time (e.g. "14:30:15") -- interpreted as that time on epoch day, UTC</li>
   *   <li>Epoch milliseconds as a numeric string (e.g. "1715000000000")</li>
   * </ul>
   *
   * @param str the string to parse
   * @return a {@link Timestamp} representing the parsed value
   * @throws IllegalArgumentException if str is null or blank
   * @throws ParseException if the string cannot be parsed as any supported format
   */
  public static Timestamp parse(final String str) throws ParseException
  {
    if (str == null || str.isBlank()) {
      throw new IllegalArgumentException("Timestamp string cannot be null or blank");
    }

    try {
      final TemporalAccessor parsed = TIMESTAMP_FORMATTER.parseBest(
          str,
          OffsetDateTime::from,
          LocalDateTime::from,
          LocalDate::from,
          LocalTime::from
      );

      final Instant instant;
      if (parsed instanceof OffsetDateTime offsetDateTime) {
        instant = offsetDateTime.toInstant();
      } else if (parsed instanceof LocalDateTime localDateTime) {
        instant = localDateTime.toInstant(ZoneOffset.UTC);
      } else if (parsed instanceof LocalDate localDate) {
        instant = localDate.atStartOfDay(ZoneOffset.UTC).toInstant();
      } else if (parsed instanceof LocalTime localTime) {
        instant = localTime.atDate(LocalDate.EPOCH).toInstant(ZoneOffset.UTC);
      } else {
        // Not expecting to reach here.
        throw new IllegalStateException("Unexpected parsed class: " + parsed.getClass());
      }

      return Timestamp.from(instant);
    }
    catch (DateTimeParseException ignored) {
      // Not a string timestamp; fall through to checking for milliseconds.
    }

    try {
      // Epoch milliseconds as a numeric string (e.g. "1715000000000").
      return new Timestamp(Long.parseLong(str));
    }
    catch (NumberFormatException e) {
      throw new ParseException(StringUtils.format("Cannot parse timestamp string: %s", str), 0);
    }
  }

  /**
   * Parse a date or timestamp string and return the calendar date it names, taking the date fields as written,
   * ignoring the timezone information in the string.
   *
   * @param str the string to parse
   * @return the calendar date named by {@code str}
   * @throws IllegalArgumentException if str is null or blank
   * @throws ParseException if the string cannot be parsed as any supported format
   */
  public static LocalDate parseLocalDate(final String str) throws ParseException
  {
    if (str == null || str.isBlank()) {
      throw new IllegalArgumentException("Date string cannot be null or blank");
    }

    try {
      final TemporalAccessor parsed = TIMESTAMP_FORMATTER.parseBest(
          str,
          OffsetDateTime::from,
          LocalDateTime::from,
          LocalDate::from
      );

      if (parsed instanceof OffsetDateTime offsetDateTime) {
        return offsetDateTime.toLocalDate();
      } else if (parsed instanceof LocalDateTime localDateTime) {
        return localDateTime.toLocalDate();
      } else if (parsed instanceof LocalDate localDate) {
        return localDate;
      }
    }
    catch (DateTimeParseException ignored) {
      // Not a string date; fall through to checking for milliseconds.
    }

    try {
      // Epoch milliseconds. There is no offset to go by, so read the date in UTC.
      return Instant.ofEpochMilli(Long.parseLong(str)).atZone(ZoneOffset.UTC).toLocalDate();
    }
    catch (NumberFormatException e) {
      throw new ParseException(StringUtils.format("Cannot parse date string: %s", str), 0);
    }
  }
}
