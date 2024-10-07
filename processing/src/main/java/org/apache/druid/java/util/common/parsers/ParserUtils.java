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

package org.apache.druid.java.util.common.parsers;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.druid.common.config.NullHandling;
import org.joda.time.DateTimeZone;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ParserUtils
{
  private static final String DEFAULT_COLUMN_NAME_PREFIX = "column_";

  private static final Map<String, DateTimeZone> TIMEZONE_LOOKUP = new HashMap<>();

  static {
    for (String tz : TimeZone.getAvailableIDs()) {
      try {
        TIMEZONE_LOOKUP.put(tz, DateTimeZone.forTimeZone(TimeZone.getTimeZone(tz)));
      }
      catch (IllegalArgumentException e) {
        // Ignore certain date time zone ids like SystemV/AST4. More here https://confluence.atlassian.com/confkb/the-datetime-zone-id-is-not-recognised-167183146.html
      }
    }
  }

  /**
   * @return a transformation function on an input value. The function performs the following transformations on the input string:
   * <li> Splits it into multiple values using the {@code listSplitter} if the {@code list delimiter} is present in the input. </li>
   * <li> If {@code tryParseNumbers} is true, the function will also attempt to parse any numeric values present in the input:
   * integers as {@code Long} and floating-point numbers as {@code Double}. If the input is not a number or parsing fails, the input
   * is returned as-is as a string. </li>
   */
  public static Function<String, Object> getTransformationFunction(
      final String listDelimiter,
      final Splitter listSplitter,
      final boolean tryParseNumbers
  )
  {
    return (input) -> {
      if (input == null) {
        return NullHandling.emptyToNullIfNeeded(input);
      }

      if (input.contains(listDelimiter)) {
        return StreamSupport.stream(listSplitter.split(input).spliterator(), false)
            .map(NullHandling::emptyToNullIfNeeded)
            .map(value -> tryParseNumbers ? ParserUtils.tryParseStringAsNumber(value) : value)
            .collect(Collectors.toList());
      } else {
        return tryParseNumbers ?
            tryParseStringAsNumber(input) :
            NullHandling.emptyToNullIfNeeded(input);

      }
    };
  }

  /**
   * Attempts to parse the input string into a numeric value, if applicable. If the input is a number, the method first
   * tries to parse the input number as a {@code Long}. If parsing as a {@code Long} fails, it then attempts to parse
   * the input number as a {@code Double}. For all other scenarios, the input is returned as-is as a {@code String} type.
   */
  @Nullable
  private static Object tryParseStringAsNumber(@Nullable final String input)
  {
    if (!NumberUtils.isCreatable(input)) {
      return NullHandling.emptyToNullIfNeeded(input);
    }

    final Long l = Longs.tryParse(input);
    if (l != null) {
      return l;
    }
    final Double d = Doubles.tryParse(input);
    if (d != null) {
      return d;
    }
    // fall back to given input if we cannot parse the input as a Long & Double for whatever reason
    return input;
  }

  public static ArrayList<String> generateFieldNames(int length)
  {
    final ArrayList<String> names = new ArrayList<>(length);
    for (int i = 0; i < length; ++i) {
      names.add(getDefaultColumnName(i));
    }
    return names;
  }

  @VisibleForTesting
  static Set<String> findDuplicates(Iterable<String> fieldNames)
  {
    Set<String> duplicates = new HashSet<>();
    Set<String> uniqueNames = new HashSet<>();

    for (String fieldName : fieldNames) {
      if (uniqueNames.contains(fieldName)) {
        duplicates.add(fieldName);
      }
      uniqueNames.add(fieldName);
    }

    return duplicates;
  }

  public static void validateFields(Iterable<String> fieldNames)
  {
    Set<String> duplicates = findDuplicates(fieldNames);
    if (!duplicates.isEmpty()) {
      throw new ParseException(null, "Duplicate column entries found : %s", duplicates.toString());
    }
  }

  public static String stripQuotes(String input)
  {
    input = input.trim();
    if (input.charAt(0) == '\"' && input.charAt(input.length() - 1) == '\"') {
      input = input.substring(1, input.length() - 1).trim();
    }
    return input;
  }

  @Nullable
  public static DateTimeZone getDateTimeZone(String timeZone)
  {
    return TIMEZONE_LOOKUP.get(timeZone);
  }

  /**
   * Return a function to generate default column names.
   * Note that the postfix for default column names starts from 1.
   *
   * @return column name generating function
   */
  public static String getDefaultColumnName(int ordinal)
  {
    return DEFAULT_COLUMN_NAME_PREFIX + (ordinal + 1);
  }
}
