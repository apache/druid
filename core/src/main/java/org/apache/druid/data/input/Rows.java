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

package org.apache.druid.data.input;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.primitives.Longs;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.ParseException;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 *
 */
public final class Rows
{
  /**
   * @param timeStamp rollup up timestamp to be used to create group key
   * @param inputRow  input row
   *
   * @return groupKey for the given input row
   */
  public static List<Object> toGroupKey(long timeStamp, InputRow inputRow)
  {
    final Map<String, Set<String>> dims = new TreeMap<>();
    for (final String dim : inputRow.getDimensions()) {
      final Set<String> dimValues = ImmutableSortedSet.copyOf(inputRow.getDimension(dim));
      if (dimValues.size() > 0) {
        dims.put(dim, dimValues);
      }
    }
    return ImmutableList.of(timeStamp, dims);
  }

  /**
   * Convert an object to a list of strings.
   */
  public static List<String> objectToStrings(final Object inputValue)
  {
    if (inputValue == null) {
      return Collections.emptyList();
    } else if (inputValue instanceof List) {
      // guava's toString function fails on null objects, so please do not use it
      return ((List<?>) inputValue).stream().map(String::valueOf).collect(Collectors.toList());
    } else if (inputValue instanceof byte[]) {
      // convert byte[] to base64 encoded string
      return Collections.singletonList(StringUtils.encodeBase64String((byte[]) inputValue));
    } else {
      return Collections.singletonList(String.valueOf(inputValue));
    }
  }

  /**
   * Convert an object to a number.
   *
   * If {@link NullHandling#replaceWithDefault()} is true, this method will never return null. If false, it will return
   * {@link NullHandling#defaultLongValue()} instead of null.
   *
   * @param name                 field name of the object being converted (may be used for exception messages)
   * @param inputValue           the actual object being converted
   * @param throwParseExceptions whether this method should throw a {@link ParseException} or use a default/null value
   *                             when {@param inputValue} is not numeric
   *
   * @return a Number; will not necessarily be the same type as {@param zeroClass}
   *
   * @throws ParseException if the input cannot be converted to a number and {@code throwParseExceptions} is true
   */
  @Nullable
  public static <T extends Number> Number objectToNumber(
      final String name,
      final Object inputValue,
      final boolean throwParseExceptions
  )
  {
    if (inputValue == null) {
      return NullHandling.defaultLongValue();
    } else if (inputValue instanceof Number) {
      return (Number) inputValue;
    } else if (inputValue instanceof String) {
      try {
        String metricValueString = StringUtils.removeChar(((String) inputValue).trim(), ',');
        // Longs.tryParse() doesn't support leading '+', so we need to trim it ourselves
        metricValueString = trimLeadingPlusOfLongString(metricValueString);
        Long v = Longs.tryParse(metricValueString);
        // Do NOT use ternary operator here, because it makes Java to convert Long to Double
        if (v != null) {
          return v;
        } else {
          return Double.valueOf(metricValueString);
        }
      }
      catch (Exception e) {
        if (throwParseExceptions) {
          throw new ParseException(e, "Unable to parse value[%s] for field[%s]", inputValue, name);
        } else {
          return NullHandling.defaultLongValue();
        }
      }
    } else {
      if (throwParseExceptions) {
        throw new ParseException("Unknown type[%s] for field[%s]", inputValue.getClass(), name);
      } else {
        return NullHandling.defaultLongValue();
      }
    }
  }

  private static String trimLeadingPlusOfLongString(String metricValueString)
  {
    if (metricValueString.length() > 1 && metricValueString.charAt(0) == '+') {
      char secondChar = metricValueString.charAt(1);
      if (secondChar >= '0' && secondChar <= '9') {
        metricValueString = metricValueString.substring(1);
      }
    }
    return metricValueString;
  }

  private Rows()
  {
  }
}
