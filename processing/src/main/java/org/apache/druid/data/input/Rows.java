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
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.segment.column.ValueType;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.Arrays;
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
   * Convert an object to a list of strings. This function translates single value nulls into an empty list, and any
   * nulls inside of a list or array into the string "null". Do not use this method if you don't want this behavior,
   * but note that many implementations of {@link InputRow#getDimension(String)} do use this method, so it is
   * recommended to use {@link InputRow#getRaw(String)} if you want the actual value without this coercion. For legacy
   * reasons, some stuff counts on this incorrect behavior, (such as {@link Rows#toGroupKey(long, InputRow)}).
   */
  public static List<String> objectToStrings(final Object inputValue)
  {
    if (inputValue == null) {
      return Collections.emptyList();
    } else if (inputValue instanceof List) {
      // guava's toString function fails on null objects, so please do not use it
      return ((List<?>) inputValue).stream().map(String::valueOf).collect(Collectors.toList());
    } else if (inputValue instanceof byte[]) {
      byte[] array = (byte[]) inputValue;
      return objectToStringsByteA(array);
    } else if (inputValue instanceof ByteBuffer) {
      byte[] array = ((ByteBuffer) inputValue).array();
      return objectToStringsByteA(array);
    } else if (inputValue instanceof Object[]) {
      return Arrays.stream((Object[]) inputValue).map(String::valueOf).collect(Collectors.toList());
    } else {
      return Collections.singletonList(String.valueOf(inputValue));
    }
  }

  private static List<String> objectToStringsByteA(byte[] array)
  {
    // convert byte[] to base64 encoded string
    return Collections.singletonList(StringUtils.encodeBase64String(array));
  }

  /**
   * Convert an object to a number.
   *
   * If {@link NullHandling#replaceWithDefault()} is true, this method will never return null. If false, it will return
   * {@link NullHandling#defaultLongValue()} instead of null.
   *
   * @param name                 field name of the object being converted (may be used for exception messages)
   * @param inputValue           the actual object being converted
   * @param outputType           expected return type, or null if it should be automatically detected
   * @param throwParseExceptions whether this method should throw a {@link ParseException} or use a default/null value
   *                             when {@param inputValue} is not numeric
   *
   * @return a Number; will not necessarily be the same type as {@param zeroClass}
   *
   * @throws ParseException if the input cannot be converted to a number and {@code throwParseExceptions} is true
   */
  @Nullable
  public static Number objectToNumber(
      final String name,
      final Object inputValue,
      @Nullable final ValueType outputType,
      final boolean throwParseExceptions
  )
  {
    if (outputType != null && !outputType.isNumeric()) {
      throw new IAE("Output type[%s] must be numeric", outputType);
    }

    if (inputValue == null) {
      return (Number) NullHandling.defaultValueForType(outputType != null ? outputType : ValueType.LONG);
    } else if (inputValue instanceof Number) {
      return (Number) inputValue;
    } else if (inputValue instanceof String) {
      try {
        String metricValueString = StringUtils.removeChar(((String) inputValue).trim(), ',');
        // Longs.tryParse() doesn't support leading '+', so we need to trim it ourselves
        metricValueString = trimLeadingPlusOfLongString(metricValueString);

        Number v = null;

        // Try parsing as Long first, since it's significantly faster than Double parsing, and also there are various
        // integer numbers that can be represented as Long but cannot be represented as Double.
        if (outputType == null || outputType == ValueType.LONG) {
          v = Longs.tryParse(metricValueString);
        }

        if (v == null && outputType != ValueType.LONG) {
          v = Double.valueOf(metricValueString);
        }

        if (v == null) {
          if (throwParseExceptions) {
            throw new ParseException(
                String.valueOf(inputValue),
                "Unable to parse value[%s] for field[%s] as type[%s]",
                inputValue.getClass(),
                name,
                outputType
            );
          } else {
            return (Number) NullHandling.defaultValueForType(outputType);
          }
        } else {
          return outputType == ValueType.FLOAT ? Float.valueOf(v.floatValue()) : v;
        }
      }
      catch (Exception e) {
        if (throwParseExceptions) {
          throw new ParseException(
              String.valueOf(inputValue),
              e,
              "Unable to parse value[%s] for field[%s]",
              inputValue,
              name
          );
        } else {
          return (Number) NullHandling.defaultValueForType(outputType != null ? outputType : ValueType.LONG);
        }
      }
    } else {
      if (throwParseExceptions) {
        throw new ParseException(
            String.valueOf(inputValue),
            "Unknown type[%s] for field[%s]",
            inputValue.getClass(),
            name
        );
      } else {
        return (Number) NullHandling.defaultValueForType(outputType != null ? outputType : ValueType.LONG);
      }
    }
  }

  /**
   * Shorthand for {@link #objectToNumber(String, Object, ValueType, boolean)} with null expectedType.
   */
  public static Number objectToNumber(
      final String name,
      final Object inputValue,
      final boolean throwParseExceptions
  )
  {
    return objectToNumber(name, inputValue, null, throwParseExceptions);
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
