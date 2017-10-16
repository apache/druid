/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.data.input;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Maps;
import com.google.common.primitives.Longs;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.parsers.ParseException;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class Rows
{
  /**
   * @param timeStamp rollup up timestamp to be used to create group key
   * @param inputRow  input row
   *
   * @return groupKey for the given input row
   */
  public static List<Object> toGroupKey(long timeStamp, InputRow inputRow)
  {
    final Map<String, Set<String>> dims = Maps.newTreeMap();
    for (final String dim : inputRow.getDimensions()) {
      final Set<String> dimValues = ImmutableSortedSet.copyOf(inputRow.getDimension(dim));
      if (dimValues.size() > 0) {
        dims.put(dim, dimValues);
      }
    }
    return ImmutableList.of(
        timeStamp,
        dims
    );
  }

  /**
   * Convert a string to a number. Throws ParseException
   *
   * @param name name of the string being parsed (may be used for exception messages)
   * @param s    the actual string being parsed
   *
   * @return a number
   *
   * @throws NullPointerException if the string is null
   * @throws ParseException       if the string cannot be parsed as a number
   */
  public static Number stringToNumber(final String name, final String s)
  {
    Preconditions.checkNotNull(s, "s");

    try {
      String metricValueString = StringUtils.removeChar(s.trim(), ',');
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
      throw new ParseException(e, "Unable to parse metrics[%s], value[%s]", name, s);
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
}
