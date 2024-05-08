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

package org.apache.druid.data.input.impl;

import com.google.common.collect.ImmutableList;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.Rows;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class RowsTest extends InitializedNullHandlingTest
{
  private static final String FIELD_NAME = "foo";

  private final Map<Object, Number> validCases = new LinkedHashMap<>();
  private final List<Object> invalidCases = new ArrayList<>();

  @Before
  public void setUp()
  {
    // Null
    validCases.put(null, NullHandling.sqlCompatible() ? null : 0L);

    // Empty string
    if (NullHandling.sqlCompatible()) {
      invalidCases.add("");
    } else {
      validCases.put("", 0L);
    }

    // Strings that are valid longs
    validCases.put("0", 0L);
    validCases.put("+0", 0L);
    validCases.put("1", 1L);
    validCases.put("1,234", 1234L);
    validCases.put("-1,234", -1234L);
    validCases.put("+1", 1L);
    validCases.put("-1", -1L);
    validCases.put(String.valueOf(Long.MAX_VALUE), Long.MAX_VALUE);
    validCases.put(String.valueOf(Long.MIN_VALUE), Long.MIN_VALUE);

    // Strings that are valid regular doubles
    validCases.put("0.0", 0.0);
    validCases.put("-0.0", -0.0);
    validCases.put("+0.0", 0.0);
    validCases.put("1e1", 10.0);
    validCases.put("-1e1", -10.0);
    validCases.put("1.0", 1.0);
    validCases.put("2.1", 2.1);

    // Strings that are valid special doubles
    //CHECKSTYLE.OFF: Regexp
    validCases.put(String.valueOf(Double.MAX_VALUE), Double.MAX_VALUE);
    validCases.put(String.valueOf(Double.MIN_VALUE), Double.MIN_VALUE);
    validCases.put(String.valueOf(Double.POSITIVE_INFINITY), Double.POSITIVE_INFINITY);
    validCases.put(String.valueOf(Double.NEGATIVE_INFINITY), Double.NEGATIVE_INFINITY);
    //CHECKSTYLE.ON: Regexp
    validCases.put(String.valueOf(Double.NaN), Double.NaN);

    // Numbers are also valid numbers
    validCases.put(0, 0);
    validCases.put(0L, 0L);
    validCases.put(0.0, 0.0);
    validCases.put(0f, 0f);
    validCases.put(-1L, -1L);
    validCases.put(1.1, 1.1);

    // Invalid objects
    invalidCases.add("notanumber");
    invalidCases.add(ImmutableList.of(1L));
    invalidCases.add(ImmutableList.of("1"));
  }

  @Test
  public void test_objectToNumber_typeUnknown_noThrow()
  {
    for (final Map.Entry<Object, Number> entry : validCases.entrySet()) {
      Assert.assertEquals(
          StringUtils.format(
              "%s (%s)",
              entry.getKey(),
              entry.getKey() == null ? null : entry.getKey().getClass().getSimpleName()
          ),
          entry.getValue(), Rows.objectToNumber(FIELD_NAME, entry.getKey(), null, false)
      );
    }

    for (final Object o : invalidCases) {
      Assert.assertEquals(
          o + " (nothrow)",
          NullHandling.defaultLongValue(),
          Rows.objectToNumber(FIELD_NAME, o, null, false)
      );

      final ParseException e = Assert.assertThrows(
          o + " (throw)",
          ParseException.class,
          () -> Rows.objectToNumber(FIELD_NAME, o, null, true)
      );

      MatcherAssert.assertThat(
          e,
          ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString("for field[" + FIELD_NAME + "]"))
      );
    }
  }

  @Test
  public void test_objectToNumber_typeLong_noThrow()
  {
    for (final Map.Entry<Object, Number> entry : validCases.entrySet()) {
      Assert.assertEquals(
          StringUtils.format(
              "%s (%s)",
              entry.getKey(),
              entry.getKey() == null ? null : entry.getKey().getClass().getSimpleName()
          ),
          entry.getValue() != null ? entry.getValue().longValue() : null,
          Rows.objectToNumber(FIELD_NAME, entry.getKey(), ValueType.LONG, false)
      );
    }

    for (final Object o : invalidCases) {
      Assert.assertEquals(
          o + " (nothrow)",
          NullHandling.defaultLongValue(),
          Rows.objectToNumber(FIELD_NAME, o, ValueType.LONG, false)
      );

      final ParseException e = Assert.assertThrows(
          o + " (throw)",
          ParseException.class,
          () -> Rows.objectToNumber(FIELD_NAME, o, ValueType.LONG, true)
      );

      MatcherAssert.assertThat(
          e,
          ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString("for field[" + FIELD_NAME + "]"))
      );
    }
  }

  @Test
  public void test_objectToNumber_typeFloat_noThrow()
  {
    for (final Map.Entry<Object, Number> entry : validCases.entrySet()) {
      Assert.assertEquals(
          StringUtils.format(
              "%s (%s)",
              entry.getKey(),
              entry.getKey() == null ? null : entry.getKey().getClass().getSimpleName()
          ),
          entry.getValue() != null ? entry.getValue().floatValue() : null,
          Rows.objectToNumber(FIELD_NAME, entry.getKey(), ValueType.FLOAT, false)
      );
    }

    for (final Object o : invalidCases) {
      Assert.assertEquals(
          o + " (nothrow)",
          NullHandling.defaultFloatValue(),
          Rows.objectToNumber(FIELD_NAME, o, ValueType.FLOAT, false)
      );

      final ParseException e = Assert.assertThrows(
          o + " (throw)",
          ParseException.class,
          () -> Rows.objectToNumber(FIELD_NAME, o, ValueType.FLOAT, true)
      );

      MatcherAssert.assertThat(
          e,
          ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString("for field[" + FIELD_NAME + "]"))
      );
    }
  }

  @Test
  public void test_objectToNumber_typeDouble_noThrow()
  {
    for (final Map.Entry<Object, Number> entry : validCases.entrySet()) {
      Assert.assertEquals(
          StringUtils.format(
              "%s (%s)",
              entry.getKey(),
              entry.getKey() == null ? null : entry.getKey().getClass().getSimpleName()
          ),
          entry.getValue() != null ? entry.getValue().doubleValue() : null,
          Rows.objectToNumber(FIELD_NAME, entry.getKey(), ValueType.DOUBLE, false)
      );
    }

    for (final Object o : invalidCases) {
      Assert.assertEquals(
          o + " (nothrow)",
          NullHandling.defaultDoubleValue(),
          Rows.objectToNumber(FIELD_NAME, o, ValueType.DOUBLE, false)
      );

      final ParseException e = Assert.assertThrows(
          o + " (throw)",
          ParseException.class,
          () -> Rows.objectToNumber(FIELD_NAME, o, ValueType.DOUBLE, true)
      );

      MatcherAssert.assertThat(
          e,
          ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString("for field[" + FIELD_NAME + "]"))
      );
    }
  }
}
