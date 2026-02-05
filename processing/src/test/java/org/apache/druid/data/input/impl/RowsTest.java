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
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.Rows;
import org.apache.druid.java.util.common.DateTimes;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RowsTest extends InitializedNullHandlingTest
{
  private static final String FIELD_NAME = "foo";

  private final Map<Object, Number> validCases = new LinkedHashMap<>();
  private final List<Object> invalidCases = new ArrayList<>();

  @Before
  public void setUp()
  {
    // Null
    validCases.put(null, null);

    // Empty string
    invalidCases.add("");

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
      Assert.assertNull(o + " (nothrow)", Rows.objectToNumber(FIELD_NAME, o, null, false));

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
      Assert.assertNull(o + " (nothrow)", Rows.objectToNumber(FIELD_NAME, o, ValueType.LONG, false));

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
      Assert.assertNull(o + " (nothrow)", Rows.objectToNumber(FIELD_NAME, o, ValueType.FLOAT, false));

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
      Assert.assertNull(o + " (nothrow)", Rows.objectToNumber(FIELD_NAME, o, ValueType.DOUBLE, false));

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

  @Test
  public void test_objectToStrings_nullInput()
  {
    // Null input should return empty list
    Assert.assertEquals(Collections.emptyList(), Rows.objectToStrings(null));
  }

  @Test
  public void test_objectToStrings_singleString()
  {
    Assert.assertEquals(Collections.singletonList("foo"), Rows.objectToStrings("foo"));
  }

  @Test
  public void test_objectToStrings_listWithStrings()
  {
    Assert.assertEquals(
        Arrays.asList("a", "b", "c"),
        Rows.objectToStrings(Arrays.asList("a", "b", "c"))
    );
  }

  @Test
  public void test_objectToStrings_listWithNullValues()
  {
    // After the fix, null values in lists are preserved as actual null, not converted to "null" string
    List<String> result = Rows.objectToStrings(Arrays.asList("a", null, "b"));
    Assert.assertEquals(3, result.size());
    Assert.assertEquals("a", result.get(0));
    Assert.assertNull(result.get(1));
    Assert.assertEquals("b", result.get(2));
  }

  @Test
  public void test_objectToStrings_listWithOnlyNull()
  {
    List<String> result = Rows.objectToStrings(Collections.singletonList(null));
    Assert.assertEquals(1, result.size());
    Assert.assertNull(result.get(0));
  }

  @Test
  public void test_objectToStrings_listWithMultipleNulls()
  {
    List<String> result = Rows.objectToStrings(Arrays.asList(null, "a", null, "b", null));
    Assert.assertEquals(5, result.size());
    Assert.assertNull(result.get(0));
    Assert.assertEquals("a", result.get(1));
    Assert.assertNull(result.get(2));
    Assert.assertEquals("b", result.get(3));
    Assert.assertNull(result.get(4));
  }

  @Test
  public void test_objectToStrings_arrayWithNullValues()
  {
    // Arrays should also preserve null values
    List<String> result = Rows.objectToStrings(new Object[]{"a", null, "b"});
    Assert.assertEquals(3, result.size());
    Assert.assertEquals("a", result.get(0));
    Assert.assertNull(result.get(1));
    Assert.assertEquals("b", result.get(2));
  }

  @Test
  public void test_objectToStrings_numberConversion()
  {
    Assert.assertEquals(Collections.singletonList("123"), Rows.objectToStrings(123));
    Assert.assertEquals(Collections.singletonList("123.45"), Rows.objectToStrings(123.45));
  }

  @Test
  public void test_objectToStrings_byteArrayBase64()
  {
    byte[] bytes = new byte[]{1, 2, 3};
    List<String> result = Rows.objectToStrings(bytes);
    Assert.assertEquals(1, result.size());
    Assert.assertEquals(StringUtils.encodeBase64String(bytes), result.get(0));
  }

  @Test
  public void test_toGroupKey_basicFunctionality()
  {
    long timestamp = DateTimes.of("2023-01-01").getMillis();
    Map<String, Object> event = new HashMap<>();
    event.put("dim1", "value1");
    event.put("dim2", Arrays.asList("a", "b"));

    InputRow inputRow = new MapBasedInputRow(
        timestamp,
        Arrays.asList("dim1", "dim2"),
        event
    );

    List<Object> groupKey = Rows.toGroupKey(timestamp, inputRow);
    Assert.assertEquals(2, groupKey.size());
    Assert.assertEquals(timestamp, groupKey.get(0));

    @SuppressWarnings("unchecked")
    Map<String, Set<String>> dimensions = (Map<String, Set<String>>) groupKey.get(1);
    Assert.assertNotNull(dimensions);
  }

  @Test
  public void test_toGroupKey_withNullInMultiValueDimension()
  {
    // After the fix, toGroupKey should handle null values in multi-value dimensions
    long timestamp = DateTimes.of("2023-01-01").getMillis();
    Map<String, Object> event = new HashMap<>();
    event.put("dim1", Arrays.asList("a", null, "b"));

    InputRow inputRow = new MapBasedInputRow(
        timestamp,
        Collections.singletonList("dim1"),
        event
    );

    // This should not throw NPE after the fix
    List<Object> groupKey = Rows.toGroupKey(timestamp, inputRow);
    Assert.assertEquals(2, groupKey.size());
    Assert.assertEquals(timestamp, groupKey.get(0));

    @SuppressWarnings("unchecked")
    Map<String, Set<String>> dimensions = (Map<String, Set<String>>) groupKey.get(1);
    Set<String> dim1Values = dimensions.get("dim1");

    // The set should contain null, "a", "b" (with null sorted first due to naturalNullsFirst comparator)
    Assert.assertEquals(3, dim1Values.size());
    Assert.assertTrue(dim1Values.contains(null));
    Assert.assertTrue(dim1Values.contains("a"));
    Assert.assertTrue(dim1Values.contains("b"));
  }

  @Test
  public void test_toGroupKey_withOnlyNullDimension()
  {
    long timestamp = DateTimes.of("2023-01-01").getMillis();
    Map<String, Object> event = new HashMap<>();
    event.put("dim1", Collections.singletonList(null));

    InputRow inputRow = new MapBasedInputRow(
        timestamp,
        Collections.singletonList("dim1"),
        event
    );

    List<Object> groupKey = Rows.toGroupKey(timestamp, inputRow);
    Assert.assertEquals(2, groupKey.size());

    @SuppressWarnings("unchecked")
    Map<String, Set<String>> dimensions = (Map<String, Set<String>>) groupKey.get(1);
    Set<String> dim1Values = dimensions.get("dim1");

    Assert.assertEquals(1, dim1Values.size());
    Assert.assertTrue(dim1Values.contains(null));
  }

  @Test
  public void test_toGroupKey_nullsSortedFirst()
  {
    // Verify that nulls are sorted first in the TreeSet
    long timestamp = DateTimes.of("2023-01-01").getMillis();
    Map<String, Object> event = new HashMap<>();
    event.put("dim1", Arrays.asList("c", null, "a", "b"));

    InputRow inputRow = new MapBasedInputRow(
        timestamp,
        Collections.singletonList("dim1"),
        event
    );

    List<Object> groupKey = Rows.toGroupKey(timestamp, inputRow);

    @SuppressWarnings("unchecked")
    Map<String, Set<String>> dimensions = (Map<String, Set<String>>) groupKey.get(1);
    Set<String> dim1Values = dimensions.get("dim1");

    // Convert to list to check ordering
    List<String> valuesList = new ArrayList<>(dim1Values);
    Assert.assertEquals(4, valuesList.size());
    // Null should be first due to naturalNullsFirst comparator
    Assert.assertNull(valuesList.get(0));
    Assert.assertEquals("a", valuesList.get(1));
    Assert.assertEquals("b", valuesList.get(2));
    Assert.assertEquals("c", valuesList.get(3));
  }
}
