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

package org.apache.druid.sql.calcite.run;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.data.ComparableList;
import org.apache.druid.segment.data.ComparableStringArray;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.hamcrest.CoreMatchers;
import org.joda.time.DateTimeZone;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

class SqlResultsTest extends InitializedNullHandlingTest
{
  private static final SqlResults.Context DEFAULT_CONTEXT = new SqlResults.Context(DateTimeZone.UTC, true, false);

  private ObjectMapper jsonMapper;

  @BeforeEach
  void setUp()
  {
    jsonMapper = TestHelper.JSON_MAPPER;
  }

  @Test
  void coerceStringArrays()
  {
    final List<String> stringList = Arrays.asList("x", "y", "z", null);
    final Object[] stringArray = new Object[]{"x", "y", "z", null};
    final ComparableStringArray comparableStringArray = ComparableStringArray.of(new String[]{"x", "y", "z", null});
    final String[] stringArray2 = new String[]{"x", "y", "z", null};

    assertCoerceArrayToList(stringList, stringList);
    assertCoerceArrayToList(stringList, stringArray);
    assertCoerceArrayToList(stringList, stringArray2);
    assertCoerceArrayToList(stringList, comparableStringArray);
  }

  @Test
  void coerceLongArrays()
  {
    final List<Long> listWithNull = Arrays.asList(1L, 2L, null, 3L);
    final Long[] arrayWithNull = new Long[]{1L, 2L, null, 3L};
    final ComparableList<Long> comparableList = new ComparableList<>(listWithNull);
    final List<Long> list = Arrays.asList(1L, 2L, 3L);
    final long[] array = new long[]{1L, 2L, 3L};

    assertCoerceArrayToList(listWithNull, listWithNull);
    assertCoerceArrayToList(listWithNull, arrayWithNull);
    assertCoerceArrayToList(listWithNull, comparableList);
    assertCoerceArrayToList(list, list);
    assertCoerceArrayToList(list, array);
  }

  @Test
  void coerceDoubleArrays()
  {
    final List<Double> listWithNull = Arrays.asList(1.1, 2.2, null, 3.3);
    final Double[] arrayWithNull = new Double[]{1.1, 2.2, null, 3.3};
    final ComparableList<Double> comparableList = new ComparableList<>(listWithNull);
    final List<Double> list = Arrays.asList(1.1, 2.2, 3.3);
    final double[] array = new double[]{1.1, 2.2, 3.3};

    assertCoerceArrayToList(listWithNull, listWithNull);
    assertCoerceArrayToList(listWithNull, arrayWithNull);
    assertCoerceArrayToList(listWithNull, comparableList);
    assertCoerceArrayToList(list, list);
    assertCoerceArrayToList(list, array);
  }

  @Test
  void coerceFloatArrays()
  {
    final List<Float> listWithNull = Arrays.asList(1.1f, 2.2f, null, 3.3f);
    final Float[] arrayWithNull = new Float[]{1.1f, 2.2f, null, 3.3f};
    final ComparableList<Float> comparableList = new ComparableList<>(listWithNull);
    final List<Float> list = Arrays.asList(1.1f, 2.2f, 3.3f);
    final float[] array = new float[]{1.1f, 2.2f, 3.3f};

    assertCoerceArrayToList(listWithNull, listWithNull);
    assertCoerceArrayToList(listWithNull, arrayWithNull);
    assertCoerceArrayToList(listWithNull, comparableList);
    assertCoerceArrayToList(list, list);
    assertCoerceArrayToList(list, array);
  }

  @Test
  void coerceNestedArrays()
  {
    List<?> nestedList = Arrays.asList(Arrays.asList(1L, 2L, 3L), Arrays.asList(4L, 5L, 6L));
    Object[] nestedArray = new Object[]{new Object[]{1L, 2L, 3L}, new Object[]{4L, 5L, 6L}};

    assertCoerceArrayToList(nestedList, nestedList);
    assertCoerceArrayToList(nestedList, nestedArray);
  }

  @Test
  void coerceBoolean()
  {
    assertCoerce(false, false, SqlTypeName.BOOLEAN);
    assertCoerce(false, "xyz", SqlTypeName.BOOLEAN);
    assertCoerce(false, 0, SqlTypeName.BOOLEAN);
    assertCoerce(false, "false", SqlTypeName.BOOLEAN);
    assertCoerce(true, true, SqlTypeName.BOOLEAN);
    assertCoerce(true, "true", SqlTypeName.BOOLEAN);
    assertCoerce(true, 1, SqlTypeName.BOOLEAN);
    assertCoerce(true, 1.0, SqlTypeName.BOOLEAN);
    assertCoerce(null, null, SqlTypeName.BOOLEAN);

    assertCannotCoerce(Collections.emptyList(), SqlTypeName.BOOLEAN);
  }

  @Test
  void coerceInteger()
  {
    assertCoerce(0, 0, SqlTypeName.INTEGER);
    assertCoerce(1, 1L, SqlTypeName.INTEGER);
    assertCoerce(1, 1f, SqlTypeName.INTEGER);
    assertCoerce(1, "1", SqlTypeName.INTEGER);
    assertCoerce(null, "1.1", SqlTypeName.INTEGER);
    assertCoerce(null, "xyz", SqlTypeName.INTEGER);
    assertCoerce(null, null, SqlTypeName.INTEGER);

    assertCannotCoerce(Collections.emptyList(), SqlTypeName.INTEGER);
    assertCannotCoerce(false, SqlTypeName.INTEGER);
  }

  @Test
  void coerceBigint()
  {
    assertCoerce(0L, 0, SqlTypeName.BIGINT);
    assertCoerce(1L, 1L, SqlTypeName.BIGINT);
    assertCoerce(1L, 1f, SqlTypeName.BIGINT);
    assertCoerce(null, "1.1", SqlTypeName.BIGINT);
    assertCoerce(null, "xyz", SqlTypeName.BIGINT);
    assertCoerce(null, null, SqlTypeName.BIGINT);

    // Inconsistency with FLOAT, INTEGER, DOUBLE.
    assertCoerce(0L, false, SqlTypeName.BIGINT);
    assertCoerce(1L, true, SqlTypeName.BIGINT);

    assertCannotCoerce(Collections.emptyList(), SqlTypeName.BIGINT);
    assertCannotCoerce(new byte[]{(byte) 0xe0, 0x4f}, SqlTypeName.BIGINT);
  }

  @Test
  void coerceFloat()
  {
    assertCoerce(0f, 0, SqlTypeName.FLOAT);
    assertCoerce(1f, 1L, SqlTypeName.FLOAT);
    assertCoerce(1f, 1f, SqlTypeName.FLOAT);
    assertCoerce(1.1f, "1.1", SqlTypeName.FLOAT);
    assertCoerce(null, "xyz", SqlTypeName.FLOAT);
    assertCoerce(null, null, SqlTypeName.FLOAT);

    assertCannotCoerce(Collections.emptyList(), SqlTypeName.FLOAT);
    assertCannotCoerce(false, SqlTypeName.FLOAT);
  }

  @Test
  void coerceDouble()
  {
    assertCoerce(0d, 0, SqlTypeName.DOUBLE);
    assertCoerce(1d, 1L, SqlTypeName.DOUBLE);
    assertCoerce(1d, 1f, SqlTypeName.DOUBLE);
    assertCoerce(1.1d, "1.1", SqlTypeName.DOUBLE);
    assertCoerce(null, "xyz", SqlTypeName.DOUBLE);
    assertCoerce(null, null, SqlTypeName.DOUBLE);

    assertCannotCoerce(Collections.emptyList(), SqlTypeName.DOUBLE);
    assertCannotCoerce(false, SqlTypeName.DOUBLE);
  }

  @Test
  void coerceString()
  {
    assertCoerce(NullHandling.defaultStringValue(), null, SqlTypeName.VARCHAR);
    assertCoerce("1", 1, SqlTypeName.VARCHAR);
    assertCoerce("true", true, SqlTypeName.VARCHAR);
    assertCoerce("abc", "abc", SqlTypeName.VARCHAR);

    assertCoerce("[\"abc\",\"def\"]", ImmutableList.of("abc", "def"), SqlTypeName.VARCHAR);
    assertCoerce("[\"abc\",\"def\"]", ImmutableSortedSet.of("abc", "def"), SqlTypeName.VARCHAR);
    assertCoerce("[\"abc\",\"def\"]", new String[]{"abc", "def"}, SqlTypeName.VARCHAR);
    assertCoerce("[\"abc\",\"def\"]", new Object[]{"abc", "def"}, SqlTypeName.VARCHAR);

    assertCoerce("[\"abc\"]", ImmutableList.of("abc"), SqlTypeName.VARCHAR);
    assertCoerce("[\"abc\"]", ImmutableSortedSet.of("abc"), SqlTypeName.VARCHAR);
    assertCoerce("[\"abc\"]", new String[]{"abc"}, SqlTypeName.VARCHAR);
    assertCoerce("[\"abc\"]", new Object[]{"abc"}, SqlTypeName.VARCHAR);

    assertCannotCoerce(new Object(), SqlTypeName.VARCHAR);
  }

  @Test
  void coerceOfArrayOfPrimitives()
  {
    try {
      assertCoerce("", new byte[1], SqlTypeName.BIGINT);
      fail("Should throw an exception");
    }
    catch (Exception e) {
      assertEquals("Cannot coerce field [fieldName] from type [Byte Array] to type [BIGINT]", e.getMessage());
    }
  }

  @Test
  void coerceArrayFails()
  {
    assertCannotCoerce("xyz", SqlTypeName.ARRAY);
  }

  @Test
  void coerceUnsupportedType()
  {
    assertCannotCoerce("xyz", SqlTypeName.VARBINARY);
  }

  @Test
  void mustCoerce()
  {
    assertNull(SqlResults.maybeCoerceArrayToList("hello", true));
  }

  @Test
  void mayNotCoerce()
  {
    assertEquals("hello", SqlResults.maybeCoerceArrayToList("hello", false));
  }

  private void assertCoerce(Object expected, Object toCoerce, SqlTypeName typeName)
  {
    assertEquals(
        expected,
        SqlResults.coerce(jsonMapper, DEFAULT_CONTEXT, toCoerce, typeName, "fieldName"),
        StringUtils.format("Coerce [%s] to [%s]", toCoerce, typeName)
    );
  }

  private void assertCannotCoerce(Object toCoerce, SqlTypeName typeName)
  {
    final DruidException e = assertThrows(
        DruidException.class,
        () -> SqlResults.coerce(jsonMapper, DEFAULT_CONTEXT, toCoerce, typeName, ""),
        StringUtils.format("Coerce [%s] to [%s]", toCoerce, typeName)
    );

    assertThat(e, ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString("Cannot coerce")));
  }

  private static void assertCoerceArrayToList(Object expected, Object toCoerce)
  {
    Object coerced = SqlResults.maybeCoerceArrayToList(toCoerce, true);
    assertEquals(expected, coerced);
  }
}
