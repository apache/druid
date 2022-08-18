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

import org.apache.druid.segment.data.ComparableList;
import org.apache.druid.segment.data.ComparableStringArray;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class NativeQueryMakerTest
{

  @Test
  public void testCoerceStringArrays()
  {
    final List<String> stringList = Arrays.asList("x", "y", "z", null);
    final Object[] stringArray = new Object[]{"x", "y", "z", null};
    final ComparableStringArray comparableStringArray = ComparableStringArray.of(new String[]{"x", "y", "z", null});
    final String[] stringArray2 = new String[]{"x", "y", "z", null};

    assertCoerced(stringList, stringList, true);
    assertCoerced(stringList, stringArray, true);
    assertCoerced(stringList, stringArray2, true);
    assertCoerced(stringList, comparableStringArray, true);
  }

  @Test
  public void testCoerceLongArrays()
  {
    final List<Long> listWithNull = Arrays.asList(1L, 2L, null, 3L);
    final Long[] arrayWithNull = new Long[]{1L, 2L, null, 3L};
    final ComparableList<Long> comparableList = new ComparableList<>(listWithNull);
    final List<Long> list = Arrays.asList(1L, 2L, 3L);
    final long[] array = new long[]{1L, 2L, 3L};

    assertCoerced(listWithNull, listWithNull, true);
    assertCoerced(listWithNull, arrayWithNull, true);
    assertCoerced(listWithNull, comparableList, true);
    assertCoerced(list, list, true);
    assertCoerced(list, array, true);
  }

  @Test
  public void testCoerceDoubleArrays()
  {
    final List<Double> listWithNull = Arrays.asList(1.1, 2.2, null, 3.3);
    final Double[] arrayWithNull = new Double[]{1.1, 2.2, null, 3.3};
    final ComparableList<Double> comparableList = new ComparableList<>(listWithNull);
    final List<Double> list = Arrays.asList(1.1, 2.2, 3.3);
    final double[] array = new double[]{1.1, 2.2, 3.3};

    assertCoerced(listWithNull, listWithNull, true);
    assertCoerced(listWithNull, arrayWithNull, true);
    assertCoerced(listWithNull, comparableList, true);
    assertCoerced(list, list, true);
    assertCoerced(list, array, true);
  }

  @Test
  public void testCoerceFloatArrays()
  {
    final List<Float> listWithNull = Arrays.asList(1.1f, 2.2f, null, 3.3f);
    final Float[] arrayWithNull = new Float[]{1.1f, 2.2f, null, 3.3f};
    final ComparableList<Float> comparableList = new ComparableList<>(listWithNull);
    final List<Float> list = Arrays.asList(1.1f, 2.2f, 3.3f);
    final float[] array = new float[]{1.1f, 2.2f, 3.3f};

    assertCoerced(listWithNull, listWithNull, true);
    assertCoerced(listWithNull, arrayWithNull, true);
    assertCoerced(listWithNull, comparableList, true);
    assertCoerced(list, list, true);
    assertCoerced(list, array, true);
  }

  @Test
  public void testCoerceNestedArrays()
  {
    List<?> nestedList = Arrays.asList(Arrays.asList(1L, 2L, 3L), Arrays.asList(4L, 5L, 6L));
    Object[] nestedArray = new Object[]{new Object[]{1L, 2L, 3L}, new Object[]{4L, 5L, 6L}};

    assertCoerced(nestedList, nestedList, true);
    assertCoerced(nestedList, nestedArray, true);
  }

  @Test
  public void testMustCoerce()
  {
    Assert.assertNull(NativeQueryMaker.maybeCoerceArrayToList("hello", true));
  }

  private static void assertCoerced(Object expected, Object toCoerce, boolean mustCoerce)
  {
    Object coerced = NativeQueryMaker.maybeCoerceArrayToList(toCoerce, mustCoerce);
    Assert.assertEquals(expected, coerced);
  }
}
