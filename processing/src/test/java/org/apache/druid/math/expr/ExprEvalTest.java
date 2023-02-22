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

package org.apache.druid.math.expr;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.NonnullPair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.TypeStrategies;
import org.apache.druid.segment.column.TypeStrategiesTest;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ExprEvalTest extends InitializedNullHandlingTest
{
  private static int MAX_SIZE_BYTES = 1 << 13;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  ByteBuffer buffer = ByteBuffer.allocate(1 << 16);

  @BeforeClass
  public static void setup()
  {
    TypeStrategies.registerComplex(
        TypeStrategiesTest.NULLABLE_TEST_PAIR_TYPE.getComplexTypeName(),
        new TypeStrategiesTest.NullableLongPairTypeStrategy()
    );
  }

  @Test
  public void testStringSerde()
  {
    assertExpr(0, "hello");
    assertExpr(1234, "hello");
    assertExpr(0, ExprEval.bestEffortOf(null));
  }

  @Test
  public void testStringSerdeTooBig()
  {
    expectedException.expect(ISE.class);
    expectedException.expectMessage(StringUtils.format(
        "Unable to serialize [%s], max size bytes is [%s], but need at least [%s] bytes to write entire value",
        ExpressionType.STRING,
        10,
        16
    ));
    assertExpr(0, ExprEval.of("hello world"), 10);
  }


  @Test
  public void testLongSerde()
  {
    assertExpr(0, 1L);
    assertExpr(1234, 1L);
    assertExpr(1234, ExprEval.ofLong(null));
  }

  @Test
  public void testDoubleSerde()
  {
    assertExpr(0, 1.123);
    assertExpr(1234, 1.123);
    assertExpr(1234, ExprEval.ofDouble(null));
  }

  @Test
  public void testStringArraySerde()
  {
    assertExpr(0, new String[]{"hello", "hi", "hey"});
    assertExpr(1024, new String[]{"hello", null, "hi", "hey"});
    assertExpr(2048, new String[]{});
  }

  @Test
  public void testStringArraySerdeToBig()
  {
    expectedException.expect(ISE.class);
    expectedException.expectMessage(StringUtils.format(
        "Unable to serialize [%s], max size bytes is [%s], but need at least [%s] bytes to write entire value",
        ExpressionType.STRING_ARRAY,
        10,
        30
    ));
    assertExpr(0, ExprEval.ofStringArray(new String[]{"hello", "hi", "hey"}), 10);
  }

  @Test
  public void testStringArrayEvalToBig()
  {
    expectedException.expect(ISE.class);
    // this has a different failure size than string serde because it doesn't check incrementally
    expectedException.expectMessage(StringUtils.format(
        "Unable to serialize [%s], max size bytes is [%s], but need at least [%s] bytes to write entire value",
        ExpressionType.STRING_ARRAY,
        10,
        30
    ));
    assertExpr(0, ExprEval.ofStringArray(new String[]{"hello", "hi", "hey"}), 10);
  }

  @Test
  public void testLongArraySerde()
  {
    assertExpr(0, new Long[]{1L, 2L, 3L});
    assertExpr(0, new long[]{1L, 2L, 3L});
    assertExpr(1234, new Long[]{1L, 2L, null, 3L});
    assertExpr(1234, new Long[]{});
  }

  @Test
  public void testLongArraySerdeTooBig()
  {
    expectedException.expect(ISE.class);
    expectedException.expectMessage(StringUtils.format(
        "Unable to serialize [%s], max size bytes is [%s], but need at least [%s] bytes to write entire value",
        ExpressionType.LONG_ARRAY,
        10,
        32
    ));
    assertExpr(0, ExprEval.ofLongArray(new Long[]{1L, 2L, 3L}), 10);
  }

  @Test
  public void testLongArrayEvalTooBig()
  {
    expectedException.expect(ISE.class);
    expectedException.expectMessage(StringUtils.format(
        "Unable to serialize [%s], max size bytes is [%s], but need at least [%s] bytes to write entire value",
        ExpressionType.LONG_ARRAY,
        10,
        32
    ));
    assertExpr(0, ExprEval.ofLongArray(new Long[]{1L, 2L, 3L}), 10);
  }

  @Test
  public void testDoubleArraySerde()
  {
    assertExpr(0, new Double[]{1.1, 2.2, 3.3});
    assertExpr(0, new double[]{1.1, 2.2, 3.3});
    assertExpr(1234, new Double[]{1.1, 2.2, null, 3.3});
    assertExpr(1234, new Double[]{});
  }

  @Test
  public void testDoubleArraySerdeTooBig()
  {
    expectedException.expect(ISE.class);
    expectedException.expectMessage(StringUtils.format(
        "Unable to serialize [%s], max size bytes is [%s], but need at least [%s] bytes to write entire value",
        ExpressionType.DOUBLE_ARRAY,
        10,
        32
    ));
    assertExpr(0, ExprEval.ofDoubleArray(new Double[]{1.1, 2.2, 3.3}), 10);
  }

  @Test
  public void testDoubleArrayEvalTooBig()
  {
    expectedException.expect(ISE.class);
    expectedException.expectMessage(StringUtils.format(
        "Unable to serialize [%s], max size bytes is [%s], but need at least [%s] bytes to write entire value",
        ExpressionType.DOUBLE_ARRAY,
        10,
        32
    ));
    assertExpr(0, ExprEval.ofDoubleArray(new Double[]{1.1, 2.2, 3.3}), 10);
  }

  @Test
  public void testComplexEval()
  {
    final ExpressionType complexType = ExpressionType.fromColumnType(TypeStrategiesTest.NULLABLE_TEST_PAIR_TYPE);
    assertExpr(0, ExprEval.ofComplex(complexType, new TypeStrategiesTest.NullableLongPair(1234L, 5678L)));
    assertExpr(1024, ExprEval.ofComplex(complexType, new TypeStrategiesTest.NullableLongPair(1234L, 5678L)));
  }

  @Test
  public void testComplexEvalTooBig()
  {
    final ExpressionType complexType = ExpressionType.fromColumnType(TypeStrategiesTest.NULLABLE_TEST_PAIR_TYPE);
    expectedException.expect(ISE.class);
    expectedException.expectMessage(StringUtils.format(
        "Unable to serialize [%s], max size bytes is [%s], but need at least [%s] bytes to write entire value",
        complexType.asTypeString(),
        10,
        19
    ));
    assertExpr(0, ExprEval.ofComplex(complexType, new TypeStrategiesTest.NullableLongPair(1234L, 5678L)), 10);
  }

  @Test
  public void test_coerceListToArray()
  {
    Assert.assertNull(ExprEval.coerceListToArray(null, false));

    NonnullPair<ExpressionType, Object[]> coerced = ExprEval.coerceListToArray(ImmutableList.of(), false);
    Assert.assertEquals(ExpressionType.STRING_ARRAY, coerced.lhs);
    Assert.assertArrayEquals(new Object[0], coerced.rhs);

    coerced = ExprEval.coerceListToArray(null, true);
    Assert.assertEquals(ExpressionType.STRING_ARRAY, coerced.lhs);
    Assert.assertArrayEquals(new Object[]{null}, coerced.rhs);

    coerced = ExprEval.coerceListToArray(ImmutableList.of(), true);
    Assert.assertEquals(ExpressionType.STRING_ARRAY, coerced.lhs);
    Assert.assertArrayEquals(new Object[]{null}, coerced.rhs);

    List<Long> longList = ImmutableList.of(1L, 2L, 3L);
    coerced = ExprEval.coerceListToArray(longList, false);
    Assert.assertEquals(ExpressionType.LONG_ARRAY, coerced.lhs);
    Assert.assertArrayEquals(new Object[]{1L, 2L, 3L}, coerced.rhs);

    List<Integer> intList = ImmutableList.of(1, 2, 3);
    ExprEval.coerceListToArray(intList, false);
    Assert.assertEquals(ExpressionType.LONG_ARRAY, coerced.lhs);
    Assert.assertArrayEquals(new Object[]{1L, 2L, 3L}, coerced.rhs);

    List<Float> floatList = ImmutableList.of(1.0f, 2.0f, 3.0f);
    coerced = ExprEval.coerceListToArray(floatList, false);
    Assert.assertEquals(ExpressionType.DOUBLE_ARRAY, coerced.lhs);
    Assert.assertArrayEquals(new Object[]{1.0, 2.0, 3.0}, coerced.rhs);

    List<Double> doubleList = ImmutableList.of(1.0, 2.0, 3.0);
    coerced = ExprEval.coerceListToArray(doubleList, false);
    Assert.assertEquals(ExpressionType.DOUBLE_ARRAY, coerced.lhs);
    Assert.assertArrayEquals(new Object[]{1.0, 2.0, 3.0}, coerced.rhs);

    List<String> stringList = ImmutableList.of("a", "b", "c");
    coerced = ExprEval.coerceListToArray(stringList, false);
    Assert.assertEquals(ExpressionType.STRING_ARRAY, coerced.lhs);
    Assert.assertArrayEquals(new Object[]{"a", "b", "c"}, coerced.rhs);

    List<String> withNulls = new ArrayList<>();
    withNulls.add("a");
    withNulls.add(null);
    withNulls.add("c");
    coerced = ExprEval.coerceListToArray(withNulls, false);
    Assert.assertEquals(ExpressionType.STRING_ARRAY, coerced.lhs);
    Assert.assertArrayEquals(new Object[]{"a", null, "c"}, coerced.rhs);

    List<Long> withNumberNulls = new ArrayList<>();
    withNumberNulls.add(1L);
    withNumberNulls.add(null);
    withNumberNulls.add(3L);

    coerced = ExprEval.coerceListToArray(withNumberNulls, false);
    Assert.assertEquals(ExpressionType.LONG_ARRAY, coerced.lhs);
    Assert.assertArrayEquals(new Object[]{1L, null, 3L}, coerced.rhs);

    List<Object> withStringMix = ImmutableList.of(1L, "b", 3L);
    coerced = ExprEval.coerceListToArray(withStringMix, false);
    Assert.assertEquals(ExpressionType.STRING_ARRAY, coerced.lhs);
    Assert.assertArrayEquals(
        new Object[]{"1", "b", "3"},
        coerced.rhs
    );

    List<Number> withIntsAndLongs = ImmutableList.of(1, 2L, 3);
    coerced = ExprEval.coerceListToArray(withIntsAndLongs, false);
    Assert.assertEquals(ExpressionType.LONG_ARRAY, coerced.lhs);
    Assert.assertArrayEquals(
        new Object[]{1L, 2L, 3L},
        coerced.rhs
    );

    List<Number> withFloatsAndLongs = ImmutableList.of(1, 2L, 3.0f);
    coerced = ExprEval.coerceListToArray(withFloatsAndLongs, false);
    Assert.assertEquals(ExpressionType.DOUBLE_ARRAY, coerced.lhs);
    Assert.assertArrayEquals(
        new Object[]{1.0, 2.0, 3.0},
        coerced.rhs
    );

    List<Number> withDoublesAndLongs = ImmutableList.of(1, 2L, 3.0);
    coerced = ExprEval.coerceListToArray(withDoublesAndLongs, false);
    Assert.assertEquals(ExpressionType.DOUBLE_ARRAY, coerced.lhs);
    Assert.assertArrayEquals(
        new Object[]{1.0, 2.0, 3.0},
        coerced.rhs
    );

    List<Number> withFloatsAndDoubles = ImmutableList.of(1L, 2.0f, 3.0);
    coerced = ExprEval.coerceListToArray(withFloatsAndDoubles, false);
    Assert.assertEquals(ExpressionType.DOUBLE_ARRAY, coerced.lhs);
    Assert.assertArrayEquals(
        new Object[]{1.0, 2.0, 3.0},
        coerced.rhs
    );

    List<String> withAllNulls = new ArrayList<>();
    withAllNulls.add(null);
    withAllNulls.add(null);
    withAllNulls.add(null);
    coerced = ExprEval.coerceListToArray(withAllNulls, false);
    Assert.assertEquals(ExpressionType.STRING_ARRAY, coerced.lhs);
    Assert.assertArrayEquals(
        new Object[]{null, null, null},
        coerced.rhs
    );

    List<List<String>> nestedLists = ImmutableList.of(
        ImmutableList.of("a", "b", "c"),
        ImmutableList.of("d", "e", "f")
    );
    coerced = ExprEval.coerceListToArray(nestedLists, false);
    Assert.assertEquals(ExpressionTypeFactory.getInstance().ofArray(ExpressionType.STRING_ARRAY), coerced.lhs);
    Assert.assertArrayEquals(
        new Object[]{new Object[]{"a", "b", "c"}, new Object[]{"d", "e", "f"}},
        coerced.rhs
    );

    Map<String, Object> unknown1 = ImmutableMap.of("x", 1L, "y", 2L);
    Map<String, Object> unknown2 = ImmutableMap.of("x", 4L, "y", 5L);
    List<Map<String, Object>> listUnknownComplex = ImmutableList.of(unknown1, unknown2);
    coerced = ExprEval.coerceListToArray(listUnknownComplex, false);
    Assert.assertEquals(ExpressionTypeFactory.getInstance().ofArray(ExpressionType.UNKNOWN_COMPLEX), coerced.lhs);
    Assert.assertArrayEquals(
        new Object[]{unknown1, unknown2},
        coerced.rhs
    );


    Map<String, Object> unknown3 = ImmutableMap.of("x", 5L, "y", 7L);
    Map<String, Object> unknown4 = ImmutableMap.of("x", 6L, "y", 8L);

    List<List<Map<String, Object>>> nestedListsComplex = ImmutableList.of(
        ImmutableList.of(unknown1, unknown2),
        ImmutableList.of(unknown3, unknown4)
    );
    coerced = ExprEval.coerceListToArray(nestedListsComplex, false);
    Assert.assertEquals(
        ExpressionTypeFactory.getInstance().ofArray(
            ExpressionTypeFactory.getInstance().ofArray(ExpressionType.UNKNOWN_COMPLEX)
        ),
        coerced.lhs
    );
    Assert.assertArrayEquals(
        new Object[]{
            new Object[]{unknown1, unknown2},
            new Object[]{unknown3, unknown4}
        },
        coerced.rhs
    );

    List<List<Object>> mixed = ImmutableList.of(
        ImmutableList.of("a", "b", "c"),
        ImmutableList.of(1L, 2L, 3L),
        ImmutableList.of(3.0, 4.0, 5.0),
        ImmutableList.of("a", 2L, 3.0)
    );
    coerced = ExprEval.coerceListToArray(mixed, false);
    Assert.assertEquals(
        ExpressionTypeFactory.getInstance().ofArray(ExpressionType.STRING_ARRAY),
        coerced.lhs
    );
    Assert.assertArrayEquals(
        new Object[]{
            new Object[]{"a", "b", "c"},
            new Object[]{"1", "2", "3"},
            new Object[]{"3.0", "4.0", "5.0"},
            new Object[]{"a", "2", "3.0"}
        },
        coerced.rhs
    );
  }

  @Test
  public void testStringArrayToNumberArray()
  {
    ExprEval someStringArray = ExprEval.ofStringArray(new String[]{"1", "2", "foo", null, "3.3"});
    Assert.assertArrayEquals(
        new Object[]{1L, 2L, NullHandling.defaultLongValue(), NullHandling.defaultLongValue(), 3L},
        someStringArray.castTo(ExpressionType.LONG_ARRAY).asArray()
    );
    Assert.assertArrayEquals(
        new Object[]{1.0, 2.0, NullHandling.defaultDoubleValue(), NullHandling.defaultDoubleValue(), 3.3},
        someStringArray.castTo(ExpressionType.DOUBLE_ARRAY).asArray()
    );
  }

  @Test
  public void testEmptyArrayFromList()
  {
    // empty arrays will materialize from JSON into an empty list, which coerce list to array will make into Object[]
    // make sure we can handle it
    ExprEval someEmptyArray = ExprEval.bestEffortOf(new ArrayList<>());
    Assert.assertTrue(someEmptyArray.isArray());
    Assert.assertEquals(0, someEmptyArray.asArray().length);
  }

  private void assertExpr(int position, Object expected)
  {
    assertExpr(position, ExprEval.bestEffortOf(expected));
  }

  private void assertExpr(int position, ExprEval expected)
  {
    assertExpr(position, expected, MAX_SIZE_BYTES);
  }

  private void assertExpr(int position, ExprEval expected, int maxSizeBytes)
  {
    ExprEval.serialize(buffer, position, expected.type(), expected, maxSizeBytes);
    if (expected.type().isArray()) {
      Assert.assertArrayEquals(
          "deserialized value with buffer references allowed",
          expected.asArray(),
          ExprEval.deserialize(buffer, position, MAX_SIZE_BYTES, expected.type(), true).asArray()
      );

      Assert.assertArrayEquals(
          "deserialized value with buffer references not allowed",
          expected.asArray(),
          ExprEval.deserialize(buffer, position, MAX_SIZE_BYTES, expected.type(), false).asArray()
      );
    } else {
      Assert.assertEquals(
          "deserialized value with buffer references allowed",
          expected.value(),
          ExprEval.deserialize(buffer, position, MAX_SIZE_BYTES, expected.type(), true).value()
      );

      Assert.assertEquals(
          "deserialized value with buffer references not allowed",
          expected.value(),
          ExprEval.deserialize(buffer, position, MAX_SIZE_BYTES, expected.type(), false).value()
      );
    }
  }
}
