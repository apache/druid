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
import org.apache.druid.collections.SerializablePair;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.NonnullPair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.TypeStrategies;
import org.apache.druid.segment.column.TypeStrategiesTest;
import org.apache.druid.segment.column.Types;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ExprEvalTest extends InitializedNullHandlingTest
{
  private static final int MAX_SIZE_BYTES = 1 << 13;

  private final ByteBuffer buffer = ByteBuffer.allocate(1 << 16);

  @BeforeAll
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
    final String expectedMessage = StringUtils.format(
        "Unable to serialize [%s], max size bytes is [%s], but need at least [%s] bytes to write entire value",
        ExpressionType.STRING,
        10,
        16
    );
    final ISE exception = Assertions.assertThrows(
        ISE.class,
        () -> assertExpr(0, ExprEval.ofString("hello world"), 10)
    );
    Assertions.assertEquals(expectedMessage, exception.getMessage());
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
    final String expectedMessage = StringUtils.format(
        "Unable to serialize [%s], max size bytes is [%s], but need at least [%s] bytes to write entire value",
        ExpressionType.STRING_ARRAY,
        10,
        30
    );
    final ISE exception = Assertions.assertThrows(
        ISE.class,
        () -> assertExpr(0, ExprEval.ofStringArray(new String[]{"hello", "hi", "hey"}), 10)
    );
    Assertions.assertEquals(expectedMessage, exception.getMessage());
  }

  @Test
  public void testStringArrayEvalToBig()
  {
    // this has a different failure size than string serde because it doesn't check incrementally
    final String expectedMessage = StringUtils.format(
        "Unable to serialize [%s], max size bytes is [%s], but need at least [%s] bytes to write entire value",
        ExpressionType.STRING_ARRAY,
        10,
        30
    );
    final ISE exception = Assertions.assertThrows(
        ISE.class,
        () -> assertExpr(0, ExprEval.ofStringArray(new String[]{"hello", "hi", "hey"}), 10)
    );
    Assertions.assertEquals(expectedMessage, exception.getMessage());
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
    final String expectedMessage = StringUtils.format(
        "Unable to serialize [%s], max size bytes is [%s], but need at least [%s] bytes to write entire value",
        ExpressionType.LONG_ARRAY,
        10,
        32
    );
    final ISE exception = Assertions.assertThrows(
        ISE.class,
        () -> assertExpr(0, ExprEval.ofLongArray(new Long[]{1L, 2L, 3L}), 10)
    );
    Assertions.assertEquals(expectedMessage, exception.getMessage());
  }

  @Test
  public void testLongArrayEvalTooBig()
  {
    final String expectedMessage = StringUtils.format(
        "Unable to serialize [%s], max size bytes is [%s], but need at least [%s] bytes to write entire value",
        ExpressionType.LONG_ARRAY,
        10,
        32
    );
    final ISE exception = Assertions.assertThrows(
        ISE.class,
        () -> assertExpr(0, ExprEval.ofLongArray(new Long[]{1L, 2L, 3L}), 10)
    );
    Assertions.assertEquals(expectedMessage, exception.getMessage());
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
    final String expectedMessage = StringUtils.format(
        "Unable to serialize [%s], max size bytes is [%s], but need at least [%s] bytes to write entire value",
        ExpressionType.DOUBLE_ARRAY,
        10,
        32
    );
    final ISE exception = Assertions.assertThrows(
        ISE.class,
        () -> assertExpr(0, ExprEval.ofDoubleArray(new Double[]{1.1, 2.2, 3.3}), 10)
    );
    Assertions.assertEquals(expectedMessage, exception.getMessage());
  }

  @Test
  public void testDoubleArrayEvalTooBig()
  {
    final String expectedMessage = StringUtils.format(
        "Unable to serialize [%s], max size bytes is [%s], but need at least [%s] bytes to write entire value",
        ExpressionType.DOUBLE_ARRAY,
        10,
        32
    );
    final ISE exception = Assertions.assertThrows(
        ISE.class,
        () -> assertExpr(0, ExprEval.ofDoubleArray(new Double[]{1.1, 2.2, 3.3}), 10)
    );
    Assertions.assertEquals(expectedMessage, exception.getMessage());
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
    final String expectedMessage = StringUtils.format(
        "Unable to serialize [%s], max size bytes is [%s], but need at least [%s] bytes to write entire value",
        complexType.asTypeString(),
        10,
        19
    );
    final ISE exception = Assertions.assertThrows(
        ISE.class,
        () -> assertExpr(
            0,
            ExprEval.ofComplex(complexType, new TypeStrategiesTest.NullableLongPair(1234L, 5678L)),
            10
        )
    );
    Assertions.assertEquals(expectedMessage, exception.getMessage());
  }

  @Test
  public void test_coerceListToArray()
  {
    Assertions.assertNull(ExprEval.coerceListToArray(null, false));

    NonnullPair<ExpressionType, Object[]> coerced = ExprEval.coerceListToArray(ImmutableList.of(), false);
    Assertions.assertEquals(ExpressionType.STRING_ARRAY, coerced.lhs);
    Assertions.assertArrayEquals(new Object[0], coerced.rhs);

    coerced = ExprEval.coerceListToArray(null, true);
    Assertions.assertEquals(ExpressionType.STRING_ARRAY, coerced.lhs);
    Assertions.assertArrayEquals(new Object[]{null}, coerced.rhs);

    coerced = ExprEval.coerceListToArray(ImmutableList.of(), true);
    Assertions.assertEquals(ExpressionType.STRING_ARRAY, coerced.lhs);
    Assertions.assertArrayEquals(new Object[]{null}, coerced.rhs);

    List<Long> longList = ImmutableList.of(1L, 2L, 3L);
    coerced = ExprEval.coerceListToArray(longList, false);
    Assertions.assertEquals(ExpressionType.LONG_ARRAY, coerced.lhs);
    Assertions.assertArrayEquals(new Object[]{1L, 2L, 3L}, coerced.rhs);

    List<Integer> intList = ImmutableList.of(1, 2, 3);
    ExprEval.coerceListToArray(intList, false);
    Assertions.assertEquals(ExpressionType.LONG_ARRAY, coerced.lhs);
    Assertions.assertArrayEquals(new Object[]{1L, 2L, 3L}, coerced.rhs);

    List<Float> floatList = ImmutableList.of(1.0f, 2.0f, 3.0f);
    coerced = ExprEval.coerceListToArray(floatList, false);
    Assertions.assertEquals(ExpressionType.DOUBLE_ARRAY, coerced.lhs);
    Assertions.assertArrayEquals(new Object[]{1.0, 2.0, 3.0}, coerced.rhs);

    List<Double> doubleList = ImmutableList.of(1.0, 2.0, 3.0);
    coerced = ExprEval.coerceListToArray(doubleList, false);
    Assertions.assertEquals(ExpressionType.DOUBLE_ARRAY, coerced.lhs);
    Assertions.assertArrayEquals(new Object[]{1.0, 2.0, 3.0}, coerced.rhs);

    List<String> stringList = ImmutableList.of("a", "b", "c");
    coerced = ExprEval.coerceListToArray(stringList, false);
    Assertions.assertEquals(ExpressionType.STRING_ARRAY, coerced.lhs);
    Assertions.assertArrayEquals(new Object[]{"a", "b", "c"}, coerced.rhs);

    List<String> withNulls = new ArrayList<>();
    withNulls.add("a");
    withNulls.add(null);
    withNulls.add("c");
    coerced = ExprEval.coerceListToArray(withNulls, false);
    Assertions.assertEquals(ExpressionType.STRING_ARRAY, coerced.lhs);
    Assertions.assertArrayEquals(new Object[]{"a", null, "c"}, coerced.rhs);

    List<Long> withNumberNulls = new ArrayList<>();
    withNumberNulls.add(1L);
    withNumberNulls.add(null);
    withNumberNulls.add(3L);

    coerced = ExprEval.coerceListToArray(withNumberNulls, false);
    Assertions.assertEquals(ExpressionType.LONG_ARRAY, coerced.lhs);
    Assertions.assertArrayEquals(new Object[]{1L, null, 3L}, coerced.rhs);

    List<Object> withStringMix = ImmutableList.of(1L, "b", 3L);
    coerced = ExprEval.coerceListToArray(withStringMix, false);
    Assertions.assertEquals(ExpressionType.STRING_ARRAY, coerced.lhs);
    Assertions.assertArrayEquals(
        new Object[]{"1", "b", "3"},
        coerced.rhs
    );

    List<Number> withIntsAndLongs = ImmutableList.of(1, 2L, 3);
    coerced = ExprEval.coerceListToArray(withIntsAndLongs, false);
    Assertions.assertEquals(ExpressionType.LONG_ARRAY, coerced.lhs);
    Assertions.assertArrayEquals(
        new Object[]{1L, 2L, 3L},
        coerced.rhs
    );

    List<Number> withFloatsAndLongs = ImmutableList.of(1, 2L, 3.0f);
    coerced = ExprEval.coerceListToArray(withFloatsAndLongs, false);
    Assertions.assertEquals(ExpressionType.DOUBLE_ARRAY, coerced.lhs);
    Assertions.assertArrayEquals(
        new Object[]{1.0, 2.0, 3.0},
        coerced.rhs
    );

    List<Number> withDoublesAndLongs = ImmutableList.of(1, 2L, 3.0);
    coerced = ExprEval.coerceListToArray(withDoublesAndLongs, false);
    Assertions.assertEquals(ExpressionType.DOUBLE_ARRAY, coerced.lhs);
    Assertions.assertArrayEquals(
        new Object[]{1.0, 2.0, 3.0},
        coerced.rhs
    );

    List<Number> withFloatsAndDoubles = ImmutableList.of(1L, 2.0f, 3.0);
    coerced = ExprEval.coerceListToArray(withFloatsAndDoubles, false);
    Assertions.assertEquals(ExpressionType.DOUBLE_ARRAY, coerced.lhs);
    Assertions.assertArrayEquals(
        new Object[]{1.0, 2.0, 3.0},
        coerced.rhs
    );

    List<String> withAllNulls = new ArrayList<>();
    withAllNulls.add(null);
    withAllNulls.add(null);
    withAllNulls.add(null);
    coerced = ExprEval.coerceListToArray(withAllNulls, false);
    Assertions.assertEquals(ExpressionType.STRING_ARRAY, coerced.lhs);
    Assertions.assertArrayEquals(
        new Object[]{null, null, null},
        coerced.rhs
    );

    Map<String, Object> nested1 = ImmutableMap.of("x", 1L, "y", 2L);
    List<Object> mixedObject = ImmutableList.of(
        "a",
        1L,
        3.0,
        nested1
    );
    coerced = ExprEval.coerceListToArray(mixedObject, false);
    Assertions.assertEquals(
        ExpressionTypeFactory.getInstance().ofArray(ExpressionType.NESTED_DATA),
        coerced.lhs
    );
    Assertions.assertArrayEquals(
        new Object[]{
            "a",
            1L,
            3.0,
            nested1
        },
        coerced.rhs
    );

    List<Object> mixedObject2 = ImmutableList.of(
        nested1,
        "a",
        1L,
        3.0
    );
    coerced = ExprEval.coerceListToArray(mixedObject2, false);
    Assertions.assertEquals(
        ExpressionTypeFactory.getInstance().ofArray(ExpressionType.NESTED_DATA),
        coerced.lhs
    );
    Assertions.assertArrayEquals(
        new Object[]{
            nested1,
            "a",
            1L,
            3.0
        },
        coerced.rhs
    );

    List<List<String>> nestedLists = ImmutableList.of(
        ImmutableList.of("a", "b", "c"),
        ImmutableList.of("d", "e", "f")
    );
    coerced = ExprEval.coerceListToArray(nestedLists, false);
    Assertions.assertEquals(ExpressionTypeFactory.getInstance().ofArray(ExpressionType.STRING_ARRAY), coerced.lhs);
    Assertions.assertArrayEquals(
        new Object[]{new Object[]{"a", "b", "c"}, new Object[]{"d", "e", "f"}},
        coerced.rhs
    );


    Map<String, Object> nested2 = ImmutableMap.of("x", 4L, "y", 5L);
    List<Map<String, Object>> listUnknownComplex = ImmutableList.of(nested1, nested2);
    coerced = ExprEval.coerceListToArray(listUnknownComplex, false);
    Assertions.assertEquals(ExpressionTypeFactory.getInstance().ofArray(ExpressionType.NESTED_DATA), coerced.lhs);
    Assertions.assertArrayEquals(
        new Object[]{nested1, nested2},
        coerced.rhs
    );


    Map<String, Object> nested3 = ImmutableMap.of("x", 5L, "y", 7L);
    Map<String, Object> nested4 = ImmutableMap.of("x", 6L, "y", 8L);

    List<List<Map<String, Object>>> nestedListsComplex = ImmutableList.of(
        ImmutableList.of(nested1, nested2),
        ImmutableList.of(nested3, nested4)
    );
    coerced = ExprEval.coerceListToArray(nestedListsComplex, false);
    Assertions.assertEquals(
        ExpressionTypeFactory.getInstance().ofArray(
            ExpressionTypeFactory.getInstance().ofArray(ExpressionType.NESTED_DATA)
        ),
        coerced.lhs
    );
    Assertions.assertArrayEquals(
        new Object[]{
            new Object[]{nested1, nested2},
            new Object[]{nested3, nested4}
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
    Assertions.assertEquals(
        ExpressionTypeFactory.getInstance().ofArray(ExpressionType.STRING_ARRAY),
        coerced.lhs
    );
    Assertions.assertArrayEquals(
        new Object[]{
            new Object[]{"a", "b", "c"},
            new Object[]{"1", "2", "3"},
            new Object[]{"3.0", "4.0", "5.0"},
            new Object[]{"a", "2", "3.0"}
        },
        coerced.rhs
    );

    List<List<Object>> mixedNested = ImmutableList.of(
        ImmutableList.of("a", "b", "c"),
        ImmutableList.of(1L, 2L, 3L),
        ImmutableList.of(3.0, 4.0, 5.0),
        ImmutableList.of("a", 2L, 3.0, nested1)
    );
    coerced = ExprEval.coerceListToArray(mixedNested, false);
    Assertions.assertEquals(
        ExpressionTypeFactory.getInstance().ofArray(
            ExpressionTypeFactory.getInstance().ofArray(ExpressionType.NESTED_DATA)
        ),
        coerced.lhs
    );
    Assertions.assertArrayEquals(
        new Object[]{
            new Object[]{"a", "b", "c"},
            new Object[]{1L, 2L, 3L},
            new Object[]{3.0, 4.0, 5.0},
            new Object[]{"a", 2L, 3.0, nested1}
        },
        coerced.rhs
    );

    List<Object> mixedNested2 = ImmutableList.of(
        "a",
        1L,
        3.0,
        ImmutableList.of("a", "b", "c"),
        ImmutableList.of(1L, 2L, 3L),
        ImmutableList.of(3.0, 4.0, 5.0),
        ImmutableList.of(nested1, nested2, nested3)
    );
    coerced = ExprEval.coerceListToArray(mixedNested2, false);
    Assertions.assertEquals(
        ExpressionTypeFactory.getInstance().ofArray(
            ExpressionTypeFactory.getInstance().ofArray(ExpressionType.NESTED_DATA)
        ),
        coerced.lhs
    );
    Assertions.assertArrayEquals(
        new Object[]{
            new Object[]{"a"},
            new Object[]{1L},
            new Object[]{3.0},
            new Object[]{"a", "b", "c"},
            new Object[]{1L, 2L, 3L},
            new Object[]{3.0, 4.0, 5.0},
            new Object[]{nested1, nested2, nested3}
        },
        coerced.rhs
    );


    List<Object> mixedNested3 = ImmutableList.of(
        "a",
        1L,
        3.0,
        nested1,
        ImmutableList.of("a", "b", "c"),
        ImmutableList.of(1L, 2L, 3L),
        ImmutableList.of(3.0, 4.0, 5.0),
        ImmutableList.of(nested1, nested2, nested3)
    );
    coerced = ExprEval.coerceListToArray(mixedNested3, false);
    // this one is only ARRAY<COMPLEX<json>> instead of ARRAY<ARRAY<COMPLEX<json>> because of a COMPLEX<json> element..
    Assertions.assertEquals(
        ExpressionTypeFactory.getInstance().ofArray(ExpressionType.NESTED_DATA),
        coerced.lhs
    );
    Assertions.assertArrayEquals(
        new Object[]{
            "a",
            1L,
            3.0,
            nested1,
            new Object[]{"a", "b", "c"},
            new Object[]{1L, 2L, 3L},
            new Object[]{3.0, 4.0, 5.0},
            new Object[]{nested1, nested2, nested3}
        },
        coerced.rhs
    );

    List<List<Object>> mixedUnknown = ImmutableList.of(
        ImmutableList.of("a", "b", "c"),
        ImmutableList.of(1L, 2L, 3L),
        ImmutableList.of(3.0, 4.0, 5.0, new SerializablePair<>("hello", 1234L)),
        ImmutableList.of("a", 2L, 3.0, nested1)
    );
    Throwable t = Assertions.assertThrows(
        Types.IncompatibleTypeException.class,
        () -> ExprEval.coerceListToArray(mixedUnknown, false)
    );
    Assertions.assertEquals("Cannot implicitly cast [DOUBLE] to [COMPLEX]", t.getMessage());
  }

  @Test
  public void testCastString()
  {
    ExprEval<?> eval = ExprEval.ofString("hello");

    ExprEval<?> cast = eval.castTo(ExpressionType.DOUBLE);
    Assertions.assertNull(cast.value());

    cast = eval.castTo(ExpressionType.LONG);
    Assertions.assertNull(cast.value());

    cast = eval.castTo(ExpressionType.STRING_ARRAY);
    Assertions.assertArrayEquals(new Object[]{"hello"}, (Object[]) cast.value());

    cast = eval.castTo(ExpressionType.LONG_ARRAY);
    Assertions.assertArrayEquals(new Object[]{null}, (Object[]) cast.value());

    cast = eval.castTo(ExpressionType.DOUBLE_ARRAY);
    Assertions.assertArrayEquals(new Object[]{null}, (Object[]) cast.value());

    cast = eval.castTo(ExpressionType.NESTED_DATA);
    Assertions.assertEquals("hello", cast.value());

    cast = eval.castTo(ExpressionTypeFactory.getInstance().ofArray(ExpressionType.NESTED_DATA));
    Assertions.assertArrayEquals(new Object[]{"hello"}, (Object[]) cast.value());

    eval = ExprEval.ofString("1234.3");

    cast = eval.castTo(ExpressionType.DOUBLE);
    Assertions.assertEquals(1234.3, cast.value());

    cast = eval.castTo(ExpressionType.LONG);
    Assertions.assertEquals(1234L, cast.value());

    cast = eval.castTo(ExpressionType.DOUBLE_ARRAY);
    Assertions.assertArrayEquals(new Object[]{1234.3}, (Object[]) cast.value());

    cast = eval.castTo(ExpressionType.LONG_ARRAY);
    Assertions.assertArrayEquals(new Object[]{1234L}, (Object[]) cast.value());

    eval = ExprEval.ofType(ExpressionType.STRING, null);

    cast = eval.castTo(ExpressionType.DOUBLE);
    Assertions.assertNull(cast.value());

    cast = eval.castTo(ExpressionType.LONG);
    Assertions.assertNull(cast.value());

    cast = eval.castTo(ExpressionType.STRING_ARRAY);
    Assertions.assertNull(cast.value());

    cast = eval.castTo(ExpressionType.LONG_ARRAY);
    Assertions.assertNull(cast.value());

    cast = eval.castTo(ExpressionType.DOUBLE_ARRAY);
    Assertions.assertNull(cast.value());

    cast = eval.castTo(ExpressionType.NESTED_DATA);
    Assertions.assertNull(cast.value());
  }

  @Test
  public void testCastDouble()
  {
    ExprEval<?> eval = ExprEval.of(123.4);

    ExprEval<?> cast = eval.castTo(ExpressionType.STRING);
    Assertions.assertEquals("123.4", cast.value());

    cast = eval.castTo(ExpressionType.LONG);
    Assertions.assertEquals(123L, cast.value());

    cast = eval.castTo(ExpressionType.STRING_ARRAY);
    Assertions.assertArrayEquals(new Object[]{"123.4"}, (Object[]) cast.value());

    cast = eval.castTo(ExpressionType.LONG_ARRAY);
    Assertions.assertArrayEquals(new Object[]{123L}, (Object[]) cast.value());

    cast = eval.castTo(ExpressionType.DOUBLE_ARRAY);
    Assertions.assertArrayEquals(new Object[]{123.4}, (Object[]) cast.value());

    cast = eval.castTo(ExpressionType.NESTED_DATA);
    Assertions.assertEquals(123.4, cast.value());

    eval = ExprEval.ofType(ExpressionType.DOUBLE, null);

    cast = eval.castTo(ExpressionType.STRING);
    Assertions.assertNull(cast.value());

    cast = eval.castTo(ExpressionType.LONG);
    Assertions.assertNull(cast.value());

    cast = eval.castTo(ExpressionType.STRING_ARRAY);
    Assertions.assertNull(cast.value());

    cast = eval.castTo(ExpressionType.LONG_ARRAY);
    Assertions.assertNull(cast.value());

    cast = eval.castTo(ExpressionType.DOUBLE_ARRAY);
    Assertions.assertNull(cast.value());

    cast = eval.castTo(ExpressionType.NESTED_DATA);
    Assertions.assertNull(cast.value());
  }

  @Test
  public void testCastLong()
  {
    ExprEval<?> eval = ExprEval.of(1234L);

    ExprEval<?> cast = eval.castTo(ExpressionType.STRING);
    Assertions.assertEquals("1234", cast.value());

    cast = eval.castTo(ExpressionType.DOUBLE);
    Assertions.assertEquals(1234.0, cast.value());

    cast = eval.castTo(ExpressionType.STRING_ARRAY);
    Assertions.assertArrayEquals(new Object[]{"1234"}, (Object[]) cast.value());

    cast = eval.castTo(ExpressionType.LONG_ARRAY);
    Assertions.assertArrayEquals(new Object[]{1234L}, (Object[]) cast.value());

    cast = eval.castTo(ExpressionType.DOUBLE_ARRAY);
    Assertions.assertArrayEquals(new Object[]{1234.0}, (Object[]) cast.value());

    cast = eval.castTo(ExpressionType.NESTED_DATA);
    Assertions.assertEquals(1234L, cast.value());

    eval = ExprEval.ofType(ExpressionType.LONG, null);

    cast = eval.castTo(ExpressionType.STRING);
    Assertions.assertNull(cast.value());

    cast = eval.castTo(ExpressionType.LONG);
    Assertions.assertNull(cast.value());

    cast = eval.castTo(ExpressionType.STRING_ARRAY);
    Assertions.assertNull(cast.value());

    cast = eval.castTo(ExpressionType.LONG_ARRAY);
    Assertions.assertNull(cast.value());

    cast = eval.castTo(ExpressionType.DOUBLE_ARRAY);
    Assertions.assertNull(cast.value());

    cast = eval.castTo(ExpressionType.NESTED_DATA);
    Assertions.assertNull(cast.value());
  }

  @Test
  public void testCastArray()
  {
    ExprEval<?> eval = ExprEval.ofStringArray(new String[]{"1", "2", "foo", null, "3.3"});

    ExprEval<?> cast = eval.castTo(ExpressionType.LONG_ARRAY);
    Assertions.assertArrayEquals(new Object[]{1L, 2L, null, null, 3L}, (Object[]) cast.value());

    cast = eval.castTo(ExpressionType.DOUBLE_ARRAY);
    Assertions.assertArrayEquals(new Object[]{1.0, 2.0, null, null, 3.3}, (Object[]) cast.value());

    cast = eval.castTo(ExpressionType.NESTED_DATA);
    Assertions.assertArrayEquals(new Object[]{"1", "2", "foo", null, "3.3"}, (Object[]) cast.value());

    cast = eval.castTo(ExpressionTypeFactory.getInstance().ofArray(ExpressionType.NESTED_DATA));
    Assertions.assertArrayEquals(new Object[]{"1", "2", "foo", null, "3.3"}, (Object[]) cast.value());

    cast = eval.castTo(ExpressionType.LONG);
    Assertions.assertNull(cast.value());

    cast = eval.castTo(ExpressionType.DOUBLE);
    Assertions.assertNull(cast.value());

    cast = eval.castTo(ExpressionType.STRING);
    Assertions.assertNull(cast.value());

    eval = ExprEval.ofType(ExpressionType.LONG_ARRAY, new Object[]{1234L});

    cast = eval.castTo(ExpressionType.DOUBLE_ARRAY);
    Assertions.assertArrayEquals(new Object[]{1234.0}, (Object[]) cast.value());

    cast = eval.castTo(ExpressionType.STRING_ARRAY);
    Assertions.assertArrayEquals(new Object[]{"1234"}, (Object[]) cast.value());

    cast = eval.castTo(ExpressionType.STRING);
    Assertions.assertEquals("1234", cast.value());

    cast = eval.castTo(ExpressionType.DOUBLE);
    Assertions.assertEquals(1234.0, cast.value());

    cast = eval.castTo(ExpressionType.LONG);
    Assertions.assertEquals(1234L, cast.value());
  }

  @Test
  public void testCastNestedData()
  {
    ExprEval<?> eval = ExprEval.ofType(ExpressionType.NESTED_DATA, ImmutableMap.of("x", 1234L, "y", 12.34));
    Assertions.assertEquals(
        ImmutableMap.of("x", 1234L, "y", 12.34),
        eval.castTo(ExpressionType.NESTED_DATA).value()
    );
    Throwable t = Assertions.assertThrows(IAE.class, () -> eval.castTo(ExpressionType.LONG));
    Assertions.assertEquals("Invalid type, cannot cast [COMPLEX<json>] to [LONG]", t.getMessage());
  }

  @Test
  public void testEmptyArrayFromList()
  {
    // empty arrays will materialize from JSON into an empty list, which coerce list to array will make into Object[]
    // make sure we can handle it
    ExprEval someEmptyArray = ExprEval.bestEffortOf(new ArrayList<>());
    Assertions.assertTrue(someEmptyArray.isArray());
    Assertions.assertEquals(0, someEmptyArray.asArray().length);
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
      Assertions.assertArrayEquals(
          expected.asArray(),
          ExprEval.deserialize(buffer, position, MAX_SIZE_BYTES, expected.type(), true).asArray(),
          "deserialized value with buffer references allowed"
      );

      Assertions.assertArrayEquals(
          expected.asArray(),
          ExprEval.deserialize(buffer, position, MAX_SIZE_BYTES, expected.type(), false).asArray(),
          "deserialized value with buffer references not allowed"
      );
    } else {
      Assertions.assertEquals(
          expected.value(),
          ExprEval.deserialize(buffer, position, MAX_SIZE_BYTES, expected.type(), true).value(),
          "deserialized value with buffer references allowed"
      );

      Assertions.assertEquals(
          expected.value(),
          ExprEval.deserialize(buffer, position, MAX_SIZE_BYTES, expected.type(), false).value(),
          "deserialized value with buffer references not allowed"
      );
    }
  }
}
