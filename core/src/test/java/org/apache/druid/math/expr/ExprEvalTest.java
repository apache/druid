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
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class ExprEvalTest extends InitializedNullHandlingTest
{
  private static int MAX_SIZE_BYTES = 1 << 13;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  ByteBuffer buffer = ByteBuffer.allocate(1 << 16);

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
        "Unable to serialize [%s], size [%s] is larger than max [%s]",
        ExprType.STRING,
        16,
        10
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
        "Unable to serialize [%s], size [%s] is larger than max [%s]",
        ExprType.STRING_ARRAY,
        14,
        10
    ));
    assertExpr(0, ExprEval.ofStringArray(new String[]{"hello", "hi", "hey"}), 10);
  }

  @Test
  public void testStringArrayEvalToBig()
  {
    expectedException.expect(ISE.class);
    // this has a different failure size than string serde because it doesn't check incrementally
    expectedException.expectMessage(StringUtils.format(
        "Unable to serialize [%s], size [%s] is larger than max [%s]",
        ExprType.STRING_ARRAY,
        27,
        10
    ));
    assertEstimatedBytes(ExprEval.ofStringArray(new String[]{"hello", "hi", "hey"}), 10);
  }

  @Test
  public void testLongArraySerde()
  {
    assertExpr(0, new Long[]{1L, 2L, 3L});
    assertExpr(1234, new Long[]{1L, 2L, null, 3L});
    assertExpr(1234, new Long[]{});
  }

  @Test
  public void testLongArraySerdeTooBig()
  {
    expectedException.expect(ISE.class);
    expectedException.expectMessage(StringUtils.format(
        "Unable to serialize [%s], size [%s] is larger than max [%s]",
        ExprType.LONG_ARRAY,
        29,
        10
    ));
    assertExpr(0, ExprEval.ofLongArray(new Long[]{1L, 2L, 3L}), 10);
  }

  @Test
  public void testLongArrayEvalTooBig()
  {
    expectedException.expect(ISE.class);
    expectedException.expectMessage(StringUtils.format(
        "Unable to serialize [%s], size [%s] is larger than max [%s]",
        ExprType.LONG_ARRAY,
        NullHandling.sqlCompatible() ? 32 : 29,
        10
    ));
    assertEstimatedBytes(ExprEval.ofLongArray(new Long[]{1L, 2L, 3L}), 10);
  }

  @Test
  public void testDoubleArraySerde()
  {
    assertExpr(0, new Double[]{1.1, 2.2, 3.3});
    assertExpr(1234, new Double[]{1.1, 2.2, null, 3.3});
    assertExpr(1234, new Double[]{});
  }

  @Test
  public void testDoubleArraySerdeTooBig()
  {
    expectedException.expect(ISE.class);
    expectedException.expectMessage(StringUtils.format(
        "Unable to serialize [%s], size [%s] is larger than max [%s]",
        ExprType.DOUBLE_ARRAY,
        29,
        10
    ));
    assertExpr(0, ExprEval.ofDoubleArray(new Double[]{1.1, 2.2, 3.3}), 10);
  }

  @Test
  public void testDoubleArrayEvalTooBig()
  {
    expectedException.expect(ISE.class);
    expectedException.expectMessage(StringUtils.format(
        "Unable to serialize [%s], size [%s] is larger than max [%s]",
        ExprType.DOUBLE_ARRAY,
        NullHandling.sqlCompatible() ? 32 : 29,
        10
    ));
    assertEstimatedBytes(ExprEval.ofDoubleArray(new Double[]{1.1, 2.2, 3.3}), 10);
  }

  @Test
  public void test_coerceListToArray()
  {
    Assert.assertNull(ExprEval.coerceListToArray(null, false));
    Assert.assertArrayEquals(new Object[0], (Object[]) ExprEval.coerceListToArray(ImmutableList.of(), false));
    Assert.assertArrayEquals(new String[]{null}, (String[]) ExprEval.coerceListToArray(null, true));
    Assert.assertArrayEquals(new String[]{null}, (String[]) ExprEval.coerceListToArray(ImmutableList.of(), true));

    List<Long> longList = ImmutableList.of(1L, 2L, 3L);
    Assert.assertArrayEquals(new Long[]{1L, 2L, 3L}, (Long[]) ExprEval.coerceListToArray(longList, false));

    List<Integer> intList = ImmutableList.of(1, 2, 3);
    Assert.assertArrayEquals(new Long[]{1L, 2L, 3L}, (Long[]) ExprEval.coerceListToArray(intList, false));

    List<Float> floatList = ImmutableList.of(1.0f, 2.0f, 3.0f);
    Assert.assertArrayEquals(new Double[]{1.0, 2.0, 3.0}, (Double[]) ExprEval.coerceListToArray(floatList, false));

    List<Double> doubleList = ImmutableList.of(1.0, 2.0, 3.0);
    Assert.assertArrayEquals(new Double[]{1.0, 2.0, 3.0}, (Double[]) ExprEval.coerceListToArray(doubleList, false));

    List<String> stringList = ImmutableList.of("a", "b", "c");
    Assert.assertArrayEquals(new String[]{"a", "b", "c"}, (String[]) ExprEval.coerceListToArray(stringList, false));

    List<String> withNulls = new ArrayList<>();
    withNulls.add("a");
    withNulls.add(null);
    withNulls.add("c");
    Assert.assertArrayEquals(new String[]{"a", null, "c"}, (String[]) ExprEval.coerceListToArray(withNulls, false));

    List<Long> withNumberNulls = new ArrayList<>();
    withNumberNulls.add(1L);
    withNumberNulls.add(null);
    withNumberNulls.add(3L);

    Assert.assertArrayEquals(new Long[]{1L, null, 3L}, (Long[]) ExprEval.coerceListToArray(withNumberNulls, false));

    List<Object> withStringMix = ImmutableList.of(1L, "b", 3L);
    Assert.assertArrayEquals(
        new String[]{"1", "b", "3"},
        (String[]) ExprEval.coerceListToArray(withStringMix, false)
    );

    List<Number> withIntsAndLongs = ImmutableList.of(1, 2L, 3);
    Assert.assertArrayEquals(
        new Long[]{1L, 2L, 3L},
        (Long[]) ExprEval.coerceListToArray(withIntsAndLongs, false)
    );

    List<Number> withFloatsAndLongs = ImmutableList.of(1, 2L, 3.0f);
    Assert.assertArrayEquals(
        new Double[]{1.0, 2.0, 3.0},
        (Double[]) ExprEval.coerceListToArray(withFloatsAndLongs, false)
    );

    List<Number> withDoublesAndLongs = ImmutableList.of(1, 2L, 3.0);
    Assert.assertArrayEquals(
        new Double[]{1.0, 2.0, 3.0},
        (Double[]) ExprEval.coerceListToArray(withDoublesAndLongs, false)
    );

    List<Number> withFloatsAndDoubles = ImmutableList.of(1L, 2.0f, 3.0);
    Assert.assertArrayEquals(
        new Double[]{1.0, 2.0, 3.0},
        (Double[]) ExprEval.coerceListToArray(withFloatsAndDoubles, false)
    );

    List<String> withAllNulls = new ArrayList<>();
    withAllNulls.add(null);
    withAllNulls.add(null);
    withAllNulls.add(null);
    Assert.assertArrayEquals(
        new String[]{null, null, null},
        (String[]) ExprEval.coerceListToArray(withAllNulls, false)
    );
  }

  @Test
  public void testStringArrayToNumberArray()
  {
    ExprEval someStringArray = ExprEval.ofStringArray(new String[]{"1", "2", "foo", null, "3.3"});
    Assert.assertArrayEquals(
        new Long[]{1L, 2L, null, null, 3L},
        someStringArray.asLongArray()
    );
    Assert.assertArrayEquals(
        new Double[]{1.0, 2.0, null, null, 3.3},
        someStringArray.asDoubleArray()
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
    ExprEval.serialize(buffer, position, expected, maxSizeBytes);
    if (ExprType.isArray(expected.type())) {
      Assert.assertArrayEquals(expected.asArray(), ExprEval.deserialize(buffer, position).asArray());
    } else {
      Assert.assertEquals(expected.value(), ExprEval.deserialize(buffer, position).value());
    }
    assertEstimatedBytes(expected, maxSizeBytes);
  }

  private void assertEstimatedBytes(ExprEval eval, int maxSizeBytes)
  {
    ExprEval.estimateAndCheckMaxBytes(eval, maxSizeBytes);
  }
}
