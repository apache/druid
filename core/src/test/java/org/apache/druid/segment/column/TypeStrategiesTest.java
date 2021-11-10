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

package org.apache.druid.segment.column;

import com.google.common.primitives.Longs;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.math.expr.ExpressionType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class TypeStrategiesTest
{
  ByteBuffer buffer = ByteBuffer.allocate(1 << 16);

  public static ColumnType NULLABLE_TEST_PAIR_TYPE = ColumnType.ofComplex("nullableLongPair");

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @BeforeClass
  public static void setup()
  {
    TypeStrategies.registerComplex(NULLABLE_TEST_PAIR_TYPE.getComplexTypeName(), new NullableLongPairTypeStrategy());
  }

  @Test
  public void testRegister()
  {
    TypeStrategy<?> strategy = NULLABLE_TEST_PAIR_TYPE.getStrategy();
    Assert.assertNotNull(strategy);
    Assert.assertTrue(strategy instanceof NullableLongPairTypeStrategy);
  }

  @Test
  public void testRegisterDuplicate()
  {
    TypeStrategies.registerComplex(NULLABLE_TEST_PAIR_TYPE.getComplexTypeName(), new NullableLongPairTypeStrategy());
    TypeStrategy<?> strategy = TypeStrategies.getComplex(NULLABLE_TEST_PAIR_TYPE.getComplexTypeName());
    Assert.assertNotNull(strategy);
    Assert.assertTrue(strategy instanceof NullableLongPairTypeStrategy);
  }

  @Test
  public void testConflicting()
  {
    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage(
        "Incompatible strategy for type[nullableLongPair] already exists. "
        + "Expected [org.apache.druid.segment.column.TypeStrategiesTest$1], "
        + "found [org.apache.druid.segment.column.TypeStrategiesTest$NullableLongPairTypeStrategy]."
    );

    TypeStrategies.registerComplex(NULLABLE_TEST_PAIR_TYPE.getComplexTypeName(), new TypeStrategy<String>()
    {
      @Override
      public int estimateSizeBytes(@Nullable String value)
      {
        return 0;
      }

      @Override
      public String read(ByteBuffer buffer)
      {
        return null;
      }

      @Override
      public void write(ByteBuffer buffer, String value)
      {

      }

      @Override
      public int compare(String o1, String o2)
      {
        return 0;
      }
    });
  }

  @Test
  public void testNulls()
  {
    int offset = 0;
    TypeStrategies.writeNull(buffer, offset);
    Assert.assertTrue(TypeStrategies.isNullableNull(buffer, offset));

    // test non-zero offset
    offset = 128;
    TypeStrategies.writeNull(buffer, offset);
    Assert.assertTrue(TypeStrategies.isNullableNull(buffer, offset));
  }

  @Test
  public void testNonNullNullableLongBinary()
  {
    final long someLong = 12345567L;
    int offset = 0;
    int bytesWritten = TypeStrategies.writeNullableLong(buffer, offset, someLong);
    Assert.assertEquals(1 + Long.BYTES, bytesWritten);
    Assert.assertFalse(TypeStrategies.isNullableNull(buffer, offset));
    Assert.assertEquals(someLong, TypeStrategies.readNullableLong(buffer, offset));

    // test non-zero offset
    offset = 1024;
    bytesWritten = TypeStrategies.writeNullableLong(buffer, offset, someLong);
    Assert.assertEquals(1 + Long.BYTES, bytesWritten);
    Assert.assertFalse(TypeStrategies.isNullableNull(buffer, offset));
    Assert.assertEquals(someLong, TypeStrategies.readNullableLong(buffer, offset));
  }

  @Test
  public void testNonNullNullableDoubleBinary()
  {
    final double someDouble = 1.234567;
    int offset = 0;
    int bytesWritten = TypeStrategies.writeNullableDouble(buffer, offset, someDouble);
    Assert.assertEquals(1 + Double.BYTES, bytesWritten);
    Assert.assertFalse(TypeStrategies.isNullableNull(buffer, offset));
    Assert.assertEquals(someDouble, TypeStrategies.readNullableDouble(buffer, offset), 0);

    // test non-zero offset
    offset = 1024;
    bytesWritten = TypeStrategies.writeNullableDouble(buffer, offset, someDouble);
    Assert.assertEquals(1 + Double.BYTES, bytesWritten);
    Assert.assertFalse(TypeStrategies.isNullableNull(buffer, offset));
    Assert.assertEquals(someDouble, TypeStrategies.readNullableDouble(buffer, offset), 0);
  }

  @Test
  public void testNonNullNullableFloatBinary()
  {
    final float someFloat = 1.234567f;
    int offset = 0;
    int bytesWritten = TypeStrategies.writeNullableFloat(buffer, offset, someFloat);
    Assert.assertEquals(1 + Float.BYTES, bytesWritten);
    Assert.assertFalse(TypeStrategies.isNullableNull(buffer, offset));
    Assert.assertEquals(someFloat, TypeStrategies.readNullableFloat(buffer, offset), 0);

    // test non-zero offset
    offset = 1024;
    bytesWritten = TypeStrategies.writeNullableFloat(buffer, offset, someFloat);
    Assert.assertEquals(1 + Float.BYTES, bytesWritten);
    Assert.assertFalse(TypeStrategies.isNullableNull(buffer, offset));
    Assert.assertEquals(someFloat, TypeStrategies.readNullableFloat(buffer, offset), 0);
  }

  @Test
  public void testLongTypeStrategy()
  {
    assertStrategy(TypeStrategies.LONG, 12345567L);
  }

  @Test
  public void testFloatTypeStrategy()
  {
    assertStrategy(TypeStrategies.FLOAT, 1.234567f);
  }

  @Test
  public void testDoubleTypeStrategy()
  {
    assertStrategy(TypeStrategies.DOUBLE, 1.234567);
  }

  @Test
  public void testStringTypeStrategy()
  {
    assertStrategy(TypeStrategies.STRING, "hello hi hey");
  }

  @Test
  public void testComplexTypeStrategy()
  {
    final TypeStrategy strategy = TypeStrategies.getComplex(NULLABLE_TEST_PAIR_TYPE.getComplexTypeName());
    assertStrategy(strategy, new NullableLongPair(null, 1L));
    assertStrategy(strategy, new NullableLongPair(1234L, 5678L));
    assertStrategy(strategy, new NullableLongPair(1234L, null));
  }

  @Test
  public void testArrayTypeStrategy()
  {
    TypeStrategy strategy;
    final Object[] empty = new Object[0];

    // double array
    strategy = new TypeStrategies.ArrayTypeStrategy(ColumnType.DOUBLE_ARRAY);
    final Object[] someDoubleArray = new Double[]{1.23, 4.567, null, 8.9};

    assertArrayStrategy(strategy, empty);
    assertArrayStrategy(strategy, someDoubleArray);

    // long array
    strategy = new TypeStrategies.ArrayTypeStrategy(ColumnType.LONG_ARRAY);
    final Long[] someLongArray = new Long[]{1L, 2L, 3L, null, 4L};

    assertArrayStrategy(strategy, empty);
    assertArrayStrategy(strategy, someLongArray);

    // float array
    strategy = new TypeStrategies.ArrayTypeStrategy(ColumnType.ofArray(ColumnType.FLOAT));
    final Object[] someFloatArray = new Float[]{1.0f, 2.0f, null, 3.45f};

    assertArrayStrategy(strategy, empty);
    assertArrayStrategy(strategy, someFloatArray);

    // string arrays
    strategy = new TypeStrategies.ArrayTypeStrategy(ColumnType.STRING_ARRAY);
    final String[] someStringArray = new String[]{"hello", "hi", null, "hey"};
    final Object[] someObjectStringArray = new String[]{"hello", "hi", null, "hey"};

    assertArrayStrategy(strategy, empty);
    assertArrayStrategy(strategy, someStringArray);
    assertArrayStrategy(strategy, someObjectStringArray);

    // complex array
    strategy = new TypeStrategies.ArrayTypeStrategy(ColumnType.ofArray(NULLABLE_TEST_PAIR_TYPE));
    NullableLongPair lp1 = new NullableLongPair(null, 1L);
    NullableLongPair lp2 = new NullableLongPair(1234L, 5678L);
    NullableLongPair lp3 = new NullableLongPair(1234L, null);
    final Object[] someComplexArray = new Object[]{lp1, lp2, lp3};

    assertArrayStrategy(strategy, empty);
    assertArrayStrategy(strategy, someComplexArray);

    // nested string array
    strategy = new TypeStrategies.ArrayTypeStrategy(ColumnType.ofArray(ColumnType.STRING_ARRAY));
    final Object[] nester = new Object[]{someStringArray, someObjectStringArray};

    assertArrayStrategy(strategy, empty);
    assertArrayStrategy(strategy, nester);
  }

  private <T> void assertStrategy(TypeStrategy strategy, @Nullable T value)
  {
    final int expectedLength = strategy.estimateSizeBytes(value);
    Assert.assertNotEquals(0, expectedLength);

    // test buffer
    int offset = 10;
    buffer.position(offset);
    strategy.write(buffer, value);
    Assert.assertEquals(expectedLength, buffer.position() - offset);
    buffer.position(offset);
    Assert.assertEquals(value, strategy.read(buffer));
    Assert.assertEquals(expectedLength, buffer.position() - offset);

    // test buffer nullable write read value
    buffer.position(offset);
    TypeStrategies.writeNullableType(buffer, strategy, value);
    Assert.assertEquals(1 + expectedLength, buffer.position() - offset);
    buffer.position(offset);
    Assert.assertEquals(value, TypeStrategies.readNullableType(buffer, strategy));
    Assert.assertEquals(1 + expectedLength, buffer.position() - offset);

    // test buffer nullable write read null
    buffer.position(offset);
    TypeStrategies.writeNullableType(buffer, strategy, null);
    Assert.assertEquals(1, buffer.position() - offset);
    buffer.position(offset);
    Assert.assertNull(TypeStrategies.readNullableType(buffer, strategy));
    Assert.assertEquals(1, buffer.position() - offset);

    buffer.position(0);

    // test buffer offset
    Assert.assertEquals(expectedLength, strategy.write(buffer, 1024, value));
    Assert.assertEquals(value, strategy.read(buffer, 1024));
    Assert.assertEquals(0, buffer.position());

    // test buffer offset nullable write read value
    Assert.assertEquals(1 + expectedLength, TypeStrategies.writeNullableType(buffer, 1024, strategy, value));
    Assert.assertEquals(value, TypeStrategies.readNullableType(buffer, 1024, strategy));
    Assert.assertEquals(0, buffer.position());

    // test buffer offset nullable write read null
    Assert.assertEquals(1, TypeStrategies.writeNullableType(buffer, 1024, strategy, null));
    Assert.assertNull(TypeStrategies.readNullableType(buffer, 1024, strategy));
    Assert.assertEquals(0, buffer.position());
  }

  private void assertArrayStrategy(TypeStrategy strategy, @Nullable Object[] value)
  {
    final int expectedLength = strategy.estimateSizeBytes(value);
    Assert.assertNotEquals(0, expectedLength);

    // test buffer
    int offset = 10;
    buffer.position(offset);
    strategy.write(buffer, value);
    Assert.assertEquals(expectedLength, buffer.position() - offset);
    buffer.position(offset);
    Assert.assertArrayEquals(value, (Object[]) strategy.read(buffer));
    Assert.assertEquals(expectedLength, buffer.position() - offset);

    // test buffer nullable write read value
    buffer.position(offset);
    TypeStrategies.writeNullableType(buffer, strategy, value);
    Assert.assertEquals(1 + expectedLength, buffer.position() - offset);
    buffer.position(offset);
    Assert.assertArrayEquals(value, (Object[]) TypeStrategies.readNullableType(buffer, strategy));
    Assert.assertEquals(1 + expectedLength, buffer.position() - offset);

    // test buffer nullable write read null
    buffer.position(offset);
    TypeStrategies.writeNullableType(buffer, strategy, null);
    Assert.assertEquals(1, buffer.position() - offset);
    buffer.position(offset);
    Assert.assertNull(TypeStrategies.readNullableType(buffer, strategy));
    Assert.assertEquals(1, buffer.position() - offset);

    buffer.position(0);

    // test buffer offset
    Assert.assertEquals(expectedLength, strategy.write(buffer, 1024, value));
    Assert.assertArrayEquals(value, (Object[]) strategy.read(buffer, 1024));
    Assert.assertEquals(0, buffer.position());

    // test buffer offset nullable write read value
    Assert.assertEquals(1 + expectedLength, TypeStrategies.writeNullableType(buffer, 1024, strategy, value));
    Assert.assertArrayEquals(value, (Object[]) TypeStrategies.readNullableType(buffer, 1024, strategy));
    Assert.assertEquals(0, buffer.position());

    // test buffer offset nullable write read null
    Assert.assertEquals(1, TypeStrategies.writeNullableType(buffer, 1024, strategy, null));
    Assert.assertNull(TypeStrategies.readNullableType(buffer, 1024, strategy));
    Assert.assertEquals(0, buffer.position());
  }

  public static class NullableLongPair extends Pair<Long, Long> implements Comparable<NullableLongPair>
  {
    public NullableLongPair(@Nullable Long lhs, @Nullable Long rhs)
    {
      super(lhs, rhs);
    }

    @Override
    public int compareTo(NullableLongPair o)
    {
      return Comparators.<Long>naturalNullsFirst().thenComparing(Longs::compare).compare(this.lhs, o.lhs);
    }
  }
  
  public static class NullableLongPairTypeStrategy implements TypeStrategy<NullableLongPair>
  {
    @Override
    public int compare(NullableLongPair o1, NullableLongPair o2)
    {
      return Comparators.<NullableLongPair>naturalNullsFirst().compare(o1, o2);
    }

    @Override
    public int estimateSizeBytes(@Nullable NullableLongPair value)
    {
      if (value == null) {
        return 0;
      }
      TypeStrategy<Long> longStrategy = ExpressionType.LONG.getStrategy();
      return longStrategy.estimateSizeBytesNullable(value.lhs) + longStrategy.estimateSizeBytesNullable(value.rhs);
    }

    @Override
    public NullableLongPair read(ByteBuffer buffer)
    {
      TypeStrategy<Long> longTypeStrategy = ExpressionType.LONG.getStrategy();
      Long lhs = TypeStrategies.readNullableType(buffer, longTypeStrategy);
      Long rhs = TypeStrategies.readNullableType(buffer, longTypeStrategy);
      return new NullableLongPair(lhs, rhs);
    }

    @Override
    public void write(ByteBuffer buffer, NullableLongPair value)
    {
      TypeStrategy<Long> longTypeStrategy = ExpressionType.LONG.getStrategy();
      TypeStrategies.writeNullableType(buffer, longTypeStrategy, value.lhs);
      TypeStrategies.writeNullableType(buffer, longTypeStrategy, value.rhs);
    }
  }
}
