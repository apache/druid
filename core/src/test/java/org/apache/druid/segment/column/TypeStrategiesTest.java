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
import org.apache.druid.java.util.common.IAE;
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
      public int write(ByteBuffer buffer, String value, int maxSizeBytes)
      {
        return 1;
      }

      @Override
      public int compare(String o1, String o2)
      {
        return 0;
      }
    });
  }

  @Test
  public void testStringComparator()
  {
    TypeStrategy<String> strategy = ColumnType.STRING.getStrategy();
    Assert.assertEquals(-1, strategy.compare("a", "b"));
    //noinspection EqualsWithItself
    Assert.assertEquals(0, strategy.compare("a", "a"));
    Assert.assertEquals(1, strategy.compare("b", "a"));
    Assert.assertEquals(-48, strategy.compare("1", "a"));
    Assert.assertEquals(48, strategy.compare("a", "1"));

    NullableTypeStrategy<String> nullableTypeStrategy = ColumnType.STRING.getNullableStrategy();
    Assert.assertEquals(-1, nullableTypeStrategy.compare("a", "b"));
    Assert.assertEquals(-1, nullableTypeStrategy.compare(null, "b"));
    //noinspection EqualsWithItself
    Assert.assertEquals(0, nullableTypeStrategy.compare("a", "a"));
    Assert.assertEquals(1, nullableTypeStrategy.compare("b", "a"));
    Assert.assertEquals(1, nullableTypeStrategy.compare("b", null));
    Assert.assertEquals(-48, nullableTypeStrategy.compare("1", "a"));
    Assert.assertEquals(48, nullableTypeStrategy.compare("a", "1"));
  }

  @Test
  public void testDoubleComparator()
  {
    TypeStrategy<Double> strategy = ColumnType.DOUBLE.getStrategy();
    Assert.assertEquals(-1, strategy.compare(0.01, 1.01));
    //noinspection EqualsWithItself
    Assert.assertEquals(0, strategy.compare(0.00001, 0.00001));
    Assert.assertEquals(1, strategy.compare(1.01, 0.01));

    NullableTypeStrategy nullableTypeStrategy = ColumnType.DOUBLE.getNullableStrategy();
    Assert.assertEquals(-1, nullableTypeStrategy.compare(0.01, 1.01));
    Assert.assertEquals(-1, nullableTypeStrategy.compare(null, 1.01));
    //noinspection EqualsWithItself
    Assert.assertEquals(0, nullableTypeStrategy.compare(0.00001, 0.00001));
    Assert.assertEquals(1, nullableTypeStrategy.compare(1.01, 0.01));
    Assert.assertEquals(1, nullableTypeStrategy.compare(1.01, null));
  }

  @Test
  public void testFloatComparator()
  {
    TypeStrategy<Float> strategy = ColumnType.FLOAT.getStrategy();
    Assert.assertEquals(-1, strategy.compare(0.01f, 1.01f));
    //noinspection EqualsWithItself
    Assert.assertEquals(0, strategy.compare(0.00001f, 0.00001f));
    Assert.assertEquals(1, strategy.compare(1.01f, 0.01f));

    NullableTypeStrategy<Float> nullableTypeStrategy = ColumnType.FLOAT.getNullableStrategy();
    Assert.assertEquals(-1, nullableTypeStrategy.compare(0.01f, 1.01f));
    //noinspection EqualsWithItself
    Assert.assertEquals(0, nullableTypeStrategy.compare(0.00001f, 0.00001f));
    Assert.assertEquals(1, nullableTypeStrategy.compare(1.01f, 0.01f));
  }

  @Test
  public void testLongComparator()
  {
    TypeStrategy<Long> strategy = ColumnType.LONG.getStrategy();
    Assert.assertEquals(-1, strategy.compare(-1L, 1L));
    //noinspection EqualsWithItself
    Assert.assertEquals(0, strategy.compare(1L, 1L));
    Assert.assertEquals(1, strategy.compare(1L, -1L));

    NullableTypeStrategy<Long> nullableTypeStrategy = ColumnType.LONG.getNullableStrategy();
    Assert.assertEquals(-1, nullableTypeStrategy.compare(-1L, 1L));
    Assert.assertEquals(-1, nullableTypeStrategy.compare(null, 1L));
    //noinspection EqualsWithItself
    Assert.assertEquals(0, nullableTypeStrategy.compare(1L, 1L));
    Assert.assertEquals(1, nullableTypeStrategy.compare(1L, -1L));
    Assert.assertEquals(1, nullableTypeStrategy.compare(1L, null));
  }

  @Test
  public void testArrayComparator()
  {
    TypeStrategy<Object[]> strategy = ColumnType.LONG_ARRAY.getStrategy();
    Assert.assertEquals(-1, strategy.compare(new Long[]{1L, 1L, 2L}, new Long[]{1L, 2L, 3L}));
    Assert.assertEquals(-1, strategy.compare(new Long[]{1L, 2L}, new Long[]{1L, 2L, 3L}));
    Assert.assertEquals(-1, strategy.compare(new Long[]{}, new Long[]{1L}));
    Assert.assertEquals(-1, strategy.compare(null, new Long[]{}));
    //noinspection EqualsWithItself
    Assert.assertEquals(0, strategy.compare(new Long[]{1L, 2L, 3L}, new Long[]{1L, 2L, 3L}));

    Assert.assertEquals(1, strategy.compare(new Long[]{1L, 1L, 2L}, new Long[]{-1L, 2L, 3L}));
    Assert.assertEquals(1, strategy.compare(new Long[]{1L, 2L, 2L}, new Long[]{1L, 2L, -3L}));
    Assert.assertEquals(1, strategy.compare(new Long[]{1L, 2L}, new Long[]{-1L, 2L, 3L}));
    Assert.assertEquals(1, strategy.compare(new Long[]{1L, 2L}, null));

    NullableTypeStrategy<Object[]> nullableTypeStrategy = ColumnType.LONG_ARRAY.getNullableStrategy();
    Assert.assertEquals(-1, nullableTypeStrategy.compare(new Long[]{1L, 1L, 2L}, new Long[]{1L, 2L, 3L}));
    Assert.assertEquals(-1, nullableTypeStrategy.compare(new Long[]{1L, 2L}, new Long[]{1L, 2L, 3L}));
    Assert.assertEquals(-1, nullableTypeStrategy.compare(new Long[]{}, new Long[]{1L}));
    Assert.assertEquals(-1, nullableTypeStrategy.compare(null, new Long[]{}));
    //noinspection EqualsWithItself
    Assert.assertEquals(0, nullableTypeStrategy.compare(new Long[]{1L, 2L, 3L}, new Long[]{1L, 2L, 3L}));

    Assert.assertEquals(1, nullableTypeStrategy.compare(new Long[]{1L, 1L, 2L}, new Long[]{-1L, 2L, 3L}));
    Assert.assertEquals(1, nullableTypeStrategy.compare(new Long[]{1L, 2L, 2L}, new Long[]{1L, 2L, -3L}));
    Assert.assertEquals(1, nullableTypeStrategy.compare(new Long[]{1L, 2L}, new Long[]{-1L, 2L, 3L}));
    Assert.assertEquals(1, nullableTypeStrategy.compare(new Long[]{1L, 2L}, null));

    strategy = ColumnType.ofArray(ColumnType.ofArray(ColumnType.DOUBLE)).getStrategy();
    Assert.assertEquals(
        -1,
        strategy.compare(
            new Object[]{new Object[]{1.0, 2.0}},
            new Object[]{new Object[]{1.0, 2.0}, new Object[]{1.1, -12.345}}
        )
    );
    Assert.assertEquals(
        -1,
        strategy.compare(
            new Object[]{new Object[]{1.0, 2.0}, new Object[]{1.1, -23.456}},
            new Object[]{new Object[]{1.0, 2.0}, new Object[]{1.1, -12.345}}
        )
    );
    Assert.assertEquals(
        -1,
        strategy.compare(
            null,
            new Object[]{new Object[]{1.0, 2.0}, new Object[]{1.1, -12.345}}
        )
    );
    Assert.assertEquals(
        -1,
        strategy.compare(
            new Object[]{new Object[]{1.0, 2.0}, null},
            new Object[]{new Object[]{1.0, 2.0}, new Object[]{1.1, -12.345}}
        )
    );

    //noinspection EqualsWithItself
    Assert.assertEquals(
        0,
        strategy.compare(
            new Object[]{new Object[]{1.0, 2.0}, null},
            new Object[]{new Object[]{1.0, 2.0}, null}
        )
    );

    Assert.assertEquals(
        1,
        strategy.compare(
            new Object[]{new Object[]{1.0, 2.1}},
            new Object[]{new Object[]{1.0, 2.0}, new Object[]{1.1, -12.345}}
        )
    );
    Assert.assertEquals(
        1,
        strategy.compare(
            new Object[]{new Object[]{1.0, 2.0}, new Object[]{1.1, -23.456}},
            new Object[]{new Object[]{1.0, 2.0}, null}
        )
    );

    nullableTypeStrategy = ColumnType.ofArray(ColumnType.ofArray(ColumnType.DOUBLE)).getNullableStrategy();
    Assert.assertEquals(
        -1,
        nullableTypeStrategy.compare(
            new Object[]{new Object[]{1.0, 2.0}},
            new Object[]{new Object[]{1.0, 2.0}, new Object[]{1.1, -12.345}}
        )
    );
    Assert.assertEquals(
        -1,
        nullableTypeStrategy.compare(
            new Object[]{new Object[]{1.0, 2.0}, new Object[]{1.1, -23.456}},
            new Object[]{new Object[]{1.0, 2.0}, new Object[]{1.1, -12.345}}
        )
    );
    Assert.assertEquals(
        -1,
        nullableTypeStrategy.compare(
            null,
            new Object[]{new Object[]{1.0, 2.0}, new Object[]{1.1, -12.345}}
        )
    );
    Assert.assertEquals(
        -1,
        nullableTypeStrategy.compare(
            new Object[]{new Object[]{1.0, 2.0}, null},
            new Object[]{new Object[]{1.0, 2.0}, new Object[]{1.1, -12.345}}
        )
    );

    //noinspection EqualsWithItself
    Assert.assertEquals(
        0,
        nullableTypeStrategy.compare(
            new Object[]{new Object[]{1.0, 2.0}, null},
            new Object[]{new Object[]{1.0, 2.0}, null}
        )
    );

    Assert.assertEquals(
        1,
        nullableTypeStrategy.compare(
            new Object[]{new Object[]{1.0, 2.1}},
            new Object[]{new Object[]{1.0, 2.0}, new Object[]{1.1, -12.345}}
        )
    );
    Assert.assertEquals(
        1,
        nullableTypeStrategy.compare(
            new Object[]{new Object[]{1.0, 2.0}, new Object[]{1.1, -23.456}},
            new Object[]{new Object[]{1.0, 2.0}, null}
        )
    );
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
    int bytesWritten = TypeStrategies.writeNotNullNullableLong(buffer, offset, someLong);
    Assert.assertEquals(1 + Long.BYTES, bytesWritten);
    Assert.assertFalse(TypeStrategies.isNullableNull(buffer, offset));
    Assert.assertEquals(someLong, TypeStrategies.readNotNullNullableLong(buffer, offset));

    // test non-zero offset
    offset = 1024;
    bytesWritten = TypeStrategies.writeNotNullNullableLong(buffer, offset, someLong);
    Assert.assertEquals(1 + Long.BYTES, bytesWritten);
    Assert.assertFalse(TypeStrategies.isNullableNull(buffer, offset));
    Assert.assertEquals(someLong, TypeStrategies.readNotNullNullableLong(buffer, offset));
  }

  @Test
  public void testNonNullNullableDoubleBinary()
  {
    final double someDouble = 1.234567;
    int offset = 0;
    int bytesWritten = TypeStrategies.writeNotNullNullableDouble(buffer, offset, someDouble);
    Assert.assertEquals(1 + Double.BYTES, bytesWritten);
    Assert.assertFalse(TypeStrategies.isNullableNull(buffer, offset));
    Assert.assertEquals(someDouble, TypeStrategies.readNotNullNullableDouble(buffer, offset), 0);

    // test non-zero offset
    offset = 1024;
    bytesWritten = TypeStrategies.writeNotNullNullableDouble(buffer, offset, someDouble);
    Assert.assertEquals(1 + Double.BYTES, bytesWritten);
    Assert.assertFalse(TypeStrategies.isNullableNull(buffer, offset));
    Assert.assertEquals(someDouble, TypeStrategies.readNotNullNullableDouble(buffer, offset), 0);
  }

  @Test
  public void testNonNullNullableFloatBinary()
  {
    final float someFloat = 1.234567f;
    int offset = 0;
    int bytesWritten = TypeStrategies.writeNotNullNullableFloat(buffer, offset, someFloat);
    Assert.assertEquals(1 + Float.BYTES, bytesWritten);
    Assert.assertFalse(TypeStrategies.isNullableNull(buffer, offset));
    Assert.assertEquals(someFloat, TypeStrategies.readNotNullNullableFloat(buffer, offset), 0);

    // test non-zero offset
    offset = 1024;
    bytesWritten = TypeStrategies.writeNotNullNullableFloat(buffer, offset, someFloat);
    Assert.assertEquals(1 + Float.BYTES, bytesWritten);
    Assert.assertFalse(TypeStrategies.isNullableNull(buffer, offset));
    Assert.assertEquals(someFloat, TypeStrategies.readNotNullNullableFloat(buffer, offset), 0);
  }

  @Test
  public void testCheckMaxSize()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage(
        "Unable to write [STRING], maxSizeBytes [2048] is greater than available [1024]"
    );
    ByteBuffer buffer = ByteBuffer.allocate(1 << 10);
    TypeStrategies.checkMaxSize(buffer.remaining(), 2048, ColumnType.STRING);
  }

  @Test
  public void testCheckMaxSizePosition()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage(
        "Unable to write [STRING], maxSizeBytes [1024] is greater than available [24]"
    );
    final int maxSize = 1 << 10;
    ByteBuffer buffer = ByteBuffer.allocate(maxSize);
    buffer.position(1000);
    TypeStrategies.checkMaxSize(buffer.remaining(), maxSize, ColumnType.STRING);
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
    final int maxSize = 2048;
    final int expectedLength = strategy.estimateSizeBytes(value);
    Assert.assertNotEquals(0, expectedLength);

    // test buffer
    int offset = 10;
    buffer.position(offset);
    Assert.assertEquals(expectedLength, strategy.write(buffer, value, maxSize));
    Assert.assertEquals(expectedLength, buffer.position() - offset);
    buffer.position(offset);
    Assert.assertEquals(value, strategy.read(buffer));
    Assert.assertEquals(expectedLength, buffer.position() - offset);

    // test buffer nullable write read value
    NullableTypeStrategy nullableTypeStrategy = new NullableTypeStrategy(strategy);
    buffer.position(offset);
    Assert.assertEquals(1 + expectedLength, nullableTypeStrategy.write(buffer, value, maxSize));
    Assert.assertEquals(1 + expectedLength, buffer.position() - offset);
    buffer.position(offset);
    Assert.assertEquals(value, nullableTypeStrategy.read(buffer));
    Assert.assertEquals(1 + expectedLength, buffer.position() - offset);

    // test buffer nullable write read null
    buffer.position(offset);
    Assert.assertEquals(1, nullableTypeStrategy.write(buffer, null, maxSize));
    Assert.assertEquals(1, buffer.position() - offset);
    buffer.position(offset);
    Assert.assertNull(nullableTypeStrategy.read(buffer));
    Assert.assertEquals(1, buffer.position() - offset);

    buffer.position(0);

    // test buffer offset
    Assert.assertEquals(expectedLength, strategy.write(buffer, 1024, value, maxSize));
    Assert.assertEquals(value, strategy.read(buffer, 1024));
    Assert.assertEquals(0, buffer.position());

    // test buffer offset nullable write read value
    Assert.assertEquals(1 + expectedLength, nullableTypeStrategy.write(buffer, 1024, value, maxSize));
    Assert.assertEquals(value, nullableTypeStrategy.read(buffer, 1024));
    Assert.assertEquals(0, buffer.position());

    // test buffer offset nullable write read null
    Assert.assertEquals(1, nullableTypeStrategy.write(buffer, 1024, null, maxSize));
    Assert.assertNull(nullableTypeStrategy.read(buffer, 1024));
    Assert.assertEquals(0, buffer.position());
  }

  private void assertArrayStrategy(TypeStrategy strategy, @Nullable Object[] value)
  {
    final int maxSize = 2048;
    final int expectedLength = strategy.estimateSizeBytes(value);
    Assert.assertNotEquals(0, expectedLength);

    // test buffer
    int offset = 10;
    buffer.position(offset);
    Assert.assertEquals(expectedLength, strategy.write(buffer, value, maxSize));
    Assert.assertEquals(expectedLength, buffer.position() - offset);
    buffer.position(offset);
    Assert.assertArrayEquals(value, (Object[]) strategy.read(buffer));
    Assert.assertEquals(expectedLength, buffer.position() - offset);

    // test buffer nullable write read value
    NullableTypeStrategy nullableTypeStrategy = new NullableTypeStrategy(strategy);
    buffer.position(offset);
    Assert.assertEquals(1 + expectedLength, nullableTypeStrategy.write(buffer, value, maxSize));
    Assert.assertEquals(1 + expectedLength, buffer.position() - offset);
    buffer.position(offset);
    Assert.assertArrayEquals(value, (Object[]) nullableTypeStrategy.read(buffer));
    Assert.assertEquals(1 + expectedLength, buffer.position() - offset);

    // test buffer nullable write read null
    buffer.position(offset);
    Assert.assertEquals(1, nullableTypeStrategy.write(buffer, null, maxSize));
    Assert.assertEquals(1, buffer.position() - offset);
    buffer.position(offset);
    Assert.assertNull(nullableTypeStrategy.read(buffer));
    Assert.assertEquals(1, buffer.position() - offset);

    buffer.position(0);

    // test buffer offset
    Assert.assertEquals(expectedLength, strategy.write(buffer, 1024, value, maxSize));
    Assert.assertArrayEquals(value, (Object[]) strategy.read(buffer, 1024));
    Assert.assertEquals(0, buffer.position());

    // test buffer offset nullable write read value
    Assert.assertEquals(1 + expectedLength, nullableTypeStrategy.write(buffer, 1024, value, maxSize));
    Assert.assertArrayEquals(value, (Object[]) nullableTypeStrategy.read(buffer, 1024));
    Assert.assertEquals(0, buffer.position());

    // test buffer offset nullable write read null
    Assert.assertEquals(1, nullableTypeStrategy.write(buffer, 1024, null, maxSize));
    Assert.assertNull(nullableTypeStrategy.read(buffer, 1024));
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
      NullableTypeStrategy<Long> longStrategy = ExpressionType.LONG.getNullableStrategy();
      return longStrategy.estimateSizeBytes(value.lhs) + longStrategy.estimateSizeBytes(value.rhs);
    }

    @Override
    public NullableLongPair read(ByteBuffer buffer)
    {
      NullableTypeStrategy<Long> longTypeStrategy = ExpressionType.LONG.getNullableStrategy();
      Long lhs = longTypeStrategy.read(buffer);
      Long rhs = longTypeStrategy.read(buffer);
      return new NullableLongPair(lhs, rhs);
    }

    @Override
    public int write(ByteBuffer buffer, NullableLongPair value, int maxSizeBytes)
    {
      NullableTypeStrategy<Long> longTypeStrategy = ExpressionType.LONG.getNullableStrategy();
      int written = longTypeStrategy.write(buffer, value.lhs, maxSizeBytes);
      if (written > 0) {
        int next = longTypeStrategy.write(buffer, value.rhs, maxSizeBytes - written);
        written = next > 0 ? written + next : next;
      }
      return written;
    }
  }
}
