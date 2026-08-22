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

import com.google.common.collect.Ordering;
import org.apache.druid.guice.BuiltInTypesModule;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.segment.nested.StructuredData;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNull;

public class TypeStrategiesTest
{
  ByteBuffer buffer = ByteBuffer.allocate(1 << 16);

  public static ColumnType NULLABLE_TEST_PAIR_TYPE = ColumnType.ofComplex("nullableLongPair");

  @BeforeAll
  public static void setup()
  {
    BuiltInTypesModule.registerHandlersAndSerde();
    TypeStrategies.registerComplex(NULLABLE_TEST_PAIR_TYPE.getComplexTypeName(), new NullableLongPairTypeStrategy());
  }

  @Test
  public void testRegister()
  {
    TypeStrategy<?> strategy = NULLABLE_TEST_PAIR_TYPE.getStrategy();
    Assertions.assertNotNull(strategy);
    Assertions.assertTrue(strategy instanceof NullableLongPairTypeStrategy);
  }

  @Test
  public void testRegisterDuplicate()
  {
    TypeStrategies.registerComplex(NULLABLE_TEST_PAIR_TYPE.getComplexTypeName(), new NullableLongPairTypeStrategy());
    TypeStrategy<?> strategy = TypeStrategies.getComplex(NULLABLE_TEST_PAIR_TYPE.getComplexTypeName());
    Assertions.assertNotNull(strategy);
    Assertions.assertTrue(strategy instanceof NullableLongPairTypeStrategy);
  }

  @Test
  public void testConflicting()
  {
    final IllegalStateException exception = Assertions.assertThrows(
        IllegalStateException.class,
        () -> TypeStrategies.registerComplex(NULLABLE_TEST_PAIR_TYPE.getComplexTypeName(), new TypeStrategy<String>()
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
        public boolean readRetainsBufferReference()
        {
          return false;
        }

        @Override
        public int write(ByteBuffer buffer, String value, int maxSizeBytes)
        {
          return 1;
        }

        @Override
        public int compare(Object o1, Object o2)
        {
          return 0;
        }

        @Override
        public boolean groupable()
        {
          return false;
        }
      })
    );
    Assertions.assertEquals(
        "Incompatible strategy for type[nullableLongPair] already exists. "
        + "Expected [org.apache.druid.segment.column.TypeStrategiesTest$1], "
        + "found [org.apache.druid.segment.column.TypeStrategiesTest$NullableLongPairTypeStrategy].",
        exception.getMessage()
    );
  }

  @Test
  public void testStringComparator()
  {
    TypeStrategy<String> strategy = ColumnType.STRING.getStrategy();
    Assertions.assertEquals(-1, strategy.compare("a", "b"));
    //noinspection EqualsWithItself
    Assertions.assertEquals(0, strategy.compare("a", "a"));
    Assertions.assertEquals(1, strategy.compare("b", "a"));
    Assertions.assertEquals(-48, strategy.compare("1", "a"));
    Assertions.assertEquals(48, strategy.compare("a", "1"));

    NullableTypeStrategy<String> nullableTypeStrategy = ColumnType.STRING.getNullableStrategy();
    Assertions.assertEquals(-1, nullableTypeStrategy.compare("a", "b"));
    Assertions.assertEquals(-1, nullableTypeStrategy.compare(null, "b"));
    //noinspection EqualsWithItself
    Assertions.assertEquals(0, nullableTypeStrategy.compare("a", "a"));
    Assertions.assertEquals(1, nullableTypeStrategy.compare("b", "a"));
    Assertions.assertEquals(1, nullableTypeStrategy.compare("b", null));
    Assertions.assertEquals(-48, nullableTypeStrategy.compare("1", "a"));
    Assertions.assertEquals(48, nullableTypeStrategy.compare("a", "1"));
  }

  @Test
  public void testDoubleComparator()
  {
    TypeStrategy<Double> strategy = ColumnType.DOUBLE.getStrategy();
    Assertions.assertEquals(-1, strategy.compare(0.01, 1.01));
    //noinspection EqualsWithItself
    Assertions.assertEquals(0, strategy.compare(0.00001, 0.00001));
    Assertions.assertEquals(1, strategy.compare(1.01, 0.01));

    NullableTypeStrategy nullableTypeStrategy = ColumnType.DOUBLE.getNullableStrategy();
    Assertions.assertEquals(-1, nullableTypeStrategy.compare(0.01, 1.01));
    Assertions.assertEquals(-1, nullableTypeStrategy.compare(null, 1.01));
    //noinspection EqualsWithItself
    Assertions.assertEquals(0, nullableTypeStrategy.compare(0.00001, 0.00001));
    Assertions.assertEquals(1, nullableTypeStrategy.compare(1.01, 0.01));
    Assertions.assertEquals(1, nullableTypeStrategy.compare(1.01, null));
  }

  @Test
  public void testFloatComparator()
  {
    TypeStrategy<Float> strategy = ColumnType.FLOAT.getStrategy();
    Assertions.assertEquals(-1, strategy.compare(0.01f, 1.01f));
    //noinspection EqualsWithItself
    Assertions.assertEquals(0, strategy.compare(0.00001f, 0.00001f));
    Assertions.assertEquals(1, strategy.compare(1.01f, 0.01f));

    NullableTypeStrategy<Float> nullableTypeStrategy = ColumnType.FLOAT.getNullableStrategy();
    Assertions.assertEquals(-1, nullableTypeStrategy.compare(0.01f, 1.01f));
    //noinspection EqualsWithItself
    Assertions.assertEquals(0, nullableTypeStrategy.compare(0.00001f, 0.00001f));
    Assertions.assertEquals(1, nullableTypeStrategy.compare(1.01f, 0.01f));
  }

  @Test
  public void testLongComparator()
  {
    TypeStrategy<Long> strategy = ColumnType.LONG.getStrategy();
    Assertions.assertEquals(-1, strategy.compare(-1L, 1L));
    //noinspection EqualsWithItself
    Assertions.assertEquals(0, strategy.compare(1L, 1L));
    Assertions.assertEquals(1, strategy.compare(1L, -1L));

    NullableTypeStrategy<Long> nullableTypeStrategy = ColumnType.LONG.getNullableStrategy();
    Assertions.assertEquals(-1, nullableTypeStrategy.compare(-1L, 1L));
    Assertions.assertEquals(-1, nullableTypeStrategy.compare(null, 1L));
    //noinspection EqualsWithItself
    Assertions.assertEquals(0, nullableTypeStrategy.compare(1L, 1L));
    Assertions.assertEquals(1, nullableTypeStrategy.compare(1L, -1L));
    Assertions.assertEquals(1, nullableTypeStrategy.compare(1L, null));
  }

  @Test
  public void testArrayComparator()
  {
    TypeStrategy<Object[]> strategy = ColumnType.LONG_ARRAY.getStrategy();
    Assertions.assertEquals(-1, strategy.compare(new Long[]{1L, 1L, 2L}, new Long[]{1L, 2L, 3L}));
    Assertions.assertEquals(-1, strategy.compare(new Long[]{1L, 2L}, new Long[]{1L, 2L, 3L}));
    Assertions.assertEquals(-1, strategy.compare(new Long[]{}, new Long[]{1L}));
    Assertions.assertEquals(-1, strategy.compare(null, new Long[]{}));
    //noinspection EqualsWithItself
    Assertions.assertEquals(0, strategy.compare(new Long[]{1L, 2L, 3L}, new Long[]{1L, 2L, 3L}));

    Assertions.assertEquals(1, strategy.compare(new Long[]{1L, 1L, 2L}, new Long[]{-1L, 2L, 3L}));
    Assertions.assertEquals(1, strategy.compare(new Long[]{1L, 2L, 2L}, new Long[]{1L, 2L, -3L}));
    Assertions.assertEquals(1, strategy.compare(new Long[]{1L, 2L}, new Long[]{-1L, 2L, 3L}));
    Assertions.assertEquals(1, strategy.compare(new Long[]{1L, 2L}, null));

    NullableTypeStrategy<Object[]> nullableTypeStrategy = ColumnType.LONG_ARRAY.getNullableStrategy();
    Assertions.assertEquals(-1, nullableTypeStrategy.compare(new Long[]{1L, 1L, 2L}, new Long[]{1L, 2L, 3L}));
    Assertions.assertEquals(-1, nullableTypeStrategy.compare(new Long[]{1L, 2L}, new Long[]{1L, 2L, 3L}));
    Assertions.assertEquals(-1, nullableTypeStrategy.compare(new Long[]{}, new Long[]{1L}));
    Assertions.assertEquals(-1, nullableTypeStrategy.compare(null, new Long[]{}));
    //noinspection EqualsWithItself
    Assertions.assertEquals(0, nullableTypeStrategy.compare(new Long[]{1L, 2L, 3L}, new Long[]{1L, 2L, 3L}));

    Assertions.assertEquals(1, nullableTypeStrategy.compare(new Long[]{1L, 1L, 2L}, new Long[]{-1L, 2L, 3L}));
    Assertions.assertEquals(1, nullableTypeStrategy.compare(new Long[]{1L, 2L, 2L}, new Long[]{1L, 2L, -3L}));
    Assertions.assertEquals(1, nullableTypeStrategy.compare(new Long[]{1L, 2L}, new Long[]{-1L, 2L, 3L}));
    Assertions.assertEquals(1, nullableTypeStrategy.compare(new Long[]{1L, 2L}, null));

    strategy = ColumnType.ofArray(ColumnType.ofArray(ColumnType.DOUBLE)).getStrategy();
    Assertions.assertEquals(
        -1,
        strategy.compare(
            new Object[]{new Object[]{1.0, 2.0}},
            new Object[]{new Object[]{1.0, 2.0}, new Object[]{1.1, -12.345}}
        )
    );
    Assertions.assertEquals(
        -1,
        strategy.compare(
            new Object[]{new Object[]{1.0, 2.0}, new Object[]{1.1, -23.456}},
            new Object[]{new Object[]{1.0, 2.0}, new Object[]{1.1, -12.345}}
        )
    );
    Assertions.assertEquals(
        -1,
        strategy.compare(
            null,
            new Object[]{new Object[]{1.0, 2.0}, new Object[]{1.1, -12.345}}
        )
    );
    Assertions.assertEquals(
        -1,
        strategy.compare(
            new Object[]{new Object[]{1.0, 2.0}, null},
            new Object[]{new Object[]{1.0, 2.0}, new Object[]{1.1, -12.345}}
        )
    );

    //noinspection EqualsWithItself
    Assertions.assertEquals(
        0,
        strategy.compare(
            new Object[]{new Object[]{1.0, 2.0}, null},
            new Object[]{new Object[]{1.0, 2.0}, null}
        )
    );

    Assertions.assertEquals(
        1,
        strategy.compare(
            new Object[]{new Object[]{1.0, 2.1}},
            new Object[]{new Object[]{1.0, 2.0}, new Object[]{1.1, -12.345}}
        )
    );
    Assertions.assertEquals(
        1,
        strategy.compare(
            new Object[]{new Object[]{1.0, 2.0}, new Object[]{1.1, -23.456}},
            new Object[]{new Object[]{1.0, 2.0}, null}
        )
    );

    nullableTypeStrategy = ColumnType.ofArray(ColumnType.ofArray(ColumnType.DOUBLE)).getNullableStrategy();
    Assertions.assertEquals(
        -1,
        nullableTypeStrategy.compare(
            new Object[]{new Object[]{1.0, 2.0}},
            new Object[]{new Object[]{1.0, 2.0}, new Object[]{1.1, -12.345}}
        )
    );
    Assertions.assertEquals(
        -1,
        nullableTypeStrategy.compare(
            new Object[]{new Object[]{1.0, 2.0}, new Object[]{1.1, -23.456}},
            new Object[]{new Object[]{1.0, 2.0}, new Object[]{1.1, -12.345}}
        )
    );
    Assertions.assertEquals(
        -1,
        nullableTypeStrategy.compare(
            null,
            new Object[]{new Object[]{1.0, 2.0}, new Object[]{1.1, -12.345}}
        )
    );
    Assertions.assertEquals(
        -1,
        nullableTypeStrategy.compare(
            new Object[]{new Object[]{1.0, 2.0}, null},
            new Object[]{new Object[]{1.0, 2.0}, new Object[]{1.1, -12.345}}
        )
    );

    //noinspection EqualsWithItself
    Assertions.assertEquals(
        0,
        nullableTypeStrategy.compare(
            new Object[]{new Object[]{1.0, 2.0}, null},
            new Object[]{new Object[]{1.0, 2.0}, null}
        )
    );

    Assertions.assertEquals(
        1,
        nullableTypeStrategy.compare(
            new Object[]{new Object[]{1.0, 2.1}},
            new Object[]{new Object[]{1.0, 2.0}, new Object[]{1.1, -12.345}}
        )
    );
    Assertions.assertEquals(
        1,
        nullableTypeStrategy.compare(
            new Object[]{new Object[]{1.0, 2.0}, new Object[]{1.1, -23.456}},
            new Object[]{new Object[]{1.0, 2.0}, null}
        )
    );
  }

  @Test
  public void testJsonComparator()
  {
    TypeStrategy<StructuredData> strategy = ColumnType.NESTED_DATA.getStrategy();
    Assertions.assertEquals(-1, strategy.compare(StructuredData.wrap(null), StructuredData.wrap(Map.of("key", "val"))));

    NullableTypeStrategy<StructuredData> nullableTypeStrategy = ColumnType.NESTED_DATA.getNullableStrategy();
    Assertions.assertEquals(-1, nullableTypeStrategy.compare(null, StructuredData.wrap(Map.of("key", "val"))));
    Assertions.assertEquals(0, nullableTypeStrategy.compare(
        StructuredData.wrap(Map.of("key1", Map.of("sub-key1", "sub-val1"), "key2", "val2")),
        StructuredData.wrap(Map.of("key1", Map.of("sub-key1", "sub-val1"), "key2", "val2"))
    ));
    // hash value is computed based on serialized bytes
    Assertions.assertEquals(-1, nullableTypeStrategy.compare(
        StructuredData.wrap(Map.of("key1", Map.of("sub-key1", "sub-val1-different"), "key2", "val2")),
        StructuredData.wrap(Map.of("key1", Map.of("sub-key1", "sub-val1"), "key2", "val2"))
    ));
  }

  @Test
  public void testNulls()
  {
    int offset = 0;
    TypeStrategies.writeNull(buffer, offset);
    Assertions.assertTrue(TypeStrategies.isNullableNull(buffer, offset));

    // test non-zero offset
    offset = 128;
    TypeStrategies.writeNull(buffer, offset);
    Assertions.assertTrue(TypeStrategies.isNullableNull(buffer, offset));
  }

  @Test
  public void testNonNullNullableLongBinary()
  {
    final long someLong = 12345567L;
    int offset = 0;
    int bytesWritten = TypeStrategies.writeNotNullNullableLong(buffer, offset, someLong);
    Assertions.assertEquals(1 + Long.BYTES, bytesWritten);
    Assertions.assertFalse(TypeStrategies.isNullableNull(buffer, offset));
    Assertions.assertEquals(someLong, TypeStrategies.readNotNullNullableLong(buffer, offset));

    // test non-zero offset
    offset = 1024;
    bytesWritten = TypeStrategies.writeNotNullNullableLong(buffer, offset, someLong);
    Assertions.assertEquals(1 + Long.BYTES, bytesWritten);
    Assertions.assertFalse(TypeStrategies.isNullableNull(buffer, offset));
    Assertions.assertEquals(someLong, TypeStrategies.readNotNullNullableLong(buffer, offset));
  }

  @Test
  public void testNonNullNullableDoubleBinary()
  {
    final double someDouble = 1.234567;
    int offset = 0;
    int bytesWritten = TypeStrategies.writeNotNullNullableDouble(buffer, offset, someDouble);
    Assertions.assertEquals(1 + Double.BYTES, bytesWritten);
    Assertions.assertFalse(TypeStrategies.isNullableNull(buffer, offset));
    Assertions.assertEquals(someDouble, TypeStrategies.readNotNullNullableDouble(buffer, offset), 0);

    // test non-zero offset
    offset = 1024;
    bytesWritten = TypeStrategies.writeNotNullNullableDouble(buffer, offset, someDouble);
    Assertions.assertEquals(1 + Double.BYTES, bytesWritten);
    Assertions.assertFalse(TypeStrategies.isNullableNull(buffer, offset));
    Assertions.assertEquals(someDouble, TypeStrategies.readNotNullNullableDouble(buffer, offset), 0);
  }

  @Test
  public void testNonNullNullableFloatBinary()
  {
    final float someFloat = 1.234567f;
    int offset = 0;
    int bytesWritten = TypeStrategies.writeNotNullNullableFloat(buffer, offset, someFloat);
    Assertions.assertEquals(1 + Float.BYTES, bytesWritten);
    Assertions.assertFalse(TypeStrategies.isNullableNull(buffer, offset));
    Assertions.assertEquals(someFloat, TypeStrategies.readNotNullNullableFloat(buffer, offset), 0);

    // test non-zero offset
    offset = 1024;
    bytesWritten = TypeStrategies.writeNotNullNullableFloat(buffer, offset, someFloat);
    Assertions.assertEquals(1 + Float.BYTES, bytesWritten);
    Assertions.assertFalse(TypeStrategies.isNullableNull(buffer, offset));
    Assertions.assertEquals(someFloat, TypeStrategies.readNotNullNullableFloat(buffer, offset), 0);
  }

  @Test
  public void testCheckMaxSize()
  {
    final IAE exception = Assertions.assertThrows(
        IAE.class,
        () -> TypeStrategies.checkMaxSize(1 << 10, 2048, ColumnType.STRING)
    );
    Assertions.assertEquals(
        "Unable to write [STRING], maxSizeBytes [2048] is greater than available [1024]",
        exception.getMessage()
    );
  }

  @Test
  public void testCheckMaxSizePosition()
  {
    final int maxSize = 1 << 10;
    final ByteBuffer buffer = ByteBuffer.allocate(maxSize);
    buffer.position(1000);
    final IAE exception = Assertions.assertThrows(
        IAE.class,
        () -> TypeStrategies.checkMaxSize(buffer.remaining(), maxSize, ColumnType.STRING)
    );
    Assertions.assertEquals(
        "Unable to write [STRING], maxSizeBytes [1024] is greater than available [24]",
        exception.getMessage()
    );
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
  public void testComplexJsonTypeStrategy()
  {
    final TypeStrategy strategy = TypeStrategies.getComplex(ColumnType.NESTED_DATA.getComplexTypeName());
    Map<String, Object> nested = new HashMap<>();
    nested.put("key1", "val");
    assertStrategy(strategy, StructuredData.wrap(nested));
    nested.put("key2", null);
    assertStrategy(strategy, StructuredData.wrap(nested));
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
    final Object[] someObjectStringArray = new Object[]{"hello", "hi", null, "hey"};

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

  @Test
  public void testArrayTypeStrategyCloseToTheLimit()
  {
    TypeStrategy strategy = new TypeStrategies.ArrayTypeStrategy(ColumnType.STRING_ARRAY);
    String filler = "AAAAAAAAAA";
    // test runs at offset 10, and 5 bytes for array null byte and size int, so
    int size = (int) Math.floor(
        (double) (buffer.capacity() - 5) / (double) ColumnType.STRING.getNullableStrategy().estimateSizeBytes(filler)
    );
    Object[] filler_array = new Object[size];
    Arrays.fill(filler_array, filler);
    assertArrayStrategy(strategy, filler_array, buffer.capacity(), 0);
  }

  private <T> void assertStrategy(TypeStrategy strategy, @Nullable T value)
  {
    final int maxSize = 2048;
    final int expectedLength = strategy.estimateSizeBytes(value);
    Assertions.assertNotEquals(0, expectedLength);

    // test buffer
    int offset = 10;
    buffer.position(offset);
    Assertions.assertEquals(expectedLength, strategy.write(buffer, value, maxSize));
    Assertions.assertEquals(expectedLength, buffer.position() - offset);
    buffer.position(offset);
    Assertions.assertEquals(value, strategy.read(buffer));
    Assertions.assertEquals(expectedLength, buffer.position() - offset);

    // test buffer nullable write read value
    NullableTypeStrategy nullableTypeStrategy = new NullableTypeStrategy(strategy);
    buffer.position(offset);
    Assertions.assertEquals(1 + expectedLength, nullableTypeStrategy.write(buffer, value, maxSize));
    Assertions.assertEquals(1 + expectedLength, buffer.position() - offset);
    buffer.position(offset);
    Assertions.assertEquals(value, nullableTypeStrategy.read(buffer));
    Assertions.assertEquals(1 + expectedLength, buffer.position() - offset);

    // test buffer nullable write read null
    buffer.position(offset);
    Assertions.assertEquals(1, nullableTypeStrategy.write(buffer, null, maxSize));
    Assertions.assertEquals(1, buffer.position() - offset);
    buffer.position(offset);
    Assertions.assertNull(nullableTypeStrategy.read(buffer));
    Assertions.assertEquals(1, buffer.position() - offset);

    buffer.position(0);

    // test buffer offset
    Assertions.assertEquals(expectedLength, strategy.write(buffer, 1024, value, maxSize));
    Assertions.assertEquals(value, strategy.read(buffer, 1024));
    Assertions.assertEquals(0, buffer.position());

    // test buffer offset nullable write read value
    Assertions.assertEquals(1 + expectedLength, nullableTypeStrategy.write(buffer, 1024, value, maxSize));
    Assertions.assertEquals(value, nullableTypeStrategy.read(buffer, 1024));
    Assertions.assertEquals(0, buffer.position());

    // test buffer offset nullable write read null
    Assertions.assertEquals(1, nullableTypeStrategy.write(buffer, 1024, null, maxSize));
    Assertions.assertNull(nullableTypeStrategy.read(buffer, 1024));
    Assertions.assertEquals(0, buffer.position());
  }

  private void assertArrayStrategy(TypeStrategy strategy, @Nullable Object[] value)
  {
    final int maxSize = 2048;
    final int expectedLength = strategy.estimateSizeBytes(value);
    Assertions.assertNotEquals(0, expectedLength);

    // basic tests at some position and offset
    assertArrayStrategy(strategy, value, maxSize, 10);

    buffer.position(0);

    // test buffer offset when with different position
    NullableTypeStrategy nullableTypeStrategy = new NullableTypeStrategy(strategy);
    Assertions.assertEquals(expectedLength, strategy.write(buffer, 1024, value, maxSize));
    Assertions.assertArrayEquals(value, (Object[]) strategy.read(buffer, 1024));
    Assertions.assertEquals(0, buffer.position());

    // test buffer offset nullable write read value
    Assertions.assertEquals(1 + expectedLength, nullableTypeStrategy.write(buffer, 1024, value, maxSize));
    Assertions.assertArrayEquals(value, (Object[]) nullableTypeStrategy.read(buffer, 1024));
    Assertions.assertEquals(0, buffer.position());

    // test buffer offset nullable write read null
    Assertions.assertEquals(1, nullableTypeStrategy.write(buffer, 1024, null, maxSize));
    Assertions.assertNull(nullableTypeStrategy.read(buffer, 1024));
    Assertions.assertEquals(0, buffer.position());
  }

  private void assertArrayStrategy(TypeStrategy strategy, @Nullable Object[] value, int maxSize, int offset)
  {
    final int expectedLength = strategy.estimateSizeBytes(value);
    Assertions.assertNotEquals(0, expectedLength);

    // test buffer
    buffer.position(offset);
    Assertions.assertEquals(expectedLength, strategy.write(buffer, value, maxSize));
    Assertions.assertEquals(expectedLength, buffer.position() - offset);
    buffer.position(offset);
    Assertions.assertArrayEquals(value, (Object[]) strategy.read(buffer));
    Assertions.assertEquals(expectedLength, buffer.position() - offset);

    // test buffer nullable write read value
    NullableTypeStrategy nullableTypeStrategy = new NullableTypeStrategy(strategy);
    buffer.position(offset);
    Assertions.assertEquals(1 + expectedLength, nullableTypeStrategy.write(buffer, value, maxSize));
    Assertions.assertEquals(1 + expectedLength, buffer.position() - offset);
    buffer.position(offset);
    Assertions.assertArrayEquals(value, (Object[]) nullableTypeStrategy.read(buffer));
    Assertions.assertEquals(1 + expectedLength, buffer.position() - offset);

    // test buffer nullable write read null
    buffer.position(offset);
    Assertions.assertEquals(1, nullableTypeStrategy.write(buffer, null, maxSize));
    Assertions.assertEquals(1, buffer.position() - offset);
    buffer.position(offset);
    Assertions.assertNull(nullableTypeStrategy.read(buffer));
    Assertions.assertEquals(1, buffer.position() - offset);
  }

  public static class NullableLongPair extends Pair<Long, Long> implements Comparable<NullableLongPair>
  {
    public NullableLongPair(@Nullable Long lhs, @Nullable Long rhs)
    {
      super(lhs, rhs);
    }

    @Override
    public boolean equals(final Object o)
    {
      return super.equals(o);
    }

    @Override
    public int hashCode()
    {
      return super.hashCode();
    }

    @Override
    public int compareTo(final NullableLongPair o)
    {
      final int lhsComparison = Comparators.<Long>naturalNullsFirst().compare(lhs, o.lhs);
      return lhsComparison != 0
             ? lhsComparison
             : Comparators.<Long>naturalNullsFirst().compare(rhs, o.rhs);
    }
  }

  public static class NullableLongPairTypeStrategy implements TypeStrategy<NullableLongPair>
  {

    private Ordering<NullableLongPair> ordering = Comparators.naturalNullsFirst();

    @Override
    public int compare(Object o1, Object o2)
    {
      return ordering.compare((NullableLongPair) o1, (NullableLongPair) o2);
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
    public boolean readRetainsBufferReference()
    {
      return false;
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

    @Override
    public NullableLongPair fromBytes(byte[] value)
    {
      return read(ByteBuffer.wrap(value));
    }

    @Override
    public boolean groupable()
    {
      return false;
    }
  }

  @Test
  public void getComplexTypeNull()
  {
    assertNull(TypeStrategies.getComplex(null));
  }
}
