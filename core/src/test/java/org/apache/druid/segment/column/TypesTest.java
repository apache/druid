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
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Comparators;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class TypesTest
{
  ByteBuffer buffer = ByteBuffer.allocate(1 << 16);

  public static ColumnType NULLABLE_TEST_PAIR_TYPE = ColumnType.ofComplex("nullableLongPair");

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @BeforeClass
  public static void setup()
  {
    Types.registerStrategy(NULLABLE_TEST_PAIR_TYPE.getComplexTypeName(), new PairObjectByteStrategy());
  }

  @Test
  public void testIs()
  {
    Assert.assertTrue(Types.is(ColumnType.LONG, ValueType.LONG));
    Assert.assertTrue(Types.is(ColumnType.DOUBLE, ValueType.DOUBLE));
    Assert.assertTrue(Types.is(ColumnType.FLOAT, ValueType.FLOAT));
    Assert.assertTrue(Types.is(ColumnType.STRING, ValueType.STRING));
    Assert.assertTrue(Types.is(ColumnType.LONG_ARRAY, ValueType.ARRAY));
    Assert.assertTrue(Types.is(ColumnType.LONG_ARRAY.getElementType(), ValueType.LONG));
    Assert.assertTrue(Types.is(ColumnType.DOUBLE_ARRAY, ValueType.ARRAY));
    Assert.assertTrue(Types.is(ColumnType.DOUBLE_ARRAY.getElementType(), ValueType.DOUBLE));
    Assert.assertTrue(Types.is(ColumnType.STRING_ARRAY, ValueType.ARRAY));
    Assert.assertTrue(Types.is(ColumnType.STRING_ARRAY.getElementType(), ValueType.STRING));
    Assert.assertTrue(Types.is(NULLABLE_TEST_PAIR_TYPE, ValueType.COMPLEX));

    Assert.assertFalse(Types.is(ColumnType.LONG, ValueType.DOUBLE));
    Assert.assertFalse(Types.is(ColumnType.DOUBLE, ValueType.FLOAT));

    Assert.assertFalse(Types.is(null, ValueType.STRING));
    Assert.assertTrue(Types.isNullOr(null, ValueType.STRING));
  }

  @Test
  public void testNullOrAnyOf()
  {
    Assert.assertTrue(Types.isNullOrAnyOf(ColumnType.LONG, ValueType.STRING, ValueType.LONG, ValueType.DOUBLE));
    Assert.assertFalse(Types.isNullOrAnyOf(ColumnType.DOUBLE, ValueType.STRING, ValueType.LONG, ValueType.FLOAT));
    Assert.assertTrue(Types.isNullOrAnyOf(null, ValueType.STRING, ValueType.LONG, ValueType.FLOAT));
  }

  @Test
  public void testEither()
  {
    Assert.assertTrue(Types.either(ColumnType.LONG, ColumnType.DOUBLE, ValueType.DOUBLE));
    Assert.assertFalse(Types.either(ColumnType.LONG, ColumnType.STRING, ValueType.DOUBLE));
  }

  @Test
  public void testRegister()
  {
    ObjectByteStrategy<?> strategy = Types.getStrategy(NULLABLE_TEST_PAIR_TYPE.getComplexTypeName());
    Assert.assertNotNull(strategy);
    Assert.assertTrue(strategy instanceof PairObjectByteStrategy);
  }

  @Test
  public void testRegisterDuplicate()
  {
    Types.registerStrategy(NULLABLE_TEST_PAIR_TYPE.getComplexTypeName(), new PairObjectByteStrategy());
    ObjectByteStrategy<?> strategy = Types.getStrategy(NULLABLE_TEST_PAIR_TYPE.getComplexTypeName());
    Assert.assertNotNull(strategy);
    Assert.assertTrue(strategy instanceof PairObjectByteStrategy);
  }

  @Test
  public void testConflicting()
  {
    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage(
        "Incompatible strategy for type[nullableLongPair] already exists."
        + " Expected [org.apache.druid.segment.column.TypesTest$1],"
        + " found [org.apache.druid.segment.column.TypesTest$PairObjectByteStrategy]."
    );

    Types.registerStrategy(NULLABLE_TEST_PAIR_TYPE.getComplexTypeName(), new ObjectByteStrategy<String>()
    {
      @Override
      public int compare(String o1, String o2)
      {
        return 0;
      }

      @Override
      public Class<? extends String> getClazz()
      {
        return null;
      }

      @Nullable
      @Override
      public String fromByteBuffer(ByteBuffer buffer, int numBytes)
      {
        return null;
      }

      @Nullable
      @Override
      public byte[] toBytes(@Nullable String val)
      {
        return new byte[0];
      }
    });
  }

  @Test
  public void testNulls()
  {
    int offset = 0;
    Types.writeNull(buffer, offset);
    Assert.assertTrue(Types.isNullableNull(buffer, offset));

    // test non-zero offset
    offset = 128;
    Types.writeNull(buffer, offset);
    Assert.assertTrue(Types.isNullableNull(buffer, offset));
  }

  @Test
  public void testNonNullNullableLongBinary()
  {
    final long someLong = 12345567L;
    int offset = 0;
    int bytesWritten = Types.writeNullableLong(buffer, offset, someLong);
    Assert.assertEquals(1 + Long.BYTES, bytesWritten);
    Assert.assertFalse(Types.isNullableNull(buffer, offset));
    Assert.assertEquals(someLong, Types.readNullableLong(buffer, offset));

    // test non-zero offset
    offset = 1024;
    bytesWritten = Types.writeNullableLong(buffer, offset, someLong);
    Assert.assertEquals(1 + Long.BYTES, bytesWritten);
    Assert.assertFalse(Types.isNullableNull(buffer, offset));
    Assert.assertEquals(someLong, Types.readNullableLong(buffer, offset));
  }

  @Test
  public void testNonNullNullableDoubleBinary()
  {
    final double someDouble = 1.234567;
    int offset = 0;
    int bytesWritten = Types.writeNullableDouble(buffer, offset, someDouble);
    Assert.assertEquals(1 + Double.BYTES, bytesWritten);
    Assert.assertFalse(Types.isNullableNull(buffer, offset));
    Assert.assertEquals(someDouble, Types.readNullableDouble(buffer, offset), 0);

    // test non-zero offset
    offset = 1024;
    bytesWritten = Types.writeNullableDouble(buffer, offset, someDouble);
    Assert.assertEquals(1 + Double.BYTES, bytesWritten);
    Assert.assertFalse(Types.isNullableNull(buffer, offset));
    Assert.assertEquals(someDouble, Types.readNullableDouble(buffer, offset), 0);
  }

  @Test
  public void testNonNullNullableFloatBinary()
  {
    final float someFloat = 12345567L;
    int offset = 0;
    int bytesWritten = Types.writeNullableFloat(buffer, offset, someFloat);
    Assert.assertEquals(1 + Float.BYTES, bytesWritten);
    Assert.assertFalse(Types.isNullableNull(buffer, offset));
    Assert.assertEquals(someFloat, Types.readNullableFloat(buffer, offset), 0);

    // test non-zero offset
    offset = 1024;
    bytesWritten = Types.writeNullableFloat(buffer, offset, someFloat);
    Assert.assertEquals(1 + Float.BYTES, bytesWritten);
    Assert.assertFalse(Types.isNullableNull(buffer, offset));
    Assert.assertEquals(someFloat, Types.readNullableFloat(buffer, offset), 0);
  }

  @Test
  public void testNullableVariableBlob()
  {
    String someString = "hello";
    byte[] stringBytes = StringUtils.toUtf8(someString);
    int offset = 0;
    int bytesWritten = Types.writeNullableVariableBlob(buffer, offset, stringBytes);
    Assert.assertEquals(1 + Integer.BYTES + stringBytes.length, bytesWritten);
    Assert.assertFalse(Types.isNullableNull(buffer, offset));
    Assert.assertArrayEquals(stringBytes, Types.readNullableVariableBlob(buffer, offset));

    // test non-zero offset
    offset = 1024;
    bytesWritten = Types.writeNullableVariableBlob(buffer, offset, stringBytes);
    Assert.assertEquals(1 + Integer.BYTES + stringBytes.length, bytesWritten);
    Assert.assertFalse(Types.isNullableNull(buffer, offset));
    Assert.assertArrayEquals(stringBytes, Types.readNullableVariableBlob(buffer, offset));

    // test null
    bytesWritten = Types.writeNullableVariableBlob(buffer, offset, null);
    Assert.assertEquals(1, bytesWritten);
    Assert.assertTrue(Types.isNullableNull(buffer, offset));
  }

  @Test
  public void testNullableVariableBlobTooBig()
  {
    expectedException.expect(ISE.class);
    expectedException.expectMessage("Unable to serialize [STRING], size [10] is larger than max [5]");
    String someString = "hello";
    byte[] stringBytes = StringUtils.toUtf8(someString);
    int offset = 0;
    Types.writeNullableVariableBlob(buffer, offset, stringBytes, ColumnType.STRING, stringBytes.length);
  }

  @Test
  public void testArrays()
  {
    final Long[] longArray = new Long[]{1L, 1234567L, null, 10L};
    final Double[] doubleArray = new Double[]{1.23, 4.567, null, 8.9};
    final String[] stringArray = new String[]{"hello", "world", null, ""};

    int bytesWritten;
    int offset = 0;
    bytesWritten = Types.writeNullableLongArray(buffer, offset, longArray, buffer.limit());
    Assert.assertEquals(33, bytesWritten);
    Assert.assertFalse(Types.isNullableNull(buffer, offset));
    Assert.assertArrayEquals(longArray, Types.readNullableLongArray(buffer, offset));

    bytesWritten = Types.writeNullableDoubleArray(buffer, offset, doubleArray, buffer.limit());
    Assert.assertEquals(33, bytesWritten);
    Assert.assertFalse(Types.isNullableNull(buffer, offset));
    Assert.assertArrayEquals(doubleArray, Types.readNullableDoubleArray(buffer, offset));

    bytesWritten = Types.writeNullableStringArray(buffer, offset, stringArray, buffer.limit());
    Assert.assertEquals(31, bytesWritten);
    Assert.assertFalse(Types.isNullableNull(buffer, offset));
    Assert.assertArrayEquals(stringArray, Types.readNullableStringArray(buffer, offset));

    offset = 1024;
    bytesWritten = Types.writeNullableLongArray(buffer, offset, longArray, buffer.limit());
    Assert.assertEquals(33, bytesWritten);
    Assert.assertFalse(Types.isNullableNull(buffer, offset));
    Assert.assertArrayEquals(longArray, Types.readNullableLongArray(buffer, offset));

    bytesWritten = Types.writeNullableDoubleArray(buffer, offset, doubleArray, buffer.limit());
    Assert.assertEquals(33, bytesWritten);
    Assert.assertFalse(Types.isNullableNull(buffer, offset));
    Assert.assertArrayEquals(doubleArray, Types.readNullableDoubleArray(buffer, offset));

    bytesWritten = Types.writeNullableStringArray(buffer, offset, stringArray, buffer.limit());
    Assert.assertEquals(31, bytesWritten);
    Assert.assertFalse(Types.isNullableNull(buffer, offset));
    Assert.assertArrayEquals(stringArray, Types.readNullableStringArray(buffer, offset));

    // test nulls
    bytesWritten = Types.writeNullableLongArray(buffer, offset, null, buffer.limit());
    Assert.assertEquals(1, bytesWritten);
    Assert.assertTrue(Types.isNullableNull(buffer, offset));

    bytesWritten = Types.writeNullableDoubleArray(buffer, offset, null, buffer.limit());
    Assert.assertEquals(1, bytesWritten);
    Assert.assertTrue(Types.isNullableNull(buffer, offset));

    bytesWritten = Types.writeNullableStringArray(buffer, offset, null, buffer.limit());
    Assert.assertEquals(1, bytesWritten);
    Assert.assertTrue(Types.isNullableNull(buffer, offset));
  }

  @Test
  public void testLongArrayToBig()
  {
    expectedException.expect(ISE.class);
    expectedException.expectMessage("Unable to serialize [ARRAY<LONG>], size [14] is larger than max [10]");
    final Long[] longArray = new Long[]{1L, 1234567L, null, 10L};
    Types.writeNullableLongArray(buffer, 0, longArray, 10);
  }

  @Test
  public void testDoubleArrayToBig()
  {
    expectedException.expect(ISE.class);
    expectedException.expectMessage("Unable to serialize [ARRAY<DOUBLE>], size [14] is larger than max [10]");
    final Double[] doubleArray = new Double[]{1.23, 4.567, null, 8.9};
    Types.writeNullableDoubleArray(buffer, 0, doubleArray, 10);
  }

  @Test
  public void testStringArrayToBig()
  {
    expectedException.expect(ISE.class);
    expectedException.expectMessage("Unable to serialize [ARRAY<STRING>], size [15] is larger than max [10]");
    final String[] stringArray = new String[]{"hello", "world", null, ""};
    Types.writeNullableStringArray(buffer, 0, stringArray, 10);
  }


  @Test
  public void testComplex()
  {
    NullableLongPair lp1 = new NullableLongPair(null, 1L);
    NullableLongPair lp2 = new NullableLongPair(1234L, 5678L);
    NullableLongPair lp3 = new NullableLongPair(1234L, null);

    int bytesWritten;
    int offset = 0;
    bytesWritten = Types.writeNullableComplexType(buffer, offset, NULLABLE_TEST_PAIR_TYPE, lp1, buffer.limit());
    // 1 (not null) + 4 (length) + 1 (null) + 0 (lhs) + 1 (not null) + 8 (rhs)
    Assert.assertEquals(15, bytesWritten);
    Assert.assertFalse(Types.isNullableNull(buffer, offset));
    Assert.assertEquals(lp1, Types.readNullableComplexType(buffer, offset, NULLABLE_TEST_PAIR_TYPE));

    // 1 (not null) + 4 (length) + 1 (not null) + 8 (lhs) + 1 (not null) + 8 (rhs)
    bytesWritten = Types.writeNullableComplexType(buffer, offset, NULLABLE_TEST_PAIR_TYPE, lp2, buffer.limit());
    Assert.assertEquals(23, bytesWritten);
    Assert.assertFalse(Types.isNullableNull(buffer, offset));
    Assert.assertEquals(lp2, Types.readNullableComplexType(buffer, offset, NULLABLE_TEST_PAIR_TYPE));

    // 1 (not null) + 4 (length) + 1 (not null) + 8 (lhs) + 1 (null) + 0 (rhs)
    bytesWritten = Types.writeNullableComplexType(buffer, offset, NULLABLE_TEST_PAIR_TYPE, lp3, buffer.limit());
    Assert.assertEquals(15, bytesWritten);
    Assert.assertFalse(Types.isNullableNull(buffer, offset));
    Assert.assertEquals(lp3, Types.readNullableComplexType(buffer, offset, NULLABLE_TEST_PAIR_TYPE));
  }

  @Test
  public void testComplexTooBig()
  {
    expectedException.expect(ISE.class);
    expectedException.expectMessage("Unable to serialize [COMPLEX<nullableLongPair>], size [23] is larger than max [10]");
    Types.writeNullableComplexType(
        buffer,
        0,
        NULLABLE_TEST_PAIR_TYPE,
        new NullableLongPair(1234L, 5678L),
        10
    );
  }
  
  public static class PairObjectByteStrategy implements ObjectByteStrategy<NullableLongPair>
  {
    @Override
    public Class<? extends NullableLongPair> getClazz()
    {
      return NullableLongPair.class;
    }

    @Nullable
    @Override
    public NullableLongPair fromByteBuffer(ByteBuffer buffer, int numBytes)
    {
      int position = buffer.position();
      Long lhs = null;
      Long rhs = null;
      if (!Types.isNullableNull(buffer, position)) {
        lhs = Types.readNullableLong(buffer, position);
        position += 1 + Long.BYTES;
      } else {
        position++;
      }
      if (!Types.isNullableNull(buffer, position)) {
        rhs = Types.readNullableLong(buffer, position);
      }
      return new NullableLongPair(lhs, rhs);
    }

    @Nullable
    @Override
    public byte[] toBytes(@Nullable NullableLongPair val)
    {
      byte[] bytes = new byte[1 + Long.BYTES + 1 + Long.BYTES];
      ByteBuffer buffer = ByteBuffer.wrap(bytes);
      int position = 0;
      if (val != null) {
        if (val.lhs != null) {
          position += Types.writeNullableLong(buffer, position, val.lhs);
        } else {
          position += Types.writeNull(buffer, position);
        }
        if (val.rhs != null) {
          position += Types.writeNullableLong(buffer, position, val.rhs);
        } else {
          position += Types.writeNull(buffer, position);
        }
        return Arrays.copyOfRange(bytes, 0, position);
      } else {
        return null;
      }
    }

    @Override
    public int compare(NullableLongPair o1, NullableLongPair o2)
    {
      return Comparators.<NullableLongPair>naturalNullsFirst().compare(o1, o2);
    }
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
}
