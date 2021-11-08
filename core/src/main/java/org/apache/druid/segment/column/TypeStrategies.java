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

import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Longs;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;

public class TypeStrategies
{
  public static final int VALUE_OFFSET = Byte.BYTES;
  public static final int NULLABLE_LONG_SIZE = Byte.BYTES + Long.BYTES;
  public static final int NULLABLE_DOUBLE_SIZE = Byte.BYTES + Double.BYTES;
  public static final int NULLABLE_FLOAT_SIZE = Byte.BYTES + Float.BYTES;

  public static final LongTypeStrategy LONG = new LongTypeStrategy();
  public static final FloatTypeStrategy FLOAT = new FloatTypeStrategy();
  public static final DoubleTypeStrategy DOUBLE = new DoubleTypeStrategy();
  public static final StringTypeStrategy STRING = new StringTypeStrategy();
  public static final ConcurrentHashMap<String, TypeStrategy<?>> COMPLEX_STRATEGIES = new ConcurrentHashMap<>();

  /**
   * Get an {@link TypeStrategy} registered to some {@link TypeSignature#getComplexTypeName()}.
   */
  @Nullable
  public static TypeStrategy<?> getComplex(String typeName)
  {
    return COMPLEX_STRATEGIES.get(typeName);
  }

  /**
   * hmm... this might look familiar... (see ComplexMetrics)
   *
   * Register a complex type name -> {@link TypeStrategy} mapping.
   *
   * If the specified type name is already used and the supplied {@link TypeStrategy} is not of the
   * same type as the existing value in the map for said key, an {@link ISE} is thrown.
   *
   * @param strategy The {@link TypeStrategy} object to be associated with the 'type' in the map.
   */
  public static void registerComplex(String typeName, TypeStrategy<?> strategy)
  {
    Preconditions.checkNotNull(typeName);
    COMPLEX_STRATEGIES.compute(typeName, (key, value) -> {
      if (value == null) {
        return strategy;
      } else {
        if (!value.getClass().getName().equals(strategy.getClass().getName())) {
          throw new ISE(
              "Incompatible strategy for type[%s] already exists. Expected [%s], found [%s].",
              key,
              strategy.getClass().getName(),
              value.getClass().getName()
          );
        } else {
          return value;
        }
      }
    });
  }

  /**
   * Clear and set the 'null' byte of a nullable value to {@link NullHandling#IS_NULL_BYTE} to a {@link ByteBuffer} at
   * the supplied position. This method does not change the buffer position, limit, or mark, because it does not expect
   * to own the buffer given to it (i.e. buffer aggs)
   *
   * Nullable types are stored with a leading byte to indicate if the value is null, followed by the value bytes
   * (if not null)
   *
   * layout: | null (byte) | value |
   *
   * @return number of bytes written (always 1)
   */
  public static int writeNull(ByteBuffer buffer, int offset)
  {
    buffer.put(offset, NullHandling.IS_NULL_BYTE);
    return 1;
  }

  /**
   * Checks if a 'nullable' value's null byte is set to {@link NullHandling#IS_NULL_BYTE}. This method will mask the
   * value of the null byte to only check if the null bit is set, meaning that the higher bits can be utilized for
   * flags as necessary (e.g. using high bits to indicate if the value has been set or not for aggregators).
   *
   * Note that writing nullable values with the methods of {@link Types} will always clear and set the null byte to
   * either {@link NullHandling#IS_NULL_BYTE} or {@link NullHandling#IS_NOT_NULL_BYTE}, losing any flag bits.
   *
   * layout: | null (byte) | value |
   */
  public static boolean isNullableNull(ByteBuffer buffer, int offset)
  {
    // use & so that callers can use the high bits of the null byte to pack additional information if necessary
    return (buffer.get(offset) & NullHandling.IS_NULL_BYTE) == NullHandling.IS_NULL_BYTE;
  }

  /**
   * Write a non-null long value to a {@link ByteBuffer} at the supplied offset. The first byte is always cleared and
   * set to {@link NullHandling#IS_NOT_NULL_BYTE}, the long value is written in the next 8 bytes.
   *
   * layout: | null (byte) | long |
   *
   * This method does not change the buffer position, limit, or mark, because it does not expect to own the buffer
   * given to it (i.e. buffer aggs)
   *
   * @return number of bytes written (always 9)
   */
  public static int writeNullableLong(ByteBuffer buffer, int offset, long value)
  {
    buffer.put(offset++, NullHandling.IS_NOT_NULL_BYTE);
    buffer.putLong(offset, value);
    return NULLABLE_LONG_SIZE;
  }

  /**
   * Reads a non-null long value from a {@link ByteBuffer} at the supplied offset. This method should only be called
   * if and only if {@link #isNullableNull} for the same offset returns false.
   *
   * layout: | null (byte) | long |
   *
   * This method does not change the buffer position, limit, or mark, because it does not expect  to own the buffer
   * given to it (i.e. buffer aggs)
   */
  public static long readNullableLong(ByteBuffer buffer, int offset)
  {
    assert !isNullableNull(buffer, offset);
    return buffer.getLong(offset + VALUE_OFFSET);
  }

  /**
   * Write a non-null double value to a {@link ByteBuffer} at the supplied offset. The first byte is always cleared and
   * set to {@link NullHandling#IS_NOT_NULL_BYTE}, the double value is written in the next 8 bytes.
   *
   * layout: | null (byte) | double |
   *
   * This method does not change the buffer position, limit, or mark, because it does not expect to own the buffer
   * given to it (i.e. buffer aggs)
   *
   * @return number of bytes written (always 9)
   */
  public static int writeNullableDouble(ByteBuffer buffer, int offset, double value)
  {
    buffer.put(offset++, NullHandling.IS_NOT_NULL_BYTE);
    buffer.putDouble(offset, value);
    return NULLABLE_DOUBLE_SIZE;
  }

  /**
   * Reads a non-null double value from a {@link ByteBuffer} at the supplied offset. This method should only be called
   * if and only if {@link #isNullableNull} for the same offset returns false.
   *
   * layout: | null (byte) | double |
   *
   * This method does not change the buffer position, limit, or mark, because it does not expect to own the buffer
   * given to it (i.e. buffer aggs)
   */
  public static double readNullableDouble(ByteBuffer buffer, int offset)
  {
    assert !isNullableNull(buffer, offset);
    return buffer.getDouble(offset + VALUE_OFFSET);
  }

  /**
   * Write a non-null float value to a {@link ByteBuffer} at the supplied offset. The first byte is always cleared and
   * set to {@link NullHandling#IS_NOT_NULL_BYTE}, the float value is written in the next 4 bytes.
   *
   * layout: | null (byte) | float |
   *
   * This method does not change the buffer position, limit, or mark, because it does not expect to own the buffer
   * given to it (i.e. buffer aggs)
   *
   * @return number of bytes written (always 5)
   */
  public static int writeNullableFloat(ByteBuffer buffer, int offset, float value)
  {
    buffer.put(offset++, NullHandling.IS_NOT_NULL_BYTE);
    buffer.putFloat(offset, value);
    return NULLABLE_FLOAT_SIZE;
  }

  /**
   * Reads a non-null float value from a {@link ByteBuffer} at the supplied offset. This method should only be called
   * if and only if {@link #isNullableNull} for the same offset returns false.
   *
   * layout: | null (byte) | float |
   *
   * This method does not change the buffer position, limit, or mark, because it does not expect to own the buffer
   * given to it (i.e. buffer aggs)
   */
  public static float readNullableFloat(ByteBuffer buffer, int offset)
  {
    assert !isNullableNull(buffer, offset);
    return buffer.getFloat(offset + VALUE_OFFSET);
  }

  /**
   * Throw an {@link ISE} for consistent error messaging if the size to be written is greater than the max size
   */
  public static void checkMaxBytes(TypeSignature<?> type, int sizeBytes, int maxSizeBytes)
  {
    if (sizeBytes > maxSizeBytes) {
      throw new ISE(
          "Unable to serialize [%s], size [%s] is larger than max [%s]",
          type.asTypeString(),
          sizeBytes,
          maxSizeBytes
      );
    }
  }

  public static final class LongTypeStrategy implements TypeStrategy<Long>
  {
    private static final Comparator<Long> COMPARATOR = Comparator.nullsFirst(Longs::compare);

    @Override
    public int estimateSizeBytes(@Nullable Long value)
    {
      return Long.BYTES;
    }

    @Override
    public Long read(ByteBuffer buffer)
    {
      return buffer.getLong();
    }

    @Override
    public void write(ByteBuffer buffer, Long value)
    {
      buffer.putLong(value);
    }

    @Override
    public int compare(Long o1, Long o2)
    {
      return COMPARATOR.compare(o1, o2);
    }
  }

  public static final class FloatTypeStrategy implements TypeStrategy<Float>
  {
    private static final Comparator<Float> COMPARATOR = Comparator.nullsFirst(Floats::compare);

    @Override
    public int estimateSizeBytes(@Nullable Float value)
    {
      return Float.BYTES;
    }

    @Override
    public Float read(ByteBuffer buffer)
    {
      return buffer.getFloat();
    }

    @Override
    public void write(ByteBuffer buffer, Float value)
    {
      buffer.putFloat(value);
    }

    @Override
    public int compare(Float o1, Float o2)
    {
      return COMPARATOR.compare(o1, o2);
    }
  }

  public static final class DoubleTypeStrategy implements TypeStrategy<Double>
  {
    private static final Comparator<Double> COMPARATOR = Comparator.nullsFirst(Double::compare);

    @Override
    public int estimateSizeBytes(@Nullable Double value)
    {
      return Double.BYTES;
    }

    @Override
    public Double read(ByteBuffer buffer)
    {
      return buffer.getDouble();
    }

    @Override
    public void write(ByteBuffer buffer, Double value)
    {
      buffer.putDouble(value);
    }

    @Override
    public int compare(Double o1, Double o2)
    {
      return COMPARATOR.compare(o1, o2);
    }
  }

  public static final class StringTypeStrategy implements TypeStrategy<String>
  {
    // copy of lexicographical comparator
    private static final Ordering<String> ORDERING = Ordering.from(String::compareTo).nullsFirst();

    @Override
    public int estimateSizeBytes(@Nullable String value)
    {
      if (value == null) {
        return 0;
      }
      return Integer.BYTES + StringUtils.toUtf8(value).length;
    }

    @Override
    public String read(ByteBuffer buffer)
    {
      // | length (int) | bytes |
      final int length = buffer.getInt();
      final byte[] blob = new byte[length];
      buffer.get(blob, 0, length);
      return StringUtils.fromUtf8(blob);
    }

    @Override
    public void write(ByteBuffer buffer, String value)
    {
      final byte[] bytes = StringUtils.toUtf8(value);
      buffer.putInt(bytes.length);
      buffer.put(bytes, 0, bytes.length);
    }

    @Override
    public int compare(String s, String s2)
    {
      // copy of lexicographical string comparator in druid processing
      // Avoid comparisons for equal references
      // Assuming we mostly compare different strings, checking s.equals(s2) will only make the comparison slower.
      //noinspection StringEquality
      if (s == s2) {
        return 0;
      }

      return ORDERING.compare(s, s2);
    }
  }

  public static final class ArrayTypeStrategy implements TypeStrategy<Object[]>
  {
    private final Comparator<Object> elementComparator;
    private final TypeStrategy elementStrategy;

    public ArrayTypeStrategy(TypeSignature<?> type)
    {
      this.elementStrategy = type.getElementType().getStrategy();
      this.elementComparator = Comparator.nullsFirst(elementStrategy);
    }

    @Override
    public int estimateSizeBytes(@Nullable Object[] value)
    {
      if (value == null) {
        return 0;
      }
      return Integer.BYTES + Arrays.stream(value).mapToInt(elementStrategy::estimateSizeBytesNullable).sum();
    }

    @Override
    public Object[] read(ByteBuffer buffer)
    {
      final int arrayLength = buffer.getInt();
      final Object[] array = new Object[arrayLength];
      for (int i = 0; i < arrayLength; i++) {
        array[i] = elementStrategy.readNullable(buffer);
      }
      return array;
    }

    @Override
    public void write(ByteBuffer buffer, Object[] value)
    {
      buffer.putInt(value.length);
      for (Object o : value) {
        elementStrategy.writeNullable(buffer, o);
      }
    }

    @Override
    public int compare(Object[] o1, Object[] o2)
    {
      final int iter = Math.max(o1.length, o2.length);

      //noinspection ArrayEquality
      if (o1 == o2) {
        return 0;
      }
      for (int i = 0; i < iter; i++) {
        final int cmp = elementComparator.compare(o1[i], o2[i]);
        if (cmp == 0) {
          continue;
        }
        return cmp;
      }
      if (o1.length > o2.length) {
        return -1;
      }
      return 1;
    }
  }
}
