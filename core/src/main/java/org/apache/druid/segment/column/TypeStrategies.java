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
import org.apache.druid.java.util.common.IAE;
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
  public static int writeNotNullNullableLong(ByteBuffer buffer, int offset, long value)
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
  public static long readNotNullNullableLong(ByteBuffer buffer, int offset)
  {
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
  public static int writeNotNullNullableDouble(ByteBuffer buffer, int offset, double value)
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
  public static double readNotNullNullableDouble(ByteBuffer buffer, int offset)
  {
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
  public static int writeNotNullNullableFloat(ByteBuffer buffer, int offset, float value)
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
  public static float readNotNullNullableFloat(ByteBuffer buffer, int offset)
  {
    return buffer.getFloat(offset + VALUE_OFFSET);
  }

  public static void checkMaxSize(int available, int maxSizeBytes, TypeSignature<?> signature)
  {
    if (maxSizeBytes > available) {
      throw new IAE(
          "Unable to write [%s], maxSizeBytes [%s] is greater than available [%s]",
          signature.asTypeString(),
          maxSizeBytes,
          available
      );
    }
  }

  /**
   * Read and write non-null LONG values. If reading non-null values, consider just using {@link ByteBuffer#getLong}
   * directly, or if reading values written with {@link NullableTypeStrategy}, using {@link #isNullableNull} and
   * {@link #readNotNullNullableLong}, both of which allow dealing in primitive long values instead of objects.
   */
  public static final class LongTypeStrategy implements TypeStrategy<Long>
  {
    private static final Comparator<Long> COMPARATOR = Longs::compare;

    @Override
    public int estimateSizeBytes(Long value)
    {
      return Long.BYTES;
    }

    @Override
    public Long read(ByteBuffer buffer)
    {
      return buffer.getLong();
    }

    @Override
    public int write(ByteBuffer buffer, Long value, int maxSizeBytes)
    {
      checkMaxSize(buffer.remaining(), maxSizeBytes, ColumnType.LONG);
      final int sizeBytes = Long.BYTES;
      final int remaining = maxSizeBytes - sizeBytes;
      if (remaining >= 0) {
        buffer.putLong(value);
        return sizeBytes;
      }
      return remaining;
    }

    @Override
    public int compare(Long o1, Long o2)
    {
      return COMPARATOR.compare(o1, o2);
    }
  }

  /**
   * Read and write non-null FLOAT values. If reading non-null values, consider just using {@link ByteBuffer#getFloat}
   * directly, or if reading values written with {@link NullableTypeStrategy}, using {@link #isNullableNull} and
   * {@link #readNotNullNullableFloat}, both of which allow dealing in primitive float values instead of objects.
   */
  public static final class FloatTypeStrategy implements TypeStrategy<Float>
  {
    private static final Comparator<Float> COMPARATOR = Floats::compare;

    @Override
    public int estimateSizeBytes(Float value)
    {
      return Float.BYTES;
    }

    @Override
    public Float read(ByteBuffer buffer)
    {
      return buffer.getFloat();
    }

    @Override
    public int write(ByteBuffer buffer, Float value, int maxSizeBytes)
    {
      checkMaxSize(buffer.remaining(), maxSizeBytes, ColumnType.FLOAT);
      final int sizeBytes = Float.BYTES;
      final int remaining = maxSizeBytes - sizeBytes;
      if (remaining >= 0) {
        buffer.putFloat(value);
        return sizeBytes;
      }
      return remaining;
    }

    @Override
    public int compare(Float o1, Float o2)
    {
      return COMPARATOR.compare(o1, o2);
    }
  }

  /**
   * Read and write non-null DOUBLE values. If reading non-null values, consider just using {@link ByteBuffer#getDouble}
   * directly, or if reading values written with {@link NullableTypeStrategy}, using {@link #isNullableNull} and
   * {@link #readNotNullNullableDouble}, both of which allow dealing in primitive double values instead of objects.
   */
  public static final class DoubleTypeStrategy implements TypeStrategy<Double>
  {
    private static final Comparator<Double> COMPARATOR = Double::compare;

    @Override
    public int estimateSizeBytes(Double value)
    {
      return Double.BYTES;
    }

    @Override
    public Double read(ByteBuffer buffer)
    {
      return buffer.getDouble();
    }

    @Override
    public int write(ByteBuffer buffer, Double value, int maxSizeBytes)
    {
      checkMaxSize(buffer.remaining(), maxSizeBytes, ColumnType.DOUBLE);
      final int sizeBytes = Double.BYTES;
      final int remaining = maxSizeBytes - sizeBytes;
      if (remaining >= 0) {
        buffer.putDouble(value);
        return sizeBytes;
      }
      return remaining;
    }

    @Override
    public int compare(Double o1, Double o2)
    {
      return COMPARATOR.compare(o1, o2);
    }
  }

  /**
   * Read and write non-null UTF8 encoded String values. Encodes the length in bytes as an integer prefix followed by
   * the actual encoded value bytes.
   *
   * format: | length (int) | bytes |
   */
  public static final class StringTypeStrategy implements TypeStrategy<String>
  {
    // copy of lexicographical comparator
    private static final Ordering<String> ORDERING = Ordering.from(String::compareTo);

    @Override
    public int estimateSizeBytes(String value)
    {
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
    public int write(ByteBuffer buffer, String value, int maxSizeBytes)
    {
      checkMaxSize(buffer.remaining(), maxSizeBytes, ColumnType.STRING);
      final byte[] bytes = StringUtils.toUtf8(value);
      final int sizeBytes = Integer.BYTES + bytes.length;
      final int remaining = maxSizeBytes - sizeBytes;
      if (remaining >= 0) {
        buffer.putInt(bytes.length);
        buffer.put(bytes, 0, bytes.length);
        return sizeBytes;
      }
      return remaining;
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

  /**
   * Read and write a non-null ARRAY which is permitted to have null elements (all elements are always read and written
   * with a {@link NullableTypeStrategy} wrapper on the {@link TypeStrategy} of the
   * {@link TypeSignature#getElementType()}.
   *
   * Encodes the number of elements in the array as an integer prefix followed by the actual encoded value bytes of
   * each element serially.
   */
  public static final class ArrayTypeStrategy implements TypeStrategy<Object[]>
  {
    private final Comparator<Object> elementComparator;
    private final TypeSignature<?> arrayType;
    private final NullableTypeStrategy elementStrategy;

    public ArrayTypeStrategy(TypeSignature<?> type)
    {
      this.arrayType = type;
      this.elementStrategy = type.getElementType().getNullableStrategy();
      this.elementComparator = Comparator.nullsFirst(elementStrategy);
    }

    @Override
    public int estimateSizeBytes(Object[] value)
    {
      return Integer.BYTES + Arrays.stream(value).mapToInt(elementStrategy::estimateSizeBytes).sum();
    }

    @Override
    public Object[] read(ByteBuffer buffer)
    {
      final int arrayLength = buffer.getInt();
      final Object[] array = new Object[arrayLength];
      for (int i = 0; i < arrayLength; i++) {
        array[i] = elementStrategy.read(buffer);
      }
      return array;
    }

    @Override
    public int write(ByteBuffer buffer, Object[] value, int maxSizeBytes)
    {
      checkMaxSize(buffer.remaining(), maxSizeBytes, arrayType);
      int sizeBytes = Integer.BYTES;
      int remaining = maxSizeBytes - sizeBytes;
      if (remaining < 0) {
        return remaining;
      }
      int extraNeeded = 0;

      buffer.putInt(value.length);
      for (Object o : value) {
        int written = elementStrategy.write(buffer, o, remaining);
        if (written < 0) {
          extraNeeded += written;
          remaining = 0;
        } else {
          sizeBytes += written;
          remaining -= sizeBytes;
        }
      }
      return extraNeeded < 0 ? extraNeeded : sizeBytes;
    }

    @Override
    public int compare(@Nullable Object[] o1, @Nullable Object[] o2)
    {
      //noinspection ArrayEquality
      if (o1 == o2) {
        return 0;
      }
      if (o1 == null) {
        return -1;
      }
      if (o2 == null) {
        return 1;
      }
      final int iter = Math.min(o1.length, o2.length);
      for (int i = 0; i < iter; i++) {
        final int cmp = elementComparator.compare(o1[i], o2[i]);
        if (cmp == 0) {
          continue;
        }
        return cmp;
      }
      return Integer.compare(o1.length, o2.length);
    }
  }
}
