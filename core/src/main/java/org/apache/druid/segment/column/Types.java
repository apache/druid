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
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

public class Types
{
  private static final String ARRAY_PREFIX = "ARRAY<";
  private static final String COMPLEX_PREFIX = "COMPLEX<";
  private static final int VALUE_OFFSET = Byte.BYTES;
  private static final int NULLABLE_LONG_SIZE = Byte.BYTES + Long.BYTES;
  private static final int NULLABLE_DOUBLE_SIZE = Byte.BYTES + Double.BYTES;
  private static final int NULLABLE_FLOAT_SIZE = Byte.BYTES + Float.BYTES;
  private static final ConcurrentHashMap<String, ObjectByteStrategy<?>> STRATEGIES = new ConcurrentHashMap<>();

  /**
   * Create a {@link TypeSignature} given the value of {@link TypeSignature#asTypeString()} and a {@link TypeFactory}
   */
  @Nullable
  public static <T extends TypeSignature<?>> T fromString(TypeFactory<T> typeFactory, @Nullable String typeString)
  {
    if (typeString == null) {
      return null;
    }
    switch (StringUtils.toUpperCase(typeString)) {
      case "STRING":
        return typeFactory.ofString();
      case "LONG":
        return typeFactory.ofLong();
      case "FLOAT":
        return typeFactory.ofFloat();
      case "DOUBLE":
        return typeFactory.ofDouble();
      case "STRING_ARRAY":
        return typeFactory.ofArray(typeFactory.ofString());
      case "LONG_ARRAY":
        return typeFactory.ofArray(typeFactory.ofLong());
      case "DOUBLE_ARRAY":
        return typeFactory.ofArray(typeFactory.ofDouble());
      case "COMPLEX":
        return typeFactory.ofComplex(null);
      default:
        // we do not convert to uppercase here, because complex type name must be preserved in original casing
        // array could be converted, but are not for no particular reason other than less spooky magic
        if (typeString.startsWith(ARRAY_PREFIX)) {
          T elementType = fromString(typeFactory, typeString.substring(ARRAY_PREFIX.length(), typeString.length() - 1));
          Preconditions.checkNotNull(elementType, "Array element type must not be null");
          return typeFactory.ofArray(elementType);
        }
        if (typeString.startsWith(COMPLEX_PREFIX)) {
          return typeFactory.ofComplex(typeString.substring(COMPLEX_PREFIX.length(), typeString.length() - 1));
        }
    }
    return null;
  }

  /**
   * Returns true if {@link TypeSignature#getType()} is of the specified {@link TypeDescriptor}
   */
  public static <T extends TypeDescriptor> boolean is(@Nullable TypeSignature<T> typeSignature, T typeDescriptor)
  {
    return typeSignature != null && typeSignature.is(typeDescriptor);
  }

  /**
   * Returns true if {@link TypeSignature#getType()} is null, or of the specified {@link TypeDescriptor}
   */
  public static <T extends TypeDescriptor> boolean isNullOr(@Nullable TypeSignature<T> typeSignature, T typeDescriptor)
  {
    return typeSignature == null || typeSignature.is(typeDescriptor);
  }

  /**
   * Returns true if the {@link TypeSignature} is null, or is any one of the specified {@link TypeDescriptor}
   */
  public static <T extends TypeDescriptor> boolean isNullOrAnyOf(
      @Nullable TypeSignature<T> typeSignature,
      T... typeDescriptors
  )
  {
    return typeSignature == null || typeSignature.anyOf(typeDescriptors);
  }

  /**
   * Returns true if either supplied {@link TypeSignature#getType()} is the given {@link TypeDescriptor}
   *
   * Useful for choosing a common {@link TypeDescriptor} between two {@link TypeSignature} when one of the signatures
   * might be null.
   */
  public static <T extends TypeDescriptor> boolean either(
      @Nullable TypeSignature<T> typeSignature1,
      @Nullable TypeSignature<T> typeSignature2,
      T typeDescriptor
  )
  {
    return (typeSignature1 != null && typeSignature1.is(typeDescriptor)) ||
           (typeSignature2 != null && typeSignature2.is(typeDescriptor));
  }

  /**
   * Get an {@link ObjectByteStrategy} registered to some {@link TypeSignature#getComplexTypeName()}.
   */
  @Nullable
  public static ObjectByteStrategy<?> getStrategy(String type)
  {
    return STRATEGIES.get(type);
  }

  /**
   * hmm... this might look familiar... (see ComplexMetrics)
   *
   * Register a complex type name -> {@link ObjectByteStrategy} mapping.
   *
   * If the specified type name or type id are already used and the supplied {@link ObjectByteStrategy} is not of the
   * same type as the existing value in the map for said key, an {@link ISE} is thrown.
   *
   * @param strategy The {@link ObjectByteStrategy} object to be associated with the 'type' in the map.
   */
  public static void registerStrategy(String typeName, ObjectByteStrategy<?> strategy)
  {
    Preconditions.checkNotNull(typeName);
    STRATEGIES.compute(typeName, (key, value) -> {
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
   * Write a variably lengthed byte[] value to a {@link ByteBuffer} at the supplied offset. The first byte is set to
   * {@link NullHandling#IS_NULL_BYTE} or {@link NullHandling#IS_NOT_NULL_BYTE} as appropriate, and if the byte[] value
   * is not null, the size in bytes is written as an integer in the next 4 bytes, followed by the byte[] value itself.
   *
   * layout: | null (byte) | size (int) | byte[] |
   *
   * This method checks that no more than the specified maximum number of bytes can be written to the buffer, and the
   * proper function of this method requires that the buffer contains at least that many bytes free from the starting
   * offset. See {@link #writeNullableVariableBlob(ByteBuffer, int, byte[])} if you do not need to check the length
   * of the byte array, or wish to perform the check externally.
   *
   * This method does not change the buffer position, limit, or mark, because it does not expect to own the buffer
   * given to it (i.e. buffer aggs)
   *
   * @return number of bytes written (1 if null, or 5 + size of byte[] if not)
   */
  public static int writeNullableVariableBlob(
      ByteBuffer buffer,
      int offset,
      @Nullable byte[] value,
      TypeSignature<?> type,
      int maxSizeBytes
  )
  {
    if (value == null) {
      return writeNull(buffer, offset);
    }
    // | null (byte) | length (int) | bytes |
    checkMaxBytes(
        type,
        1 + Integer.BYTES + value.length,
        maxSizeBytes
    );
    return writeNullableVariableBlob(buffer, offset, value);
  }

  /**
   * Write a variably lengthed byte[] value to a {@link ByteBuffer} at the supplied offset. The first byte is set to
   * {@link NullHandling#IS_NULL_BYTE} or {@link NullHandling#IS_NOT_NULL_BYTE} as appropriate, and if the byte[] value
   * is not null, the size in bytes is written as an integer in the next 4 bytes, followed by the byte[] value itself.
   *
   * layout: | null (byte) | size (int) | byte[] |
   *
   * This method does not constrain the number of bytes written to the buffer, so either use
   * {@link #writeNullableVariableBlob(ByteBuffer, int, byte[], TypeSignature, int)} or first check that the size
   * of the byte array plus 5 bytes is available in the buffer before using this method.
   *
   * This method does not change the buffer position, limit, or mark, because it does not expect to own the buffer
   * given to it (i.e. buffer aggs)
   *
   * @return number of bytes written (1 if null, or 5 + size of byte[] if not)
   */
  public static int writeNullableVariableBlob(ByteBuffer buffer, int offset, @Nullable byte[] value)
  {
    // | null (byte) | length (int) | bytes |
    final int size;
    if (value == null) {
      return writeNull(buffer, offset);
    }
    final int oldPosition = buffer.position();
    buffer.position(offset);
    buffer.put(NullHandling.IS_NOT_NULL_BYTE);
    buffer.putInt(value.length);
    buffer.put(value, 0, value.length);
    size = buffer.position() - offset;
    buffer.position(oldPosition);
    return size;
  }

  /**
   * Reads a nullable variably lengthed byte[] value from a {@link ByteBuffer} at the supplied offset. If the null byte
   * is set to {@link NullHandling#IS_NULL_BYTE}, this method will return null, else it will read the next 4 bytes to
   * get the byte[] size followed by that many bytes to extract the value.
   *
   * layout: | null (byte) | size (int) | byte[] |
   *
   * This method does not change the buffer position, limit, or mark, because it does not expect to own the buffer
   * given to it (i.e. buffer aggs)
   */
  @Nullable
  public static byte[] readNullableVariableBlob(ByteBuffer buffer, int offset)
  {
    // | null (byte) | length (int) | bytes |
    final int length = buffer.getInt(offset + VALUE_OFFSET);
    final byte[] blob = new byte[length];
    final int oldPosition = buffer.position();
    buffer.position(offset + VALUE_OFFSET + Integer.BYTES);
    buffer.get(blob, 0, length);
    buffer.position(oldPosition);
    return blob;
  }

  /**
   * Write a variably lengthed Long[] value to a {@link ByteBuffer} at the supplied offset. The first byte is set to
   * {@link NullHandling#IS_NULL_BYTE} or {@link NullHandling#IS_NOT_NULL_BYTE} as appropriate, and if the Long[] value
   * is not null, the size in bytes is written as an integer in the next 4 bytes. Elements of the array are each written
   * out with {@link #writeNull} if null, or {@link #writeNullableLong} if not, taking either 1 or 9 bytes each. If the
   * total byte size of serializing the array is larger than the max size parameter, this method will explode via a call
   * to {@link #checkMaxBytes}.
   *
   * layout: | null (byte) | size (int) | {| null (byte) | long |, | null (byte) |, ... |null (byte) | long |} |
   *
   * This method checks that no more than the specified maximum number of bytes can be written to the buffer, and the
   * proper function of this method requires that the buffer contains at least that many bytes free from the starting
   * offset.
   *
   * This method does not change the buffer position, limit, or mark, because it does not expect to own the buffer
   * given to it (i.e. buffer aggs)
   *
   * @return number of bytes written (1 if null, or 5 + size of Long[] if not)
   */
  public static int writeNullableLongArray(ByteBuffer buffer, int offset, @Nullable Long[] array, int maxSizeBytes)
  {
    // | null (byte) | array length (int) | array bytes |
    if (array == null) {
      return writeNull(buffer, offset);
    }
    int sizeBytes = 1 + Integer.BYTES;

    buffer.put(offset, NullHandling.IS_NOT_NULL_BYTE);
    buffer.putInt(offset + 1, array.length);
    for (Long element : array) {
      if (element != null) {
        checkMaxBytes(
            ColumnType.LONG_ARRAY,
            sizeBytes + 1 + Long.BYTES,
            maxSizeBytes
        );
        sizeBytes += writeNullableLong(buffer, offset + sizeBytes, element);
      } else {
        checkMaxBytes(
            ColumnType.LONG_ARRAY,
            sizeBytes + 1,
            maxSizeBytes
        );
        sizeBytes += writeNull(buffer, offset + sizeBytes);
      }
    }
    return sizeBytes;
  }

  /**
   * Reads a nullable variably lengthed Long[] value from a {@link ByteBuffer} at the supplied offset. If the null byte
   * is set to {@link NullHandling#IS_NULL_BYTE}, this method will return null, else it will read the size of the array
   * from the next 4 bytes and then read that many elements with {@link #isNullableNull} and {@link #readNullableLong}.
   *
   * layout: | null (byte) | size (int) | {| null (byte) | long |, | null (byte) |, ... |null (byte) | long |} |
   *
   * This method does not change the buffer position, limit, or mark, because it does not expect to own the buffer
   * given to it (i.e. buffer aggs)
   */
  @Nullable
  public static Long[] readNullableLongArray(ByteBuffer buffer, int offset)
  {
    // | null (byte) | array length (int) | array bytes |
    if (isNullableNull(buffer, offset++)) {
      return null;
    }
    final int longArrayLength = buffer.getInt(offset);
    offset += Integer.BYTES;
    final Long[] longs = new Long[longArrayLength];
    for (int i = 0; i < longArrayLength; i++) {
      if (isNullableNull(buffer, offset)) {
        longs[i] = null;
      } else {
        longs[i] = readNullableLong(buffer, offset);
        offset += Long.BYTES;
      }
      offset++;
    }
    return longs;
  }

  /**
   * Write a variably lengthed Double[] value to a {@link ByteBuffer} at the supplied offset. The first byte is set to
   * {@link NullHandling#IS_NULL_BYTE} or {@link NullHandling#IS_NOT_NULL_BYTE} as appropriate, and if the Long[] value
   * is not null, the size in bytes is written as an integer in the next 4 bytes. Elements of the array are each written
   * out with {@link #writeNull} if null, or {@link #writeNullableDouble} if not, taking either 1 or 9 bytes each. If
   * the total byte size of serializing the array is larger than the max size parameter, this method will explode via a
   * call to {@link #checkMaxBytes}.
   *
   * layout: | null (byte) | size (int) | {| null (byte) | double |, | null (byte) |, ... |null (byte) | double |} |
   *
   * This method checks that no more than the specified maximum number of bytes can be written to the buffer, and the
   * proper function of this method requires that the buffer contains at least that many bytes free from the starting
   * offset.
   *
   * This method does not change the buffer position, limit, or mark, because it does not expect to own the buffer
   * given to it (i.e. buffer aggs)
   *
   * @return number of bytes written (1 if null, or 5 + size of Double[] if not)
   */
  public static int writeNullableDoubleArray(ByteBuffer buffer, int offset, @Nullable Double[] array, int maxSizeBytes)
  {
    // | null (byte) | array length (int) | array bytes |
    if (array == null) {
      return writeNull(buffer, offset);
    }
    int sizeBytes = 1 + Integer.BYTES;
    buffer.put(offset, NullHandling.IS_NOT_NULL_BYTE);
    buffer.putInt(offset + 1, array.length);
    for (Double element : array) {
      if (element != null) {
        checkMaxBytes(
            ColumnType.DOUBLE_ARRAY,
            sizeBytes + 1 + Double.BYTES,
            maxSizeBytes
        );
        sizeBytes += writeNullableDouble(buffer, offset + sizeBytes, element);
      } else {
        checkMaxBytes(
            ColumnType.DOUBLE_ARRAY,
            sizeBytes + 1,
            maxSizeBytes
        );
        sizeBytes += writeNull(buffer, offset + sizeBytes);
      }
    }
    return sizeBytes;
  }

  /**
   * Reads a nullable variably lengthed Double[] value from a {@link ByteBuffer} at the supplied offset. If the null
   * byte is set to {@link NullHandling#IS_NULL_BYTE}, this method will return null, else it will read the size of the
   * array from the next 4 bytes and then read that many elements with {@link #isNullableNull} and
   * {@link #readNullableDouble}.
   *
   * layout: | null (byte) | size (int) | {| null (byte) | double |, | null (byte) |, ... |null (byte) | double |} |
   *
   * This method does not change the buffer position, limit, or mark, because it does not expect to own the buffer
   * given to it (i.e. buffer aggs)
   */
  @Nullable
  public static Double[] readNullableDoubleArray(ByteBuffer buffer, int offset)
  {
    // | null (byte) | array length (int) | array bytes |
    if (isNullableNull(buffer, offset++)) {
      return null;
    }
    final int doubleArrayLength = buffer.getInt(offset);
    offset += Integer.BYTES;
    final Double[] doubles = new Double[doubleArrayLength];
    for (int i = 0; i < doubleArrayLength; i++) {
      if (isNullableNull(buffer, offset)) {
        doubles[i] = null;
      } else {
        doubles[i] = readNullableDouble(buffer, offset);
        offset += Double.BYTES;
      }
      offset++;
    }
    return doubles;
  }

  /**
   * Write a variably lengthed String[] value to a {@link ByteBuffer} at the supplied offset. The first byte is set to
   * {@link NullHandling#IS_NULL_BYTE} or {@link NullHandling#IS_NOT_NULL_BYTE} as appropriate, and if the String[]
   * value is not null, the size in bytes is written as an integer in the next 4 bytes. The Strings themselves are
   * encoded with {@link StringUtils#toUtf8} Elements of the array are each written out with {@link #writeNull} if null,
   * or {@link #writeNullableVariableBlob} if not, taking either 1 or 5 + the size of the utf8 byte array each. If the
   * total byte size of serializing the array is larger than the max size parameter, this method will explode via a
   * call to {@link #checkMaxBytes}.
   *
   * layout: | null (byte) | size (int) | {| null (byte) | size (int) | byte[] |, | null (byte) |, ... } |
   *
   * This method checks that no more than the specified maximum number of bytes can be written to the buffer, and the
   * proper function of this method requires that the buffer contains at least that many bytes free from the starting
   * offset.
   *
   * This method does not change the buffer position, limit, or mark, because it does not expect to own the buffer
   * given to it (i.e. buffer aggs)
   *
   * @return number of bytes written (1 if null, or 5 + size of String[] if not)
   */
  public static int writeNullableStringArray(ByteBuffer buffer, int offset, @Nullable String[] array, int maxSizeBytes)
  {
    // | null (byte) | array length (int) | array bytes |
    if (array == null) {
      return writeNull(buffer, offset);
    }
    int sizeBytes = 1 + Integer.BYTES;
    buffer.put(offset, NullHandling.IS_NOT_NULL_BYTE);
    buffer.putInt(offset + 1, array.length);
    for (String element : array) {
      if (element != null) {
        final byte[] stringElementBytes = StringUtils.toUtf8(element);
        checkMaxBytes(
            ColumnType.STRING_ARRAY,
            sizeBytes + 1 + Integer.BYTES + stringElementBytes.length,
            maxSizeBytes
        );
        sizeBytes += writeNullableVariableBlob(buffer, offset + sizeBytes, stringElementBytes);
      } else {
        checkMaxBytes(
            ColumnType.STRING_ARRAY,
            sizeBytes + 1,
            maxSizeBytes
        );
        sizeBytes += writeNull(buffer, offset + sizeBytes);
      }
    }
    return sizeBytes;
  }

  /**
   * Reads a nullable variably lengthed String[] value from a {@link ByteBuffer} at the supplied offset. If the null
   * byte is set to {@link NullHandling#IS_NULL_BYTE}, this method will return null, else it will read the size of the
   * array from the next 4 bytes and then read that many elements with {@link #readNullableVariableBlob} and decode them
   * with {@link StringUtils#fromUtf8} to convert to string values.
   *
   * layout: | null (byte) | size (int) | {| null (byte) | size (int) | byte[] |, | null (byte) |, ... } |
   *
   * This method does not change the buffer position, limit, or mark, because it does not expect to own the buffer
   * given to it (i.e. buffer aggs)
   */
  @Nullable
  public static String[] readNullableStringArray(ByteBuffer buffer, int offset)
  {
    // | null (byte) | array length (int) | array bytes |
    if (isNullableNull(buffer, offset++)) {
      return null;
    }
    final int stringArrayLength = buffer.getInt(offset);
    offset += Integer.BYTES;
    final String[] stringArray = new String[stringArrayLength];
    for (int i = 0; i < stringArrayLength; i++) {
      if (isNullableNull(buffer, offset)) {
        stringArray[i] = null;
      } else {
        final byte[] stringElementBytes = readNullableVariableBlob(buffer, offset);
        stringArray[i] = StringUtils.fromUtf8(stringElementBytes);
        offset += Integer.BYTES + stringElementBytes.length;
      }
      offset++;
    }
    return stringArray;
  }

  /**
   * Write a variably lengthed byte[] value derived from some {@link ObjectByteStrategy} for a complex
   * {@link TypeSignature} to a {@link ByteBuffer} at the supplied offset. The first byte is set to
   * {@link NullHandling#IS_NULL_BYTE} or {@link NullHandling#IS_NOT_NULL_BYTE} as appropriate, and if the value
   * is not null, the size in bytes is written as an integer in the next 4 bytes, followed by the byte[] value itself
   * from {@link ObjectByteStrategy#toBytes}.
   *
   * layout: | null (byte) | size (int) | byte[] |
   *
   * Note that the {@link TypeSignature#getComplexTypeName()} MUST have registered an {@link ObjectByteStrategy} with
   * {@link #registerStrategy} for this method to work, else a null pointer exception will be thrown.
   *
   * This method checks that no more than the specified maximum number of bytes can be written to the buffer, and the
   * proper function of this method requires that the buffer contains at least that many bytes free from the starting
   * offset.
   *
   * This method does not change the buffer position, limit, or mark, because it does not expect to own the buffer
   * given to it (i.e. buffer aggs)
   *
   * @return number of bytes written (1 if null, or 5 + size of byte[] if not)
   */
  public static <T> int writeNullableComplexType(
      ByteBuffer buffer,
      int offset,
      TypeSignature<?> type,
      @Nullable T value,
      int maxSizeBytes
  )
  {
    final ObjectByteStrategy strategy = Preconditions.checkNotNull(
        getStrategy(type.getComplexTypeName()),
        StringUtils.format(
            "Type %s has not registered an ObjectByteStrategy and cannot be written",
            type.asTypeString()
        )
    );
    if (value == null) {
      return writeNull(buffer, offset);
    }
    final byte[] complexBytes = strategy.toBytes(value);
    return writeNullableVariableBlob(buffer, offset, complexBytes, type, maxSizeBytes);
  }

  /**
   * Read a possibly null, variably lengthed byte[] value derived from some {@link ObjectByteStrategy} for a complex
   * {@link TypeSignature} from a {@link ByteBuffer} at the supplied offset. If the first byte is set to
   * {@link NullHandling#IS_NULL_BYTE}, this method will return null, and if the value is not null, the size in bytes
   * is read as an integer from the next 4 bytes, followed by the byte[] value itself from
   * {@link ObjectByteStrategy#fromByteBuffer}.
   *
   * layout: | null (byte) | size (int) | byte[] |
   *
   * Note that the {@link TypeSignature#getComplexTypeName()} MUST have registered an {@link ObjectByteStrategy} with
   * {@link #registerStrategy} for this method to work, else a null pointer exception will be thrown.
   *
   * This method does not change the buffer position, limit, or mark, because it does not expect to own the buffer
   * given to it (i.e. buffer aggs)
   */
  @Nullable
  public static Object readNullableComplexType(ByteBuffer buffer, int offset, TypeSignature<?> type)
  {
    if (isNullableNull(buffer, offset++)) {
      return null;
    }
    final ObjectByteStrategy strategy = Preconditions.checkNotNull(
        getStrategy(type.getComplexTypeName()),
        StringUtils.format(
            "Type %s has not registered an ObjectByteStrategy and cannot be read",
            type.asTypeString()
        )
    );
    final int complexLength = buffer.getInt(offset);
    offset += Integer.BYTES;
    ByteBuffer dupe = buffer.duplicate();
    dupe.position(offset);
    dupe.limit(offset + complexLength);
    return strategy.fromByteBuffer(dupe, complexLength);
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
}
