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

import org.apache.druid.common.config.NullHandling;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Comparator;

/**
 * TypeStrategy provides value comparison and binary serialization for Druid types. This can be obtained for ANY Druid
 * type via {@link TypeSignature#getStrategy()}.
 *
 * Implementations of this mechanism support writing both null and non-null values. When using the 'nullable' family
 * of the read and write methods, values are stored such that the leading byte contains either
 * {@link NullHandling#IS_NULL_BYTE} or {@link NullHandling#IS_NOT_NULL_BYTE} as appropriate. The default
 * implementations of these methods use masking to check the null bit, so flags may be used in the upper bits of the
 * null byte.
 *
 * This mechanism allows using the natural {@link ByteBuffer#position()} and modify the underlying position as they
 * operate, and also random access reads are specific offets, which do not modify the underlying position. If a method
 * accepts an offset parameter, it does not modify the position, if not, it does.
 *
 * The only methods implementors are required to provide are {@link #read(ByteBuffer)},
 * {@link #write(ByteBuffer, Object)} and {@link #estimateSizeBytes(Object)}, the rest provide default implementations
 * which set the null/not null byte, and reset buffer positions as appropriate, but may be overridden if a more
 * optimized implementation is needed.
 */
public interface TypeStrategy<T> extends Comparator<T>
{
  /**
   * The size in bytes that writing this value to memory would require, useful for constraining the values
   */
  int estimateSizeBytes(@Nullable T value);

  default int estimateSizeBytesNullable(@Nullable T value)
  {
    if (value == null) {
      return Byte.BYTES;
    }
    return Byte.BYTES + estimateSizeBytes(value);
  }

  /**
   * Read a non-null value from the {@link ByteBuffer} at the current {@link ByteBuffer#position()}
   */
  T read(ByteBuffer buffer);

  /**
   * Write a non-null value to the {@link ByteBuffer} at position {@link ByteBuffer#position()}
   */
  void write(ByteBuffer buffer, T value);

  @Nullable
  default T readNullable(ByteBuffer buffer)
  {
    if ((buffer.get() & NullHandling.IS_NULL_BYTE) == NullHandling.IS_NULL_BYTE) {
      return null;
    }
    return read(buffer);
  }

  default void writeNullable(ByteBuffer buffer, @Nullable T value)
  {
    if (value == null) {
      buffer.put(NullHandling.IS_NULL_BYTE);
      return;
    }
    buffer.put(NullHandling.IS_NOT_NULL_BYTE);
    write(buffer, value);
  }

  default T read(ByteBuffer buffer, int offset)
  {
    final int oldPosition = buffer.position();
    buffer.position(offset);
    T value = read(buffer);
    buffer.position(oldPosition);
    return value;
  }

  default int write(ByteBuffer buffer, int offset, T value)
  {
    final int oldPosition = buffer.position();
    buffer.position(offset);
    write(buffer, value);
    final int size = buffer.position() - offset;
    buffer.position(oldPosition);
    return size;
  }

  @Nullable
  default T readNullable(ByteBuffer buffer, int offset)
  {
    if (TypeStrategies.isNullableNull(buffer, offset)) {
      return null;
    }
    return read(buffer, offset + TypeStrategies.VALUE_OFFSET);
  }

  default int writeNullable(ByteBuffer buffer, int offset, @Nullable T value)
  {
    if (value == null) {
      return TypeStrategies.writeNull(buffer, offset);
    }
    buffer.put(offset, NullHandling.IS_NOT_NULL_BYTE);
    return Byte.BYTES + write(buffer, offset + TypeStrategies.VALUE_OFFSET, value);
  }
}
