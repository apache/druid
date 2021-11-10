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
 * IMPORTANT!!! DO NOT USE THIS FOR WRITING COLUMNS, THERE ARE VERY LIKELY FAR BETTER WAYS TO DO THIS. However, if you
 * need to store a single value or small number of values, continue reading.
 *
 * Implementations of this mechanism support reading and writing ONLY non-null values. To read and write nullable
 * values and you have enough memory to burn a full byte for every value you want to store, consider using the
 * {@link TypeStrategies#readNullableType} and {@link TypeStrategies#writeNullableType} family of
 * methods, which will store values with a leading byte containing either {@link NullHandling#IS_NULL_BYTE} or
 * {@link NullHandling#IS_NOT_NULL_BYTE} as appropriate. If you have a lot of values to write and a lot of nulls,
 * consider alternative approaches to tracking your nulls.
 *
 * This mechanism allows using the natural {@link ByteBuffer#position()} and modify the underlying position as they
 * operate, and also random access reads are specific offets, which do not modify the underlying position. If a method
 * accepts an offset parameter, it does not modify the position, if not, it does.
 *
 * The only methods implementors are required to provide are {@link #read(ByteBuffer)},
 * {@link #write(ByteBuffer, Object)} and {@link #estimateSizeBytes(Object)}, default implementations are provided
 * to set and reset buffer positions as appropriate for the offset based methods, but may be overridden if a more
 * optimized implementation is needed.
 */
public interface TypeStrategy<T> extends Comparator<T>
{
  /**
   * The size in bytes that writing this value to memory would require, useful for constraining the values maximum size
   *
   * This does not include the null byte, use {@link #estimateSizeBytesNullable(Object)} instead.
   */
  int estimateSizeBytes(@Nullable T value);

  /**
   * The size in bytes that writing this value to memory would require, including the null byte, useful for constraining
   * the values maximum size. If the value is null, the size will be {@link Byte#BYTES}, otherwise it will be
   * {@link Byte#BYTES} + {@link #estimateSizeBytes(Object)}
   */
  default int estimateSizeBytesNullable(@Nullable T value)
  {
    if (value == null) {
      return Byte.BYTES;
    }
    return Byte.BYTES + estimateSizeBytes(value);
  }

  /**
   * Read a non-null value from the {@link ByteBuffer} at the current {@link ByteBuffer#position()}. This will move
   * the underlying position by the size of the value read.
   *
   * The contract of this method is that any value returned from this method MUST be completely detached from the
   * underlying {@link ByteBuffer}, since it might outlive the memory location being allocated to hold the object.
   * In other words, if an object is memory mapped, it must be copied on heap, or relocated to another memory location
   * that is owned by the caller with {@link #write}.
   */
  T read(ByteBuffer buffer);

  /**
   * Write a non-null value to the {@link ByteBuffer} at position {@link ByteBuffer#position()}. This will move the
   * underlying position by the size of the value written.
   *
   * Callers should ensure the {@link ByteBuffer} has adequate capacity before writing values, use
   * {@link #estimateSizeBytes(Object)} to determine the required size of a value before writing if the size
   * is unknown.
   */
  void write(ByteBuffer buffer, T value);

  /**
   * Read a non-null value from the {@link ByteBuffer} at the requested position. This will not permanently move the
   * underlying {@link ByteBuffer#position()}.
   *
   * The contract of this method is that any value returned from this method MUST be completely detached from the
   * underlying {@link ByteBuffer}, since it might outlive the memory location being allocated to hold the object.
   * In other words, if an object is memory mapped, it must be copied on heap, or relocated to another memory location
   * that is owned by the caller with {@link #write}.
   */
  default T read(ByteBuffer buffer, int offset)
  {
    final int oldPosition = buffer.position();
    buffer.position(offset);
    T value = read(buffer);
    buffer.position(oldPosition);
    return value;
  }

  /**
   * Write a non-null value to the {@link ByteBuffer} at the requested position. This will not permanently move the
   * underlying {@link ByteBuffer#position()}, and returns the number of bytes written.
   *
   * Callers should ensure the {@link ByteBuffer} has adequate capacity before writing values, use
   * {@link #estimateSizeBytes(Object)} to determine the required size of a value before writing if the size
   * is unknown.
   */
  default int write(ByteBuffer buffer, int offset, T value)
  {
    final int oldPosition = buffer.position();
    buffer.position(offset);
    write(buffer, value);
    final int size = buffer.position() - offset;
    buffer.position(oldPosition);
    return size;
  }
}
