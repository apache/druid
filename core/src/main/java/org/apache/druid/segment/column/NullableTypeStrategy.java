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
 * Wrapper of {@link TypeStrategy} for nullable types, which stores {@link NullHandling#IS_NULL_BYTE} or
 * {@link NullHandling#IS_NOT_NULL_BYTE} in the leading byte of any value, as appropriate. If the value is null, only
 * {@link NullHandling#IS_NULL_BYTE} will be set, otherwise, the value bytes will be written after the null byte.
 *
 * layout: | null (byte) | value (byte[]) |
 *
 * This is not the most efficient way to track nulls, it is recommended to only use this wrapper if you MUST store null
 * values.
 *
 * @see TypeStrategy
 */
public final class NullableTypeStrategy<T> implements Comparator<T>
{
  private final TypeStrategy<T> delegate;
  private final Comparator<T> delegateComparator;

  public NullableTypeStrategy(TypeStrategy<T> delegate)
  {
    this.delegate = delegate;
    this.delegateComparator = Comparator.nullsFirst(delegate::compare);
  }

  public int estimateSizeBytes(@Nullable T value)
  {
    if (value == null) {
      return Byte.BYTES;
    }
    return Byte.BYTES + delegate.estimateSizeBytes(value);
  }


  @Nullable
  public T read(ByteBuffer buffer)
  {
    if ((buffer.get() & NullHandling.IS_NULL_BYTE) == NullHandling.IS_NULL_BYTE) {
      return null;
    }
    return delegate.read(buffer);
  }

  public int write(ByteBuffer buffer, @Nullable T value, int maxSizeBytes)
  {
    final int max = Math.min(buffer.limit() - buffer.position(), maxSizeBytes);
    final int remaining = max - Byte.BYTES;
    if (remaining >= 0) {
      // if we have room left, write the null byte and the value
      if (value == null) {
        buffer.put(NullHandling.IS_NULL_BYTE);
        return Byte.BYTES;
      }
      buffer.put(NullHandling.IS_NOT_NULL_BYTE);
      int written = delegate.write(buffer, value, maxSizeBytes - Byte.BYTES);
      return written < 0 ? written : Byte.BYTES + written;
    } else {
      if (value == null) {
        return remaining;
      }
      // call delegate.write anyway to get the total amount of extra space needed to serialize the value
      return remaining + delegate.write(buffer, value, 0);
    }
  }

  @Nullable
  public T read(ByteBuffer buffer, int offset)
  {
    final int oldPosition = buffer.position();
    try {
      buffer.position(offset);
      T value = read(buffer);
      return value;
    }
    finally {
      buffer.position(oldPosition);
    }
  }

  /**
   * Whether the {@link #read} methods return an object that may retain a reference to the provided {@link ByteBuffer}.
   * If a reference is sometimes retained, this method returns true. It returns false if, and only if, a reference
   * is *never* retained.
   */
  public boolean readRetainsBufferReference()
  {
    return delegate.readRetainsBufferReference();
  }

  public int write(ByteBuffer buffer, int offset, @Nullable T value, int maxSizeBytes)
  {
    final int oldPosition = buffer.position();
    try {
      buffer.position(offset);
      return write(buffer, value, maxSizeBytes);
    }
    finally {
      buffer.position(oldPosition);
    }
  }

  @Override
  public int compare(T o1, T o2)
  {
    return delegateComparator.compare(o1, o2);
  }
}
