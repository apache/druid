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

package org.apache.druid.frame.field;

import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.segment.BaseDoubleColumnValueSelector;

/**
 * Wraps a {@link BaseDoubleColumnValueSelector} and writes field values.
 *
 * See {@link DoubleFieldReader} for format details.
 */
public class DoubleFieldWriter implements FieldWriter
{
  public static final int SIZE = Double.BYTES + Byte.BYTES;

  // Different from the values in NullHandling, since we want to be able to sort as bytes, and we want
  // nulls to come before non-nulls.
  public static final byte NULL_BYTE = 0x00;
  public static final byte NOT_NULL_BYTE = 0x01;

  private final BaseDoubleColumnValueSelector selector;

  public DoubleFieldWriter(final BaseDoubleColumnValueSelector selector)
  {
    this.selector = selector;
  }

  @Override
  public long writeTo(final WritableMemory memory, final long position, final long maxSize)
  {
    if (maxSize < SIZE) {
      return -1;
    }

    if (selector.isNull()) {
      memory.putByte(position, NULL_BYTE);
      memory.putLong(position + Byte.BYTES, transform(0));
    } else {
      memory.putByte(position, NOT_NULL_BYTE);
      memory.putLong(position + Byte.BYTES, transform(selector.getDouble()));
    }

    return SIZE;
  }

  @Override
  public void close()
  {
    // Nothing to close.
  }

  /**
   * Transforms a double into a form where it can be compared as unsigned bytes without decoding.
   */
  public static long transform(final double n)
  {
    final long bits = Double.doubleToLongBits(n);
    final long mask = ((bits & Long.MIN_VALUE) >> 11) | Long.MIN_VALUE;
    return Long.reverseBytes(bits ^ mask);
  }

  /**
   * Inverse of {@link #transform}.
   */
  public static double detransform(final long bits)
  {
    final long reversedBits = Long.reverseBytes(bits);
    final long mask = (((reversedBits ^ Long.MIN_VALUE) & Long.MIN_VALUE) >> 11) | Long.MIN_VALUE;
    return Double.longBitsToDouble(reversedBits ^ mask);
  }
}
