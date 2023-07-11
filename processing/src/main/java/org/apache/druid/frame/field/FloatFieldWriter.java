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
import org.apache.druid.segment.BaseFloatColumnValueSelector;

/**
 * Wraps a {@link BaseFloatColumnValueSelector} and writes field values.
 *
 * See {@link FloatFieldReader} for format details.
 */
public class FloatFieldWriter implements FieldWriter
{
  public static final int SIZE = Float.BYTES + Byte.BYTES;

  // Different from the values in NullHandling, since we want to be able to sort as bytes, and we want
  // nulls to come before non-nulls.
  public static final byte NULL_BYTE = 0x00;
  public static final byte NOT_NULL_BYTE = 0x01;

  private final BaseFloatColumnValueSelector selector;

  public FloatFieldWriter(final BaseFloatColumnValueSelector selector)
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
      memory.putInt(position + Byte.BYTES, transform(0));
    } else {
      memory.putByte(position, NOT_NULL_BYTE);
      memory.putInt(position + Byte.BYTES, transform(selector.getFloat()));
    }

    return SIZE;
  }

  @Override
  public void close()
  {
    // Nothing to close.
  }

  /**
   * Transforms a float into a form where it can be compared as unsigned bytes without decoding.
   */
  public static int transform(final float n)
  {
    final int bits = Float.floatToIntBits(n);
    final int mask = ((bits & Integer.MIN_VALUE) >> 8) | Integer.MIN_VALUE;
    return Integer.reverseBytes(bits ^ mask);
  }

  /**
   * Inverse of {@link #transform}.
   */
  public static float detransform(final int bits)
  {
    final int reversedBits = Integer.reverseBytes(bits);
    final int mask = (((reversedBits ^ Integer.MIN_VALUE) & Integer.MIN_VALUE) >> 8) | Integer.MIN_VALUE;
    return Float.intBitsToFloat(reversedBits ^ mask);
  }
}
