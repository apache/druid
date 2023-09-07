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

/**
 * Utility methods to map the primitive numeric types into an equi-wide byte representation, such that the
 * given byte sequence preserves the ordering of the original type when done byte comparison.
 * Checkout {@link org.apache.druid.frame.read.FrameReaderUtils#compareMemoryToByteArrayUnsigned} for how this byte
 * comparison is performed.
 */
public class TransformUtils
{
  /**
   * Transforms a double into a form where it can be compared as unsigned bytes without decoding.
   */
  public static long transformFromDouble(final double n)
  {
    final long bits = Double.doubleToLongBits(n);
    final long mask = ((bits & Long.MIN_VALUE) >> 11) | Long.MIN_VALUE;
    return Long.reverseBytes(bits ^ mask);
  }

  /**
   * Inverse of {@link #transformFromDouble}.
   */
  public static double detransformToDouble(final long bits)
  {
    final long reversedBits = Long.reverseBytes(bits);
    final long mask = (((reversedBits ^ Long.MIN_VALUE) & Long.MIN_VALUE) >> 11) | Long.MIN_VALUE;
    return Double.longBitsToDouble(reversedBits ^ mask);
  }

  /**
   * Transforms a long into a form where it can be compared as unsigned bytes without decoding.
   */
  public static long transformFromLong(final long n)
  {
    // Must flip the first (sign) bit so comparison-as-bytes works.
    return Long.reverseBytes(n ^ Long.MIN_VALUE);
  }

  /**
   * Reverse the {@link #transformFromLong(long)} function.
   */
  public static long detransformToLong(final long bits)
  {
    return Long.reverseBytes(bits) ^ Long.MIN_VALUE;
  }

  /**
   * Transforms a float into a form where it can be compared as unsigned bytes without decoding.
   */
  public static int transformFromFloat(final float n)
  {
    final int bits = Float.floatToIntBits(n);
    final int mask = ((bits & Integer.MIN_VALUE) >> 8) | Integer.MIN_VALUE;
    return Integer.reverseBytes(bits ^ mask);
  }

  /**
   * Inverse of {@link #transformFromFloat(float)}.
   */
  public static float detransformToFloat(final int bits)
  {
    final int reversedBits = Integer.reverseBytes(bits);
    final int mask = (((reversedBits ^ Integer.MIN_VALUE) & Integer.MIN_VALUE) >> 8) | Integer.MIN_VALUE;
    return Float.intBitsToFloat(reversedBits ^ mask);
  }
}
