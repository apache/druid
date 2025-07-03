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

import org.apache.druid.error.DruidException;
import org.apache.druid.frame.FrameType;

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
   *
   * @param n         the number
   * @param frameType the output frame type; required because the floating point serialization format changed
   *                  from {@link FrameType#ROW_BASED_V1} to {@link FrameType#ROW_BASED_V2}.
   */
  public static long transformFromDouble(final double n, final FrameType frameType)
  {
    switch (frameType) {
      case ROW_BASED_V1:
        return transformFromDoubleLegacy(n);
      case ROW_BASED_V2:
        return transformFromDouble(n);
      default:
        throw DruidException.defensive("Unexpected frameType[%s]", frameType);
    }
  }

  /**
   * Transforms a double into a form where it can be compared as unsigned bytes without decoding.
   */
  private static long transformFromDouble(final double n)
  {
    final long bits = Double.doubleToLongBits(n);
    final long mask = ((bits & Long.MIN_VALUE) >> 63) | Long.MIN_VALUE;
    return Long.reverseBytes(bits ^ mask);
  }

  /**
   * Transforms a double into a form where it can be compared as unsigned bytes (incorrectly!) without decoding.
   * This is a legacy format, because the conversion is incorrect: round-tripping works fine, but comparisons are
   * not correct for certain negative numbers.
   */
  private static long transformFromDoubleLegacy(final double n)
  {
    final long bits = Double.doubleToLongBits(n);
    final long mask = ((bits & Long.MIN_VALUE) >> 11) | Long.MIN_VALUE;
    return Long.reverseBytes(bits ^ mask);
  }

  /**
   * Inverse of {@link #transformFromDouble(double, FrameType)}.
   */
  public static double detransformToDouble(final long transformedBits, final FrameType frameType)
  {
    switch (frameType) {
      case ROW_BASED_V1:
        return detransformToDoubleLegacy(transformedBits);
      case ROW_BASED_V2:
        return detransformToDouble(transformedBits);
      default:
        throw DruidException.defensive("Unexpected frameType[%s]", frameType);
    }
  }

  /**
   * Inverse of {@link #transformFromDouble(double)}.
   */
  private static double detransformToDouble(final long transformedBits)
  {
    final long reversedBits = Long.reverseBytes(transformedBits);
    final long mask = (((reversedBits ^ Long.MIN_VALUE) & Long.MIN_VALUE) >> 63) | Long.MIN_VALUE;
    return Double.longBitsToDouble(reversedBits ^ mask);
  }

  /**
   * Inverse of {@link #transformFromDoubleLegacy(double)}.
   */
  private static double detransformToDoubleLegacy(final long bits)
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
   *
   * @param n         the number
   * @param frameType the output frame type; required because the floating point serialization format changed
   *                  from {@link FrameType#ROW_BASED_V1} to {@link FrameType#ROW_BASED_V2}.
   */
  public static int transformFromFloat(final float n, final FrameType frameType)
  {
    switch (frameType) {
      case ROW_BASED_V1:
        return transformFromFloatLegacy(n);
      case ROW_BASED_V2:
        return transformFromFloat(n);
      default:
        throw DruidException.defensive("Unexpected frameType[%s]", frameType);
    }
  }

  /**
   * Transforms a float into a form where it can be compared as unsigned bytes without decoding.
   */
  private static int transformFromFloat(final float n)
  {
    final int bits = Float.floatToIntBits(n);
    final int mask = ((bits & Integer.MIN_VALUE) >> 31) | Integer.MIN_VALUE;
    return Integer.reverseBytes(bits ^ mask);
  }

  /**
   * Transforms a float into a form where it can be compared as unsigned bytes (incorrectly!) without decoding.
   * This is a legacy format, because the conversion is incorrect: round-tripping works fine, but comparisons are
   * not correct for negative numbers that have the same exponent.
   */
  private static int transformFromFloatLegacy(final float n)
  {
    final int bits = Float.floatToIntBits(n);
    final int mask = ((bits & Integer.MIN_VALUE) >> 8) | Integer.MIN_VALUE;
    return Integer.reverseBytes(bits ^ mask);
  }

  /**
   * Inverse of {@link #transformFromFloat(float, FrameType)}.
   */
  public static float detransformToFloat(final int transformedBits, final FrameType frameType)
  {
    switch (frameType) {
      case ROW_BASED_V1:
        return detransformToFloatLegacy(transformedBits);
      case ROW_BASED_V2:
        return detransformToFloat(transformedBits);
      default:
        throw DruidException.defensive("Unexpected frameType[%s]", frameType);
    }
  }

  /**
   * Inverse of {@link #transformFromFloat(float)}.
   */
  private static float detransformToFloat(final int transformedBits)
  {
    final int reversedBits = Integer.reverseBytes(transformedBits);
    final int mask = (((reversedBits ^ Integer.MIN_VALUE) & Integer.MIN_VALUE) >> 31) | Integer.MIN_VALUE;
    return Float.intBitsToFloat(reversedBits ^ mask);
  }

  /**
   * Inverse of {@link #transformFromFloatLegacy(float)}.
   */
  private static float detransformToFloatLegacy(final int bits)
  {
    final int reversedBits = Integer.reverseBytes(bits);
    final int mask = (((reversedBits ^ Integer.MIN_VALUE) & Integer.MIN_VALUE) >> 8) | Integer.MIN_VALUE;
    return Float.intBitsToFloat(reversedBits ^ mask);
  }
}
