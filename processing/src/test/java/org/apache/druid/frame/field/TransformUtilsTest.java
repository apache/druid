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

import com.google.common.collect.ImmutableList;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.frame.FrameType;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

@ParameterizedClass
@MethodSource("constructorFeeder")
public class TransformUtilsTest
{
  private final WritableMemory lhsMemory = WritableMemory.allocate(10);
  private final WritableMemory rhsMemory = WritableMemory.allocate(10);

  private static final long MEMORY_LOCATION = 0;

  @Parameter(0)
  public FrameType frameType;

  public static Stream<Object[]> constructorFeeder()
  {
    return List.of(
        new Object[]{FrameType.ROW_BASED_V1},
        new Object[]{FrameType.ROW_BASED_V2}
    ).stream();
  }

  /**
   * Returns the expected result of comparing two floats as bytes, using a given frame type.
   */
  public static int expectedComparison(final FrameType frameType, final float x, final float y)
  {
    switch (frameType) {
      case ROW_BASED_V1:
        return Long.compare(
            TransformUtils.detransformToLong(TransformUtils.transformFromFloat(x, frameType)),
            TransformUtils.detransformToLong(TransformUtils.transformFromFloat(y, frameType))
        );

      default:
        return Float.isNaN(x) && Float.isNaN(y) ? 0 : Float.compare(x, y);
    }
  }

  /**
   * Returns the expected result of comparing two doubles as bytes, using a given frame type.
   */
  public static int expectedComparison(final FrameType frameType, final double x, final double y)
  {
    switch (frameType) {
      case ROW_BASED_V1:
        return Long.compare(
            TransformUtils.detransformToLong(TransformUtils.transformFromDouble(x, frameType)),
            TransformUtils.detransformToLong(TransformUtils.transformFromDouble(y, frameType))
        );

      default:
        return Double.isNaN(x) && Double.isNaN(y) ? 0 : Double.compare(x, y);
    }
  }

  @Test
  public void doubleTest()
  {
    List<Double> values =
        ImmutableList.of(
            Double.NaN,
            //CHECKSTYLE.OFF: Regexp
            Double.MAX_VALUE,
            Double.MIN_VALUE,
            //CHECKSTYLE.ON: Regexp
            Double.POSITIVE_INFINITY,
            Double.NEGATIVE_INFINITY,
            Double.MIN_NORMAL,
            0.0d,
            1.234234d,
            -1.344234d,
            129123.123123,
            -21312213.33,
            1111.0,
            23.0,
            -0.000007692,
            -0.07692,
            -0.1410658,
            -0.183808,
            -0.3311,
            -0.4615,
            0.000007692,
            0.07692,
            0.1410658,
            0.183808,
            0.3311,
            0.4615
        );

    for (double value : values) {
      Assertions.assertEquals(
          value,
          TransformUtils.detransformToDouble(TransformUtils.transformFromDouble(value, frameType), frameType),
          0.0,
          String.valueOf(value)
      );

    }

    for (int lhsIndex = 0; lhsIndex < values.size(); ++lhsIndex) {
      for (int rhsIndex = lhsIndex; rhsIndex < values.size(); ++rhsIndex) {
        double lhs = values.get(lhsIndex);
        double rhs = values.get(rhsIndex);
        lhsMemory.putLong(MEMORY_LOCATION, TransformUtils.transformFromDouble(lhs, frameType));
        rhsMemory.putLong(MEMORY_LOCATION, TransformUtils.transformFromDouble(rhs, frameType));
        int byteCmp = byteComparison(Double.BYTES);
        final int expectedCmp = expectedComparison(frameType, lhs, rhs);
        Assertions.assertEquals(signum(expectedCmp), signum(byteCmp), StringUtils.format("compare(%s, %s)", lhs, rhs));
      }
    }
  }

  @Test
  public void longTest()
  {
    List<Long> values =
        ImmutableList.of(
            Long.MIN_VALUE,
            Long.MAX_VALUE,
            0L,
            123L,
            -123L
        );

    for (long value : values) {
      Assertions.assertEquals(
          value,
          TransformUtils.detransformToLong(TransformUtils.transformFromLong(value))
      );

    }

    for (int lhsIndex = 0; lhsIndex < values.size(); ++lhsIndex) {
      for (int rhsIndex = lhsIndex; rhsIndex < values.size(); ++rhsIndex) {
        long lhs = values.get(lhsIndex);
        long rhs = values.get(rhsIndex);
        lhsMemory.putLong(MEMORY_LOCATION, TransformUtils.transformFromLong(lhs));
        rhsMemory.putLong(MEMORY_LOCATION, TransformUtils.transformFromLong(rhs));
        int byteCmp = byteComparison(Long.BYTES);

        if (byteCmp < 0) {
          Assertions.assertTrue(lhs < rhs);
        } else if (byteCmp == 0) {
          Assertions.assertEquals(lhs, rhs);
        } else {
          Assertions.assertTrue(lhs > rhs);
        }
      }
    }
  }

  @Test
  public void floatTest()
  {
    List<Float> values =
        ImmutableList.of(
            Float.NaN,
            //CHECKSTYLE.OFF: Regexp
            Float.MIN_VALUE,
            Float.MAX_VALUE,
            //CHECKSTYLE.ON: Regexp
            Float.MIN_NORMAL,
            Float.POSITIVE_INFINITY,
            Float.NEGATIVE_INFINITY,
            0.0f,
            1.234234f,
            -1.344234f,
            129123.123123f,
            -21312213.33f,
            1111.0f,
            23.0f,
            -0.000007692f,
            -0.07692f,
            -0.1410658f,
            -0.183808f,
            -0.3311f,
            -0.4615f,
            0.000007692f,
            0.07692f,
            0.1410658f,
            0.183808f,
            0.3311f,
            0.4615f
        );

    for (float value : values) {
      Assertions.assertEquals(
          value,
          TransformUtils.detransformToFloat(TransformUtils.transformFromFloat(value, frameType), frameType),
          0.0f,
          String.valueOf(value)
      );
    }

    for (int lhsIndex = 0; lhsIndex < values.size(); ++lhsIndex) {
      for (int rhsIndex = lhsIndex; rhsIndex < values.size(); ++rhsIndex) {
        float lhs = values.get(lhsIndex);
        float rhs = values.get(rhsIndex);
        lhsMemory.putLong(MEMORY_LOCATION, TransformUtils.transformFromFloat(lhs, frameType));
        rhsMemory.putLong(MEMORY_LOCATION, TransformUtils.transformFromFloat(rhs, frameType));
        final int byteCmp = byteComparison(Long.BYTES);
        final int expectedCmp = expectedComparison(frameType, lhs, rhs);
        Assertions.assertEquals(signum(expectedCmp), signum(byteCmp), StringUtils.format("compare(%s, %s)", lhs, rhs));
      }
    }
  }

  private int byteComparison(int numBytes)
  {
    for (int i = 0; i < numBytes; ++i) {
      byte lhsByte = lhsMemory.getByte(MEMORY_LOCATION + i);
      byte rhsByte = rhsMemory.getByte(MEMORY_LOCATION + i);
      final int cmp = (lhsByte & 0xFF) - (rhsByte & 0xFF);
      if (cmp != 0) {
        return cmp;
      }
    }
    return 0;
  }

  private int signum(int n)
  {
    if (n < 0) {
      return -1;
    } else if (n > 0) {
      return 1;
    } else {
      return 0;
    }
  }
}
