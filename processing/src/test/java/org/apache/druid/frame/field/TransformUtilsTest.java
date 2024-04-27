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
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class TransformUtilsTest
{

  private final WritableMemory lhsMemory = WritableMemory.allocate(10);
  private final WritableMemory rhsMemory = WritableMemory.allocate(10);

  private static final long MEMORY_LOCATION = 0;

  @Test
  public void doubleTestWithoutNaN()
  {
    //CHECKSTYLE.OFF: Regexp
    List<Double> values =
        ImmutableList.of(
            Double.MAX_VALUE,
            Double.MIN_VALUE,
            Double.POSITIVE_INFINITY,
            Double.NEGATIVE_INFINITY,
            Double.MIN_NORMAL,
            0.0d,
            1.234234d,
            -1.344234d,
            129123.123123,
            -21312213.33,
            1111.0,
            23.0
        );
    //CHECKSTYLE.ON: Regexp

    for (double value : values) {
      Assert.assertEquals(
          value,
          TransformUtils.detransformToDouble(TransformUtils.transformFromDouble(value)),
          0.0
      );

    }

    for (int lhsIndex = 0; lhsIndex < values.size(); ++lhsIndex) {
      for (int rhsIndex = lhsIndex; rhsIndex < values.size(); ++rhsIndex) {
        double lhs = values.get(lhsIndex);
        double rhs = values.get(rhsIndex);
        lhsMemory.putLong(MEMORY_LOCATION, TransformUtils.transformFromDouble(lhs));
        rhsMemory.putLong(MEMORY_LOCATION, TransformUtils.transformFromDouble(rhs));
        int byteCmp = byteComparison(Double.BYTES);

        if (byteCmp < 0) {
          Assert.assertTrue(lhs < rhs);
        } else if (byteCmp == 0) {
          Assert.assertEquals(lhs, rhs, 0.0);
        } else {
          Assert.assertTrue(lhs > rhs);
        }

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
      Assert.assertEquals(
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
          Assert.assertTrue(lhs < rhs);
        } else if (byteCmp == 0) {
          Assert.assertEquals(lhs, rhs);
        } else {
          Assert.assertTrue(lhs > rhs);
        }
      }
    }
  }

  @Test
  public void floatTestWithoutNaN()
  {
    //CHECKSTYLE.OFF: Regexp
    List<Float> values =
        ImmutableList.of(
            Float.MIN_VALUE,
            Float.MAX_VALUE,
            Float.MIN_NORMAL,
            Float.POSITIVE_INFINITY,
            Float.NEGATIVE_INFINITY,
            0.0f,
            1.234234f,
            -1.344234f,
            129123.123123f,
            -21312213.33f,
            1111.0f,
            23.0f
        );
    //CHECKSTYLE.ON: Regexp

    for (float value : values) {
      Assert.assertEquals(
          value,
          TransformUtils.detransformToFloat(TransformUtils.transformFromFloat(value)),
          0.0
      );

    }

    for (int lhsIndex = 0; lhsIndex < values.size(); ++lhsIndex) {
      for (int rhsIndex = lhsIndex; rhsIndex < values.size(); ++rhsIndex) {
        float lhs = values.get(lhsIndex);
        float rhs = values.get(rhsIndex);
        lhsMemory.putLong(MEMORY_LOCATION, TransformUtils.transformFromFloat(lhs));
        rhsMemory.putLong(MEMORY_LOCATION, TransformUtils.transformFromFloat(rhs));
        int byteCmp = byteComparison(Long.BYTES);

        if (byteCmp < 0) {
          Assert.assertTrue(lhs < rhs);
        } else if (byteCmp == 0) {
          Assert.assertEquals(lhs, rhs, 0.0);
        } else {
          Assert.assertTrue(lhs > rhs);
        }
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
}
