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

package org.apache.druid.math.expr.vector.simd;

import javax.annotation.Nullable;

import java.util.Arrays;

/**
 * Helper for binary SIMD processors that compute output nulls as the OR of both inputs' null vectors. Used by the
 * VO_MATHLIB binary processors (POW, ATAN2, HYPOT), whose main hot loop always runs the SIMD arithmetic, and defers
 * the null OR to this helper.
 */
final class SimdBinaryNulls
{
  private SimdBinaryNulls()
  {
  }

  static void combineInto(
      boolean[] outNulls,
      @Nullable boolean[] leftNulls,
      @Nullable boolean[] rightNulls,
      int currentSize
  )
  {
    if (leftNulls == null && rightNulls == null) {
      Arrays.fill(outNulls, 0, currentSize, false);
    } else if (leftNulls != null && rightNulls != null) {
      for (int i = 0; i < currentSize; i++) {
        outNulls[i] = leftNulls[i] || rightNulls[i];
      }
    } else if (leftNulls != null) {
      System.arraycopy(leftNulls, 0, outNulls, 0, currentSize);
    } else {
      System.arraycopy(rightNulls, 0, outNulls, 0, currentSize);
    }
  }
}
