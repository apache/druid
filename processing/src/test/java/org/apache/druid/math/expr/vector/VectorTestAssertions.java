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

package org.apache.druid.math.expr.vector;

import org.apache.druid.java.util.common.StringUtils;
import org.junit.jupiter.api.Assertions;

/**
 * Shared assertions for tests that compare vectorized (SIMD or otherwise) expression outputs against a
 * scalar reference and need to tolerate the small numeric differences the SIMD path can introduce for
 * VO_MATHLIB-backed transcendentals — see {@code SimdVoMathlibParityTest} and the {@code useVectorMathApi}
 * flag javadoc for context.
 */
public final class VectorTestAssertions
{
  private VectorTestAssertions()
  {
    // no instantiation
  }

  /**
   * Assert that two doubles agree to within {@code maxUlps} ulps of each other. NaN is treated as equal to
   * NaN and bit-identical values pass trivially. On failure, the message identifies the expected/actual bits
   * and the measured ulp delta.
   */
  public static void assertDoublesEquivalent(String message, double expected, double actual, int maxUlps)
  {
    if (Double.isNaN(expected) && Double.isNaN(actual)) {
      return;
    }
    if (Double.doubleToRawLongBits(expected) == Double.doubleToRawLongBits(actual)) {
      return;
    }
    final double ulp = Math.ulp(expected);
    final double diff = Math.abs(actual - expected);
    if (Double.isNaN(diff) || diff > (double) maxUlps * ulp) {
      Assertions.fail(StringUtils.format(
          "%s: %s differs from expected %s by %s ulps (>%d allowed)",
          message == null ? "double parity" : message,
          actual,
          expected,
          diff / ulp,
          maxUlps
      ));
    }
  }
}
