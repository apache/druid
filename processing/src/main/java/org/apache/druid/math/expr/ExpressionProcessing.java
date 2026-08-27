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

package org.apache.druid.math.expr;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;

/**
 * Expressions processing configs
 */
public class ExpressionProcessing
{
  /**
   * INSTANCE is injected using static injection to avoid adding JacksonInject annotations all over the code.
   * @see {@link ExpressionProcessingModule} for details.
   *
   * It does not take effect in all unit tests since we don't use Guice Injection. Use {@link #initializeForTests}
   * when modules are not available.
   */
  @Inject
  private static ExpressionProcessingConfig INSTANCE;


  /**
   * Many unit tests do not setup modules for this value to be injected, this method provides a manual way to initialize
   * {@link #INSTANCE}
   */
  @VisibleForTesting
  public static void initializeForTests()
  {
    INSTANCE = new ExpressionProcessingConfig(null, null, null, null, null);
  }

  @VisibleForTesting
  public static void initializeForHomogenizeNullMultiValueStrings()
  {
    INSTANCE = new ExpressionProcessingConfig(null, true, null, null, null);
  }

  @VisibleForTesting
  public static void initializeForVectorApiTests()
  {
    INSTANCE = new ExpressionProcessingConfig(null, null, null, true, true);
  }

  /**
   * All {@link ExprType#ARRAY} values will be converted to {@link ExpressionType#STRING} by their column selectors
   * (not within expression processing) to be treated as multi-value strings instead of native arrays.
   */
  public static boolean processArraysAsMultiValueStrings()
  {
    checkInitialized();
    return INSTANCE.processArraysAsMultiValueStrings();
  }

  /**
   * All multi-value string expression input values of 'null', '[]', and '[null]' will be coerced to '[null]'. If false,
   * (the default) this will only be done when single value expressions are implicitly mapped across multi-value rows,
   * so that the single valued expression will always be evaluated with an input value of 'null'
   */
  public static boolean isHomogenizeNullMultiValueStringArrays()
  {
    checkInitialized();
    return INSTANCE.isHomogenizeNullMultiValueStringArrays();
  }

  public static boolean allowVectorizeFallback()
  {
    checkInitialized();
    return INSTANCE.allowVectorizeFallback();
  }

  /**
   * Whether {@link org.apache.druid.math.expr.vector.ExprVectorProcessor} implementations may dispatch to specialized
   * {@code jdk.incubator.vector} (SIMD) variants for supported math operations. Off by default; opt-in via
   * {@link ExpressionProcessingConfig#USE_VECTOR_API}. Requires the JVM to be started with
   * {@code --add-modules=jdk.incubator.vector}, which Druid already adds to its standard launch arguments.
   */
  public static boolean useVectorApi()
  {
    checkInitialized();
    return INSTANCE.useVectorApi();
  }

  /**
   * Whether SIMD dispatch is allowed for math ops backed by the JDK's VO_MATHLIB path (LOG, EXP, SIN, etc). On by
   * default whenever {@link #useVectorApi()} is on; can be turned off independently via
   * {@link ExpressionProcessingConfig#USE_VECTOR_MATH_API}=false. Has no effect unless {@link #useVectorApi()} is
   * also on.
   *
   * <p>These ops route through Intel SVML / Arm SLEEF once the JIT compiles the vector loop to C2; before that
   * compilation, they fall back to per-lane {@link Math} calls. The two paths can differ by a few ulps (bounded
   * at 2 ulps by {@code SimdVoMathlibParityTest.MAX_ULPS}), so a long-running query can produce different bits
   * for the same input across the C1→C2 tier transition. Bit-for-bit equality of floating-point results is
   * fragile in general, but this flag is an escape hatch for legacy queries relying on the scalar {@link Math} bits
   * directly.
   */
  public static boolean useVectorMathApi()
  {
    checkInitialized();
    return INSTANCE.useVectorMathApi();
  }

  private static void checkInitialized()
  {
    // this should only be null in a unit test context, in production this will be injected by the null handling module
    if (INSTANCE == null) {
      throw new IllegalStateException(
          "ExpressionProcessing module not initialized, call ExpressionProcessing.initializeForTests() or one of its variants"
      );
    }
  }
}
