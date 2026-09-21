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

import org.apache.druid.math.expr.vector.ExprVectorProcessor;
import org.apache.druid.math.expr.vector.simd.SimdDoubleDoubleAddProcessor;
import org.apache.druid.math.expr.vector.simd.SimdDoubleLongAddProcessor;
import org.apache.druid.math.expr.vector.simd.SimdDoubleNegProcessor;
import org.apache.druid.math.expr.vector.simd.SimdLongDoubleAddProcessor;
import org.apache.druid.math.expr.vector.simd.SimdLongLongAddProcessor;
import org.apache.druid.math.expr.vector.simd.SimdLongNegProcessor;
import org.apache.druid.math.expr.vector.simd.SimdLongToDoubleSqrtProcessor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Re-runs every {@link VectorExprResultConsistencyTest} case with the SIMD ({@code jdk.incubator.vector}) expression
 * vector processors enabled, ensuring the SIMD specializations agree with the non-vectorized reference.
 */
public class VectorExprResultConsistencyVectorApiTest extends VectorExprResultConsistencyTest
{
  @BeforeEach
  public void enableVectorApi()
  {
    ExpressionProcessing.initializeForVectorApiTests();
  }

  @AfterEach
  public void resetExpressionProcessing()
  {
    ExpressionProcessing.initializeForTests();
  }

  @Test
  public void testSimdProcessorFamiliesElideEmptyNullVector()
  {
    final SettableVectorInputBinding bindings = new SettableVectorInputBinding(4)
        .addLong("l1", new long[]{1, 2, 3, 4}, new boolean[4])
        .addLong("l2", new long[]{2, 3, 4, 5}, new boolean[4])
        .addDouble("d1", new double[]{1, 2, 3, 4}, new boolean[4])
        .addDouble("d2", new double[]{2, 3, 4, 5}, new boolean[4]);

    // Supplying non-null, all-false null vectors verifies that every SIMD processor base recognizes a batch without
    // null rows and exposes a null null-vector to its consumer.
    assertSimdProcessorElidesEmptyNullVector(bindings, "l1 + l2", SimdLongLongAddProcessor.class);
    assertSimdProcessorElidesEmptyNullVector(bindings, "d1 + d2", SimdDoubleDoubleAddProcessor.class);
    assertSimdProcessorElidesEmptyNullVector(bindings, "l1 + d1", SimdLongDoubleAddProcessor.class);
    assertSimdProcessorElidesEmptyNullVector(bindings, "d1 + l1", SimdDoubleLongAddProcessor.class);
    assertSimdProcessorElidesEmptyNullVector(bindings, "-l1", SimdLongNegProcessor.class);
    assertSimdProcessorElidesEmptyNullVector(bindings, "-d1", SimdDoubleNegProcessor.class);
    assertSimdProcessorElidesEmptyNullVector(bindings, "sqrt(l1)", SimdLongToDoubleSqrtProcessor.class);
  }

  private static void assertSimdProcessorElidesEmptyNullVector(
      Expr.VectorInputBinding bindings,
      String expression,
      Class<? extends ExprVectorProcessor<?>> processorClass
  )
  {
    final ExprVectorProcessor<?> processor = Parser.parse(expression, ExprMacroTable.nil())
                                                   .asVectorProcessor(bindings);
    Assertions.assertInstanceOf(processorClass, processor);
    Assertions.assertNull(processor.evalVector(bindings).getNullVector());
  }
}
