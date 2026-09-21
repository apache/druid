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

import org.apache.druid.math.expr.vector.ExprEvalVector;
import org.apache.druid.math.expr.vector.ExprVectorProcessor;
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
  public void testSimdProcessorElidesEmptyNullVector()
  {
    final SettableVectorInputBinding bindings = new SettableVectorInputBinding(4)
        .addLong("x", new long[]{1, 2, 3, 4}, new boolean[4]);
    // Addition and multiplication have SIMD implementations. Supplying a non-null, all-false null vector verifies
    // that the SIMD processor recognizes a batch without null rows and exposes a null null-vector to its consumer.
    final ExprVectorProcessor<long[]> processor = Parser.parse("(x + x) * x", ExprMacroTable.nil())
                                                        .asVectorProcessor(bindings);

    final ExprEvalVector<long[]> result = processor.evalVector(bindings);

    Assertions.assertArrayEquals(new long[]{2, 8, 18, 32}, result.values());
    Assertions.assertNull(result.getNullVector());
  }
}
