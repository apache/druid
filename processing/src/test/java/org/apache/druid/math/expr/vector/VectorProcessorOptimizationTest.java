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

import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionProcessing;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.math.expr.SettableVectorInputBinding;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class VectorProcessorOptimizationTest
{
  private static final int VECTOR_SIZE = 4;

  @BeforeEach
  public void setUp()
  {
    ExpressionProcessing.initializeForTests();
  }

  @AfterEach
  public void tearDown()
  {
    ExpressionProcessing.initializeForTests();
  }

  @Test
  public void testOrdinaryProcessorElidesEmptyNullVector()
  {
    final SettableVectorInputBinding bindings = new SettableVectorInputBinding(VECTOR_SIZE)
        .addLong("x", new long[]{1, 2, 3, 4}, new boolean[VECTOR_SIZE]);
    final ExprVectorProcessor<long[]> processor = processor("-((x + 1) * 2)", bindings);

    final ExprEvalVector<long[]> result = processor.evalVector(bindings);

    Assertions.assertArrayEquals(new long[]{-4, -6, -8, -10}, result.values());
    Assertions.assertNull(result.getNullVector());
  }

  @Test
  public void testOrdinaryProcessorPreservesActualNulls()
  {
    final SettableVectorInputBinding bindings = new SettableVectorInputBinding(VECTOR_SIZE)
        .addLong("x", new long[]{1, 0, 3, 4}, new boolean[]{false, true, false, false});
    final ExprVectorProcessor<long[]> processor = processor("-((x + 1) * 2)", bindings);

    final ExprEvalVector<long[]> result = processor.evalVector(bindings);

    Assertions.assertArrayEquals(new boolean[]{false, true, false, false}, result.getNullVector());
  }

  @Test
  public void testSimdProcessorElidesEmptyNullVector()
  {
    ExpressionProcessing.initializeForVectorApiTests();
    final SettableVectorInputBinding bindings = new SettableVectorInputBinding(VECTOR_SIZE)
        .addLong("x", new long[]{1, 2, 3, 4}, new boolean[VECTOR_SIZE]);
    final ExprVectorProcessor<long[]> processor = processor("(x + x) * x", bindings);

    final ExprEvalVector<long[]> result = processor.evalVector(bindings);

    Assertions.assertArrayEquals(new long[]{2, 8, 18, 32}, result.values());
    Assertions.assertNull(result.getNullVector());
  }

  @Test
  public void testNumericCastBuffersAreReused()
  {
    final SettableVectorInputBinding bindings = new SettableVectorInputBinding(VECTOR_SIZE)
        .addLong("l", new long[]{1, 2, 3, 4}, new boolean[VECTOR_SIZE])
        .addDouble("d", new double[]{1.2, 2.3, 3.4, 4.5}, new boolean[VECTOR_SIZE]);
    final CastToDoubleVectorProcessor toDouble = new CastToDoubleVectorProcessor(
        VectorProcessors.identifier(bindings, "l")
    );
    final CastToLongVectorProcessor toLong = new CastToLongVectorProcessor(
        VectorProcessors.identifier(bindings, "d")
    );

    final double[] firstDoubles = toDouble.evalVector(bindings).values();
    final double[] secondDoubles = toDouble.evalVector(bindings).values();
    final long[] firstLongs = toLong.evalVector(bindings).values();
    final long[] secondLongs = toLong.evalVector(bindings).values();

    Assertions.assertSame(firstDoubles, secondDoubles);
    Assertions.assertArrayEquals(new double[]{1, 2, 3, 4}, secondDoubles);
    Assertions.assertSame(firstLongs, secondLongs);
    Assertions.assertArrayEquals(new long[]{1, 2, 3, 4}, secondLongs);
  }

  @Test
  public void testConstantArithmeticUsesBoundConstantProcessor()
  {
    final SettableVectorInputBinding bindings = new SettableVectorInputBinding(VECTOR_SIZE)
        .addLong("x", new long[]{1, 2, 3, 4}, new boolean[VECTOR_SIZE])
        .addDouble("d", new double[]{1, 2, 3, 4}, new boolean[VECTOR_SIZE]);

    Assertions.assertInstanceOf(LongBivariateLongsConstantProcessor.class, processor("x + 7", bindings));
    Assertions.assertInstanceOf(LongBivariateLongsConstantProcessor.class, processor("7 - x", bindings));
    Assertions.assertInstanceOf(DoubleBivariateDoublesConstantProcessor.class, processor("d * 2.5", bindings));
  }

  private static <T> ExprVectorProcessor<T> processor(String expression, Expr.VectorInputBinding bindings)
  {
    return Parser.parse(expression, ExprMacroTable.nil()).asVectorProcessor(bindings);
  }
}
