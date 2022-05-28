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

package org.apache.druid.segment.virtual;

import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.vector.ExprEvalDoubleVector;
import org.apache.druid.math.expr.vector.ExprEvalLongVector;
import org.apache.druid.math.expr.vector.ExprEvalVector;
import org.apache.druid.math.expr.vector.ExprVectorProcessor;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ExpressionVectorValueSelectorTest
{
  private static final int MAX_SIZE = 8;
  private Expr.VectorInputBinding binding;
  private ExprVectorProcessor vectorProcessor;
  private ExpressionVectorValueSelector expressionVectorValueSelector;

  @Before
  public void setUp()
  {
    binding = EasyMock.createMock(Expr.VectorInputBinding.class);
    vectorProcessor = EasyMock.createMock(ExprVectorProcessor.class);
    EasyMock.expect(binding.getMaxVectorSize()).andReturn(MAX_SIZE).once();
    EasyMock.replay(binding, vectorProcessor);
    expressionVectorValueSelector = new ExpressionVectorValueSelector(vectorProcessor, binding);
    EasyMock.reset(binding, vectorProcessor);
  }

  @After
  public void tearDown()
  {
    EasyMock.verify(binding, vectorProcessor);
  }

  @Test
  public void testLongVector()
  {
    final long[] vector = new long[]{1L, 2L, 0L, 3L};
    final boolean[] nulls = new boolean[]{false, false, true, false};
    ExprEvalVector vectorEval = new ExprEvalLongVector(vector, nulls);
    EasyMock.expect(binding.getCurrentVectorId()).andReturn(1).anyTimes();
    EasyMock.expect(vectorProcessor.evalVector(binding)).andReturn(vectorEval).once();
    EasyMock.replay(binding, vectorProcessor);

    long[] vector1 = expressionVectorValueSelector.getLongVector();
    boolean[] bools1 = expressionVectorValueSelector.getNullVector();
    long[] vector2 = expressionVectorValueSelector.getLongVector();
    boolean[] bools2 = expressionVectorValueSelector.getNullVector();

    Assert.assertArrayEquals(vector1, vector2);
    Assert.assertArrayEquals(bools1, bools2);
  }

  @Test
  public void testDoubleVector()
  {
    final double[] vector = new double[]{1.0, 2.0, 0.0, 3.0};
    final boolean[] nulls = new boolean[]{false, false, true, false};
    ExprEvalVector vectorEval = new ExprEvalDoubleVector(vector, nulls);
    EasyMock.expect(binding.getCurrentVectorId()).andReturn(1).anyTimes();
    EasyMock.expect(vectorProcessor.evalVector(binding)).andReturn(vectorEval).once();
    EasyMock.replay(binding, vectorProcessor);

    double[] vector1 = expressionVectorValueSelector.getDoubleVector();
    boolean[] bools1 = expressionVectorValueSelector.getNullVector();
    double[] vector2 = expressionVectorValueSelector.getDoubleVector();
    boolean[] bools2 = expressionVectorValueSelector.getNullVector();

    Assert.assertArrayEquals(vector1, vector2, 0.0);
    Assert.assertArrayEquals(bools1, bools2);
  }

  @Test
  public void testFloatVector()
  {
    final double[] vector = new double[]{1.0, 2.0, 0.0, 3.0};
    final boolean[] nulls = new boolean[]{false, false, true, false};
    ExprEvalVector vectorEval = new ExprEvalDoubleVector(vector, nulls);
    EasyMock.expect(binding.getCurrentVectorId()).andReturn(1).anyTimes();
    EasyMock.expect(binding.getCurrentVectorSize()).andReturn(4).anyTimes();
    EasyMock.expect(vectorProcessor.evalVector(binding)).andReturn(vectorEval).once();
    EasyMock.replay(binding, vectorProcessor);

    float[] vector1 = expressionVectorValueSelector.getFloatVector();
    boolean[] bools1 = expressionVectorValueSelector.getNullVector();
    float[] vector2 = expressionVectorValueSelector.getFloatVector();
    boolean[] bools2 = expressionVectorValueSelector.getNullVector();

    for (int i = 0; i < vector1.length; i++) {
      Assert.assertEquals(vector1[i], vector2[i], 0.0);
    }
    Assert.assertArrayEquals(bools1, bools2);
  }
}
