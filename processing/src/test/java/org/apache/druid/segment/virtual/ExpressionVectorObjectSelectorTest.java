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
import org.apache.druid.math.expr.vector.ExprEvalStringVector;
import org.apache.druid.math.expr.vector.ExprEvalVector;
import org.apache.druid.math.expr.vector.ExprVectorProcessor;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ExpressionVectorObjectSelectorTest
{
  private static final int MAX_SIZE = 8;
  private Expr.VectorInputBinding binding;
  private ExprVectorProcessor vectorProcessor;
  private ExpressionVectorObjectSelector expressionVectorValueSelector;

  @Before
  public void setUp()
  {
    binding = EasyMock.createMock(Expr.VectorInputBinding.class);
    vectorProcessor = EasyMock.createMock(ExprVectorProcessor.class);
    EasyMock.expect(binding.getMaxVectorSize()).andReturn(MAX_SIZE).once();
    EasyMock.replay(binding, vectorProcessor);
    expressionVectorValueSelector = new ExpressionVectorObjectSelector(vectorProcessor, binding);
    EasyMock.reset(binding, vectorProcessor);
  }

  @After
  public void tearDown()
  {
    EasyMock.verify(binding, vectorProcessor);
  }

  @Test
  public void testSelectObject()
  {
    final String[] vector = new String[]{"1", "2", null, "3"};
    ExprEvalVector vectorEval = new ExprEvalStringVector(vector);
    EasyMock.expect(binding.getCurrentVectorId()).andReturn(1).anyTimes();
    EasyMock.expect(vectorProcessor.evalVector(binding)).andReturn(vectorEval).once();
    EasyMock.replay(binding, vectorProcessor);

    Object[] vector1 = expressionVectorValueSelector.getObjectVector();
    Object[] vector2 = expressionVectorValueSelector.getObjectVector();

    Assert.assertArrayEquals(vector1, vector2);
  }
}
