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

import org.apache.druid.math.expr.ExpressionType;

public final class CaseSearchedObjectVectorProcessor extends CaseSearchedFunctionVectorProcessor<Object[]>
{
  private final Object[] output;

  public CaseSearchedObjectVectorProcessor(
      ExpressionType outputType,
      ExprVectorProcessor<?>[] conditionProcessors,
      ExprVectorProcessor<Object[]>[] whenProcessors
  )
  {
    super(outputType, conditionProcessors, whenProcessors);
    this.output = new Object[conditionProcessors[0].maxVectorSize()];
  }

  @Override
  protected void processThenVector(ExprEvalVector<Object[]> thenVector, int currentMatches, int[] thenSelection)
  {
    final Object[] thenValues = thenVector.getObjectVector();
    for (int i = 0; i < currentMatches; i++) {
      final int outIndex = thenSelection[i];
      output[outIndex] = thenValues[i];
    }
  }

  @Override
  protected void processElseVector(ExprEvalVector<Object[]> elseVector, int[] conditionSelection)
  {
    final Object[] elseValues = elseVector.getObjectVector();
    for (int i = 0; i < conditionBindingFilterer.getCurrentVectorSize(); i++) {
      final int outIndex = conditionSelection[i];
      output[outIndex] = elseValues[i];
    }
  }

  @Override
  protected void processElseNull(int[] conditionSelection)
  {
    for (int i = 0; i < conditionBindingFilterer.getCurrentVectorSize(); i++) {
      final int outIndex = conditionSelection[i];
      output[outIndex] = null;
    }
  }

  @Override
  protected ExprEvalVector<Object[]> makeResultVector()
  {
    return new ExprEvalObjectVector(output, outputType);
  }
}
