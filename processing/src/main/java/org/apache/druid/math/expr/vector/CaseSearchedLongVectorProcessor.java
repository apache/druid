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

public final class CaseSearchedLongVectorProcessor extends CaseSearchedFunctionVectorProcessor<long[]>
{
  private final long[] output;
  private final boolean[] outputNulls;

  public CaseSearchedLongVectorProcessor(
      ExprVectorProcessor<?>[] conditionProcessors,
      ExprVectorProcessor<long[]>[] thenProcessors
  )
  {
    super(ExpressionType.LONG, conditionProcessors, thenProcessors);
    output = new long[conditionProcessors[0].maxVectorSize()];
    outputNulls = new boolean[conditionProcessors[0].maxVectorSize()];
  }

  @Override
  protected void processThenVector(ExprEvalVector<long[]> thenVector, int currentMatches, int[] thenSelection)
  {
    final long[] thenValues = thenVector.getLongVector();
    final boolean[] thenNulls = thenVector.getNullVector();
    for (int i = 0; i < currentMatches; i++) {
      final int outIndex = thenSelection[i];
      output[outIndex] = thenValues[i];
      if (thenNulls != null) {
        outputNulls[outIndex] = thenNulls[i];
      } else {
        outputNulls[outIndex] = false;
      }
    }
  }

  @Override
  protected void processElseVector(ExprEvalVector<long[]> elseVector, int[] conditionSelection)
  {
    final long[] elseValues = elseVector.getLongVector();
    final boolean[] elseNulls = elseVector.getNullVector();
    for (int i = 0; i < conditionBindingFilterer.getCurrentVectorSize(); i++) {
      final int outIndex = conditionSelection[i];
      output[outIndex] = elseValues[i];
      if (elseNulls != null) {
        outputNulls[outIndex] = elseNulls[i];
      } else {
        outputNulls[outIndex] = false;
      }
    }
  }

  @Override
  protected void processElseNull(int[] conditionSelection)
  {
    for (int i = 0; i < conditionBindingFilterer.getCurrentVectorSize(); i++) {
      final int outIndex = conditionSelection[i];
      output[outIndex] = 0;
      outputNulls[outIndex] = true;
    }
  }

  @Override
  protected ExprEvalVector<long[]> makeResultVector()
  {
    return new ExprEvalLongVector(output, outputNulls);
  }
}
