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
import org.apache.druid.math.expr.ExpressionType;

public class IfLongVectorProcessor extends IfFunctionVectorProcessor<long[]>
{
  private final long[] output;
  private final boolean[] outputNulls;

  public IfLongVectorProcessor(
      ExprVectorProcessor<?> conditionProcessor,
      ExprVectorProcessor<long[]> thenProcessor,
      ExprVectorProcessor<long[]> elseProcessor
  )
  {
    super(
        ExpressionType.LONG,
        conditionProcessor,
        CastToTypeVectorProcessor.cast(thenProcessor, ExpressionType.LONG),
        CastToTypeVectorProcessor.cast(elseProcessor, ExpressionType.LONG)
    );
    this.output = new long[conditionProcessor.maxVectorSize()];
    this.outputNulls = new boolean[conditionProcessor.maxVectorSize()];
  }

  @Override
  public ExprEvalVector<long[]> evalVector(Expr.VectorInputBinding bindings)
  {
    thenBindingFilterer.setBindings(bindings);
    elseBindingFilterer.setBindings(bindings);
    final ExprEvalVector<?> conditionVector = conditionProcessor.evalVector(bindings);

    final int[] thenSelection = thenBindingFilterer.getVectorMatch().getSelection();
    final int[] elseSelection = elseBindingFilterer.getVectorMatch().getSelection();
    int thens = 0;
    int elses = 0;
    for (int i = 0; i < bindings.getCurrentVectorSize(); i++) {
      if (conditionVector.elementAsBoolean(i)) {
        thenSelection[thens++] = i;
      } else {
        elseSelection[elses++] = i;
      }
    }
    thenBindingFilterer.getVectorMatch().setSelectionSize(thens);
    elseBindingFilterer.getVectorMatch().setSelectionSize(elses);

    if (elses == 0) {
      return thenProcessor.evalVector(bindings);
    } else if (thens == 0) {
      return elseProcessor.evalVector(bindings);
    }


    final ExprEvalVector<long[]> thenVector = thenProcessor.evalVector(thenBindingFilterer);
    final ExprEvalVector<long[]> elseVector = elseProcessor.evalVector(elseBindingFilterer);
    final long[] thenValues =  thenVector.getLongVector();
    final boolean[] thenNulls = thenVector.getNullVector();
    final long[] elseValues = elseVector.getLongVector();
    final boolean[] elseNulls = elseVector.getNullVector();
    for (int i = 0; i < thens; i++) {
      final int outIndex = thenSelection[i];
      if (thenNulls != null && thenNulls[i]) {
        outputNulls[outIndex] = true;
      } else {
        output[outIndex] = thenValues[i];
        outputNulls[outIndex] = false;
      }
    }
    for (int i = 0; i < elses; i++) {
      final int outIndex = elseSelection[i];
      if (elseNulls != null && elseNulls[i]) {
        outputNulls[outIndex] = true;
      } else {
        output[outIndex] = elseValues[i];
        outputNulls[outIndex] = false;
      }
    }
    return new ExprEvalLongVector(output, outputNulls);
  }
}
