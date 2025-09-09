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

public class IfObjectVectorProcessor extends IfFunctionVectorProcessor<Object[]>
{
  private final Object[] output;

  public IfObjectVectorProcessor(
      ExpressionType outputType,
      ExprVectorProcessor<?> conditionProcessor,
      ExprVectorProcessor<Object[]> thenProcessor,
      ExprVectorProcessor<Object[]> elseProcessor
  )
  {
    super(outputType, conditionProcessor, thenProcessor, elseProcessor);
    this.output = new Object[conditionProcessor.maxVectorSize()];
  }

  @Override
  public ExprEvalVector<Object[]> evalVector(Expr.VectorInputBinding bindings)
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

    final ExprEvalVector<Object[]> thenVector = thenProcessor.evalVector(thenBindingFilterer);
    final ExprEvalVector<Object[]> elseVector = elseProcessor.evalVector(elseBindingFilterer);
    final Object[] thenValues =  thenVector.getObjectVector();
    final Object[] elseValues =  elseVector.getObjectVector();
    for (int i = 0; i < thens; i++) {
      final int outIndex = thenSelection[i];
      output[outIndex] = thenValues[i];
    }
    for (int i = 0; i < elses; i++) {
      final int outIndex = elseSelection[i];
      output[outIndex] = elseValues[i];
    }
    return new ExprEvalObjectVector(output, outputType);
  }
}
