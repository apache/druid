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

public final class NvlVectorObjectProcessor extends NvlFunctionVectorProcessor<Object[]>
{
  private final Object[] output;

  public NvlVectorObjectProcessor(
      ExpressionType outputType,
      ExprVectorProcessor<Object[]> inputProcessor,
      ExprVectorProcessor<Object[]> elseProcessor
  )
  {
    super(outputType, inputProcessor, elseProcessor);
    this.output = new Object[inputProcessor.maxVectorSize()];
  }

  @Override
  public ExprEvalVector<Object[]> evalVector(Expr.VectorInputBinding bindings)
  {
    inputBindingFilterer.setBindings(bindings);
    final ExprEvalVector<Object[]> inputVector = inputProcessor.evalVector(bindings);

    Object[] inputValues = inputVector.getObjectVector();
    final int[] selection = inputBindingFilterer.getVectorMatch().getSelection();
    int nulls = 0;
    for (int i = 0; i < bindings.getCurrentVectorSize(); i++) {
      if (inputValues[i] == null) {
        selection[nulls++] = i;
      } else {
        output[i] = inputValues[i];
      }
    }
    inputBindingFilterer.getVectorMatch().setSelectionSize(nulls);
    final ExprEvalVector<Object[]> elseVector = elseProcessor.evalVector(inputBindingFilterer);
    final Object[] elseValues = elseVector.getObjectVector();
    for (int i = 0; i < nulls; i++) {
      output[selection[i]] = elseValues[i];
    }
    return new ExprEvalObjectVector(output, outputType);
  }
}
