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

public final class NvlDoubleVectorProcessor extends NvlFunctionVectorProcessor<double[]>
{
  private final double[] output;
  private final boolean[] outputNulls;

  public NvlDoubleVectorProcessor(
      ExprVectorProcessor<double[]> inputProcessor,
      ExprVectorProcessor<double[]> elseProcessor
  )
  {
    super(ExpressionType.DOUBLE, inputProcessor, elseProcessor);
    this.output = new double[inputProcessor.maxVectorSize()];
    this.outputNulls = new boolean[inputProcessor.maxVectorSize()];
  }

  @Override
  public ExprEvalVector<double[]> evalVector(Expr.VectorInputBinding bindings)
  {
    inputBindingFilterer.setBindings(bindings);
    final ExprEvalVector<double[]> inputVector = inputProcessor.evalVector(bindings);

    if (inputVector.getNullVector() == null) {
      // this one has no nulls, just spit it out
      return inputVector;
    }


    final double[] inputValues = inputVector.getDoubleVector();
    final boolean[] inputNulls = inputVector.getNullVector();
    final int[] selection = inputBindingFilterer.getVectorMatch().getSelection();
    int nulls = 0;
    for (int i = 0; i < bindings.getCurrentVectorSize(); i++) {
      if (inputNulls[i]) {
        selection[nulls++] = i;
      } else {
        outputNulls[i] = false;
        output[i] = inputValues[i];
      }
    }
    if (nulls == 0) {
      return new ExprEvalDoubleVector(output, outputNulls);
    }
    inputBindingFilterer.getVectorMatch().setSelectionSize(nulls);

    if (nulls == bindings.getCurrentVectorSize()) {
      // all nulls, just return the other
      return elseProcessor.evalVector(bindings);
    }

    final ExprEvalVector<double[]> elseVector = elseProcessor.evalVector(inputBindingFilterer);
    final double[] elseValues = elseVector.getDoubleVector();
    final boolean[] elseNulls = elseVector.getNullVector();
    for (int i = 0; i < nulls; i++) {
      final int outIndex = selection[i];
      if (elseNulls != null && elseNulls[i]) {
        outputNulls[outIndex] = true;
      } else {
        output[outIndex] = elseValues[i];
        outputNulls[outIndex] = false;
      }
    }
    return new ExprEvalDoubleVector(output, outputNulls);
  }
}
