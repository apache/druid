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
import org.apache.druid.query.filter.vector.VectorMatch;

public final class CoalesceVectorObjectProcessor extends CoalesceFunctionVectorProcessor<Object[]>
{
  private final Object[] output;

  public CoalesceVectorObjectProcessor(
      ExpressionType outputType,
      ExprVectorProcessor<Object[]>[] processors
  )
  {
    super(outputType, processors);
    this.output = new Object[processors[0].maxVectorSize()];
  }

  @Override
  public ExprEvalVector<Object[]> evalVector(Expr.VectorInputBinding bindings)
  {
    inputBindingFilterer.setBindings(bindings);
    inputBindingFilterer.getVectorMatch().copyFrom(VectorMatch.allTrue(bindings.getCurrentVectorSize()));
    final int[] selection = inputBindingFilterer.getVectorMatch().getSelection();

    int currentProcessor = 0;
    int notNull = 0;
    while (notNull < bindings.getCurrentVectorSize() && currentProcessor < processors.length) {
      final ExprEvalVector<Object[]> inputVector = processors[currentProcessor++].evalVector(inputBindingFilterer);
      final Object[] inputValues = inputVector.getObjectVector();
      int nulls = 0;
      for (int i = 0; i < inputBindingFilterer.getCurrentVectorSize(); i++) {
        final int outIndex = selection[i];
        if (inputValues[i] == null) {
          if (currentProcessor < processors.length) {
            selection[nulls++] = selection[i];
          } else {
            output[outIndex] = null;
          }
        } else {
          notNull++;
          output[outIndex] = inputValues[i];
        }
      }
      if (notNull == bindings.getCurrentVectorSize()) {
        break;
      }
      inputBindingFilterer.getVectorMatch().setSelectionSize(nulls);
    }
    return new ExprEvalObjectVector(output, outputType);
  }
}
