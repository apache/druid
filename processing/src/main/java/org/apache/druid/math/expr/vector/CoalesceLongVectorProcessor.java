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

public final class CoalesceLongVectorProcessor extends CoalesceFunctionVectorProcessor<long[]>
{
  private final long[] output;
  private final boolean[] outputNulls;

  public CoalesceLongVectorProcessor(
      ExprVectorProcessor<long[]>[] processors
  )
  {
    super(ExpressionType.LONG, processors);
    this.output = new long[processors[0].maxVectorSize()];
    this.outputNulls = new boolean[processors[0].maxVectorSize()];
  }

  @Override
  public ExprEvalVector<long[]> evalVector(Expr.VectorInputBinding bindings)
  {
    inputBindingFilterer.setBindings(bindings);
    inputBindingFilterer.getVectorMatch().copyFrom(VectorMatch.allTrue(bindings.getCurrentVectorSize()));
    final int[] selection = inputBindingFilterer.getVectorMatch().getSelection();

    ExprEvalVector<long[]> currentVector;
    int currentProcessor = 0;
    int notNull = 0;
    while (notNull < bindings.getCurrentVectorSize() && currentProcessor < processors.length) {
      currentVector = processors[currentProcessor].evalVector(inputBindingFilterer);
      final long[] currentValues = currentVector.getLongVector();
      final boolean[] currentNulls = currentVector.getNullVector();
      if (currentProcessor == 0 && currentNulls == null) {
        // this one has no nulls, bail early
        return currentVector;
      }
      currentProcessor++;
      int nulls = 0;
      for (int i = 0; i < inputBindingFilterer.getCurrentVectorSize(); i++) {
        final int outIndex = selection[i];
        if (currentNulls != null && currentNulls[i]) {
          if (currentProcessor < processors.length) {
            selection[nulls++] = selection[i];
          } else {
            outputNulls[outIndex] = true;
            output[outIndex] = 0;
          }
        } else {
          notNull++;
          outputNulls[outIndex] = false;
          output[outIndex] = currentValues[i];
        }
      }
      if (notNull == bindings.getCurrentVectorSize()) {
        break;
      }
      inputBindingFilterer.getVectorMatch().setSelectionSize(nulls);
    }
    return new ExprEvalLongVector(output, outputNulls);
  }
}
