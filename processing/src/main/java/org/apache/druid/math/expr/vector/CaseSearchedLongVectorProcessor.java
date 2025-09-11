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

import org.apache.druid.error.DruidException;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.query.filter.vector.VectorMatch;

public class CaseSearchedLongVectorProcessor extends CaseSearchedFunctionVectorProcessor<long[]>
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
  public ExprEvalVector<long[]> evalVector(Expr.VectorInputBinding bindings)
  {
    conditionBindingFilterer.setBindings(bindings);
    // reset condition match to all true
    conditionBindingFilterer.getVectorMatch().copyFrom(VectorMatch.allTrue(bindings.getCurrentVectorSize()));
    thenBindingFilterer.setBindings(bindings);

    int totalMatches = 0;
    int currentMatches = 0;
    int currentMisses = 0;
    int currentCondition = 0;
    final int[] conditionSelection = conditionBindingFilterer.getVectorMatch().getSelection();
    final int[] thenSelection = thenBindingFilterer.getVectorMatch().getSelection();
    while (totalMatches < bindings.getCurrentVectorSize() && currentCondition < conditionProcessors.length) {
      // evaluate the condition clause, for each match we set the position in the 'then' bindings, else we set the
      // position in the 'condition' bindings to prepare for the next condition to evaluate only rows which were not
      // previously matched
      final ExprEvalVector<?> conditionVector = conditionProcessors[currentCondition].evalVector(conditionBindingFilterer);
      for (int i = 0; i < conditionBindingFilterer.getCurrentVectorSize(); i++) {
        int index = conditionSelection[i];
        if (conditionVector.elementAsBoolean(i)) {
          thenSelection[currentMatches++] = index;
        } else {
          conditionSelection[currentMisses++] = index;
        }
      }
      thenBindingFilterer.getVectorMatch().setSelectionSize(currentMatches);
      conditionBindingFilterer.getVectorMatch().setSelectionSize(currentMisses);

      // evaluate the result vector for the condition using the now filtered bindings to only evaluate the rows
      // matching the condition, and populate these to the correct positions in the output value and null vectors
      final ExprEvalVector<?> thenVector = thenProcessors[currentCondition].evalVector(thenBindingFilterer);
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

      // prepare for next loop
      currentCondition++;
      totalMatches += currentMatches;
      currentMisses = 0;
      currentMatches = 0;
    }

    // if there are still rows to match, we need to evaluate the 'else' clause
    if (totalMatches < bindings.getCurrentVectorSize()) {
      // if there is an explict else clause, evaluate it by using the conditional bindings against the result
      // processor (the rows which still need to be processed were set by the previous loop).
      if (thenProcessors.length > conditionProcessors.length) {
        final ExprEvalVector<?> elseVector = thenProcessors[currentCondition].evalVector(conditionBindingFilterer);
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
      } else {
        // no explicit else clause, the remaining values are all null
        for (int i = 0; i < conditionBindingFilterer.getCurrentVectorSize(); i++) {
          final int outIndex = conditionSelection[i];
          output[outIndex] = 0;
          outputNulls[outIndex] = true;
        }
      }
      totalMatches += conditionBindingFilterer.getCurrentVectorSize();
    }

    if (totalMatches < bindings.getCurrentVectorSize()) {
      throw DruidException.defensive(
          "output vector only populated with [%s] out of [%s] values",
          totalMatches,
          bindings.getCurrentVectorSize()
      );
    }

    return new ExprEvalLongVector(output, outputNulls);
  }
}
