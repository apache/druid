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

public abstract class BivariateFunctionVectorProcessor<TLeftInput, TRightInput, TOutput> implements VectorExprProcessor<TOutput>
{
  final VectorExprProcessor<TLeftInput> left;
  final VectorExprProcessor<TRightInput> right;
  final int maxVectorSize;
  final boolean[] outNulls;
  final TOutput outValues;

  protected BivariateFunctionVectorProcessor(
      VectorExprProcessor<TLeftInput> left,
      VectorExprProcessor<TRightInput> right,
      int maxVectorSize,
      TOutput outValues
  )
  {
    this.left = left;
    this.right = right;
    this.maxVectorSize = maxVectorSize;
    this.outNulls = new boolean[maxVectorSize];
    this.outValues = outValues;
  }

  @Override
  public final VectorExprEval<TOutput> evalVector(Expr.VectorInputBinding bindings)
  {
    final VectorExprEval<TLeftInput> lhs = left.evalVector(bindings);
    final VectorExprEval<TRightInput> rhs = right.evalVector(bindings);

    final int currentSize = bindings.getCurrentVectorSize();
    final boolean[] leftNulls = lhs.getNullVector();
    final boolean[] rightNulls = rhs.getNullVector();
    final boolean hasLeftNulls = leftNulls != null;
    final boolean hasRightNulls = rightNulls != null;
    final boolean hasNulls = hasLeftNulls || hasRightNulls;

    final TLeftInput leftInput = lhs.values();
    final TRightInput rightInput = rhs.values();

    if (hasNulls) {
      for (int i = 0; i < currentSize; i++) {
        outNulls[i] = (hasLeftNulls && leftNulls[i]) || (hasRightNulls && rightNulls[i]);
        if (!outNulls[i]) {
          processIndex(leftInput, rightInput, i);
        } else {
          processNull(i);
        }
      }
    } else {
      for (int i = 0; i < currentSize; i++) {
        processIndex(leftInput, rightInput, i);
        outNulls[i] = false;
      }
    }
    return asEval();
  }

  abstract void processIndex(TLeftInput leftInput, TRightInput rightInput, int i);

  abstract void processNull(int i);

  abstract VectorExprEval<TOutput> asEval();
}
