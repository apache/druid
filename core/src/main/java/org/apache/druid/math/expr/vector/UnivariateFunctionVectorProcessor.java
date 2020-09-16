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

public abstract class UnivariateFunctionVectorProcessor<TInput, TOutput> implements VectorExprProcessor<TOutput>
{
  final VectorExprProcessor<TInput> processor;
  final int maxVectorSize;
  final boolean[] outNulls;
  final TOutput outValues;

  public UnivariateFunctionVectorProcessor(
      VectorExprProcessor<TInput> processor,
      int maxVectorSize,
      TOutput outValues
  )
  {
    this.processor = processor;
    this.maxVectorSize = maxVectorSize;
    this.outNulls = new boolean[maxVectorSize];
    this.outValues = outValues;
  }

  @Override
  public final VectorExprEval<TOutput> evalVector(Expr.VectorInputBinding bindings)
  {
    final VectorExprEval<TInput> lhs = processor.evalVector(bindings);

    final int currentSize = bindings.getCurrentVectorSize();
    final boolean[] inputNulls = lhs.getNullVector();
    final boolean hasNulls = inputNulls != null;

    final TInput input = lhs.values();

    if (hasNulls) {
      for (int i = 0; i < currentSize; i++) {
        outNulls[i] = inputNulls[i];
        if (!outNulls[i]) {
          processIndex(input, i);
        } else {
          processNull(i);
        }
      }
    } else {
      for (int i = 0; i < currentSize; i++) {
        outNulls[i] = false;
        processIndex(input, i);
      }
    }
    return asEval();
  }

  abstract void processIndex(TInput input, int i);

  abstract void processNull(int i);

  abstract VectorExprEval<TOutput> asEval();
}
