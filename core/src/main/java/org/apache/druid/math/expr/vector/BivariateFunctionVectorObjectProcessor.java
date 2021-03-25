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

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.math.expr.Expr;

import java.lang.reflect.Array;

/**
 * Base {@link ExprVectorProcessor} for expressions and functions with 2 'object' typed inputs (strings, arrays)
 */
public abstract class BivariateFunctionVectorObjectProcessor<TLeftInput, TRightInput, TOutput>
    implements ExprVectorProcessor<TOutput>
{
  final ExprVectorProcessor<TLeftInput> left;
  final ExprVectorProcessor<TRightInput> right;
  final int maxVectorSize;
  final TOutput outValues;
  final boolean sqlCompatible = NullHandling.sqlCompatible();

  protected BivariateFunctionVectorObjectProcessor(
      ExprVectorProcessor<TLeftInput> left,
      ExprVectorProcessor<TRightInput> right,
      int maxVectorSize,
      TOutput outValues
  )
  {
    this.left = left;
    this.right = right;
    this.maxVectorSize = maxVectorSize;
    this.outValues = outValues;
  }

  @Override
  public ExprEvalVector<TOutput> evalVector(Expr.VectorInputBinding bindings)
  {
    final ExprEvalVector<TLeftInput> lhs = left.evalVector(bindings);
    final ExprEvalVector<TRightInput> rhs = right.evalVector(bindings);

    final int currentSize = bindings.getCurrentVectorSize();

    final TLeftInput leftInput = lhs.values();
    final TRightInput rightInput = rhs.values();

    if (sqlCompatible) {
      for (int i = 0; i < currentSize; i++) {
        if (Array.get(leftInput, i) == null || Array.get(rightInput, i) == null) {
          processNull(i);
        } else {
          processIndex(leftInput, rightInput, i);
        }
      }
    } else {
      for (int i = 0; i < currentSize; i++) {
        processIndex(leftInput, rightInput, i);
      }
    }
    return asEval();
  }

  abstract void processIndex(TLeftInput leftInput, TRightInput rightInput, int i);

  abstract void processNull(int i);

  abstract ExprEvalVector<TOutput> asEval();
}
