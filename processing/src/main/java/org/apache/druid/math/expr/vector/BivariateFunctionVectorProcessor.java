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

import javax.annotation.Nullable;

/**
 * Basic vector processor that processes 2 inputs and works for both primitive value vectors and object vectors.
 * Different from {@link BivariateLongFunctionVectorValueProcessor}, {@link BivariateDoubleFunctionVectorValueProcessor}
 * and {@link BivariateFunctionVectorObjectProcessor} in that subclasses of this class must check for and directly
 * decide how to handle null values.
 */
public abstract class BivariateFunctionVectorProcessor<TLeftInput, TRightInput, TOutput>
    implements ExprVectorProcessor<TOutput>
{
  private final ExpressionType outputType;
  private final ExprVectorProcessor<TLeftInput> left;
  private final ExprVectorProcessor<TRightInput> right;

  public BivariateFunctionVectorProcessor(
      ExpressionType outputType,
      ExprVectorProcessor<TLeftInput> left,
      ExprVectorProcessor<TRightInput> right
  )
  {
    this.outputType = outputType;
    this.left = left;
    this.right = right;
  }

  @Override
  public ExprEvalVector<TOutput> evalVector(Expr.VectorInputBinding bindings)
  {
    final ExprEvalVector<TLeftInput> lhs = left.evalVector(bindings);
    final ExprEvalVector<TRightInput> rhs = right.evalVector(bindings);

    TLeftInput leftValues = lhs.values;
    TRightInput rightValues = rhs.values;
    final boolean[] leftNulls = outputType.isNumeric() ? lhs.getNullVector() : null;
    final boolean[] rightNulls = outputType.isNumeric() ? rhs.getNullVector() : null;

    for (int i = 0; i < bindings.getCurrentVectorSize(); i++) {
      processIndex(leftValues, leftNulls, rightValues, rightNulls, i);
    }
    return asEval();
  }

  public abstract void processIndex(
      TLeftInput leftInput,
      @Nullable boolean[] leftNulls,
      TRightInput rightInput,
      @Nullable boolean[] rightNulls,
      int i
  );

  public abstract ExprEvalVector<TOutput> asEval();

  @Override
  public ExpressionType getOutputType()
  {
    return outputType;
  }
}
