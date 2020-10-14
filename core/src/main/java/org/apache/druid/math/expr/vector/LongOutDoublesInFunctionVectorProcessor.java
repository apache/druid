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

import org.apache.druid.math.expr.ExprType;

/**
 * specialized {@link BivariateFunctionVectorProcessor} for processing (double[], double[]) -> long[]
 */
public abstract class LongOutDoublesInFunctionVectorProcessor
    extends BivariateFunctionVectorProcessor<double[], double[], long[]>
{
  public LongOutDoublesInFunctionVectorProcessor(
      ExprVectorProcessor<double[]> left,
      ExprVectorProcessor<double[]> right,
      int maxVectorSize
  )
  {
    super(left, right, maxVectorSize, new long[maxVectorSize]);
  }

  public abstract long apply(double left, double right);

  @Override
  public ExprType getOutputType()
  {
    return ExprType.LONG;
  }

  @Override
  final void processIndex(double[] leftInput, double[] rightInput, int i)
  {
    outValues[i] = apply(leftInput[i], rightInput[i]);
  }

  @Override
  final ExprEvalVector<long[]> asEval()
  {
    return new ExprEvalLongVector(outValues, outNulls);
  }
}
