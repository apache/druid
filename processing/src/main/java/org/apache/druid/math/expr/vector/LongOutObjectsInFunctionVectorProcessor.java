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

import org.apache.druid.math.expr.ExpressionType;

import javax.annotation.Nullable;

public abstract class LongOutObjectsInFunctionVectorProcessor
    extends BivariateFunctionVectorObjectProcessor<Object[], Object[], long[]>
{
  private final boolean[] outNulls;

  protected LongOutObjectsInFunctionVectorProcessor(
      ExprVectorProcessor<Object[]> left,
      ExprVectorProcessor<Object[]> right,
      int maxVectorSize,
      ExpressionType inputType
  )
  {
    super(
        CastToTypeVectorProcessor.cast(left, inputType),
        CastToTypeVectorProcessor.cast(right, inputType),
        maxVectorSize,
        new long[maxVectorSize]
    );
    this.outNulls = new boolean[maxVectorSize];
  }

  @Nullable
  abstract Long processValue(@Nullable Object leftVal, @Nullable Object rightVal);

  @Override
  void processIndex(Object[] in1, Object[] in2, int i)
  {
    final Long outVal = processValue(in1[i], in2[i]);
    if (outVal == null) {
      outValues[i] = 0L;
      outNulls[i] = true;
    } else {
      outValues[i] = outVal;
      outNulls[i] = false;
    }
  }

  @Override
  void processNull(int i)
  {
    outValues[i] = 0L;
    outNulls[i] = true;
  }

  @Override
  ExprEvalVector<long[]> asEval()
  {
    return new ExprEvalLongVector(outValues, outNulls);
  }

  @Override
  public ExpressionType getOutputType()
  {
    return ExpressionType.LONG;
  }
}
