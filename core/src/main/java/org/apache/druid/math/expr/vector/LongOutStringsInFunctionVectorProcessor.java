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

import javax.annotation.Nullable;

public abstract class LongOutStringsInFunctionVectorProcessor extends BivariateFunctionVectorObjectProcessor<String[], String[], long[]>
{
  private final boolean[] outNulls;

  protected LongOutStringsInFunctionVectorProcessor(
      ExprVectorProcessor<String[]> left,
      ExprVectorProcessor<String[]> right,
      int maxVectorSize
  )
  {
    super(
        CastToTypeVectorProcessor.cast(left, ExprType.STRING),
        CastToTypeVectorProcessor.cast(right, ExprType.STRING),
        maxVectorSize,
        new long[maxVectorSize]
    );
    this.outNulls = new boolean[maxVectorSize];
  }

  @Nullable
  abstract Long processValue(@Nullable String leftVal, @Nullable String rightVal);

  @Override
  void processIndex(String[] strings, String[] strings2, int i)
  {
    final Long outVal = processValue(strings[i], strings2[i]);
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
  public ExprType getOutputType()
  {
    return ExprType.LONG;
  }
}
