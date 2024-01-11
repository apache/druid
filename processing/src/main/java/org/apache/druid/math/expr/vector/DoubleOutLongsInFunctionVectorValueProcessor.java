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

/**
 * specialized {@link BivariateDoubleFunctionVectorValueProcessor} for processing (long[], long[]) -> double[]
 */
public abstract class DoubleOutLongsInFunctionVectorValueProcessor
    extends BivariateDoubleFunctionVectorValueProcessor<long[], long[]>
{
  public DoubleOutLongsInFunctionVectorValueProcessor(
      ExprVectorProcessor<long[]> left,
      ExprVectorProcessor<long[]> right,
      int maxVectorSize
  )
  {
    super(
        CastToTypeVectorProcessor.cast(left, ExpressionType.LONG),
        CastToTypeVectorProcessor.cast(right, ExpressionType.LONG),
        maxVectorSize
    );
  }

  public abstract double apply(long left, long right);

  @Override
  public ExpressionType getOutputType()
  {
    return ExpressionType.DOUBLE;
  }

  @Override
  final void processIndex(long[] leftInput, long[] rightInput, int i)
  {
    outValues[i] = apply(leftInput[i], rightInput[i]);
  }
}
