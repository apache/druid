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
import org.apache.druid.math.expr.vector.functional.DoubleBivariateLongDoubleFunction;

/**
 * specialized {@link DoubleBivariateFunctionVectorProcessor} for processing (long[], double[]) -> double[]
 */
public final class DoubleBivariateLongDoubleFunctionVectorProcessor
    extends DoubleBivariateFunctionVectorProcessor<long[], double[]>
{
  private final DoubleBivariateLongDoubleFunction longDoubleFunction;

  public DoubleBivariateLongDoubleFunctionVectorProcessor(
      ExprVectorProcessor<long[]> left,
      ExprVectorProcessor<double[]> right,
      DoubleBivariateLongDoubleFunction longDoubleFunction
  )
  {
    super(
        CastToTypeVectorProcessor.cast(left, ExpressionType.LONG),
        CastToTypeVectorProcessor.cast(right, ExpressionType.DOUBLE)
    );
    this.longDoubleFunction = longDoubleFunction;
  }

  @Override
  public ExpressionType getOutputType()
  {
    return ExpressionType.DOUBLE;
  }

  @Override
  void processIndex(long[] leftInput, double[] rightInput, int i)
  {
    outValues[i] = longDoubleFunction.process(leftInput[i], rightInput[i]);
  }
}
