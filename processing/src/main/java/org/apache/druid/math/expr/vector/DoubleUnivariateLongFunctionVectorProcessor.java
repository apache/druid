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
import org.apache.druid.math.expr.vector.functional.DoubleUnivariateLongFunction;

/**
 * specialized {@link DoubleUnivariateFunctionVectorProcessor} for processing (long[]) -> double[]
 */
public final class DoubleUnivariateLongFunctionVectorProcessor
    extends DoubleUnivariateFunctionVectorProcessor<long[]>
{
  private final DoubleUnivariateLongFunction longFunction;

  public DoubleUnivariateLongFunctionVectorProcessor(
      ExprVectorProcessor<long[]> processor,
      DoubleUnivariateLongFunction longFunction
  )
  {
    super(CastToTypeVectorProcessor.cast(processor, ExpressionType.LONG));
    this.longFunction = longFunction;
  }

  @Override
  public ExpressionType getOutputType()
  {
    return ExpressionType.DOUBLE;
  }

  @Override
  void processIndex(long[] input, int i)
  {
    outValues[i] = longFunction.process(input[i]);
  }
}
