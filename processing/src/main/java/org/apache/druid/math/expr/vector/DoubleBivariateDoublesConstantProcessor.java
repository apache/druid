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
import org.apache.druid.math.expr.vector.functional.DoubleBivariateDoublesFunction;

/**
 * Double arithmetic processor with one operand bound to a constant.
 */
public final class DoubleBivariateDoublesConstantProcessor extends DoubleUnivariateFunctionVectorProcessor<double[]>
{
  private final DoubleBivariateDoublesFunction function;
  private final double constant;
  private final boolean constantIsLeftOperand;

  public DoubleBivariateDoublesConstantProcessor(
      ExprVectorProcessor<?> input,
      DoubleBivariateDoublesFunction function,
      double constant,
      boolean constantIsLeftOperand
  )
  {
    super(CastToTypeVectorProcessor.cast(input, ExpressionType.DOUBLE));
    this.function = function;
    this.constant = constant;
    this.constantIsLeftOperand = constantIsLeftOperand;
  }

  @Override
  public ExpressionType getOutputType()
  {
    return ExpressionType.DOUBLE;
  }

  @Override
  void processIndex(double[] input, int i)
  {
    outValues[i] = constantIsLeftOperand
                   ? function.process(constant, input[i])
                   : function.process(input[i], constant);
  }
}
