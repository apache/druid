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
import org.apache.druid.math.expr.ExprType;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.Exprs;

/**
 * Make a 1 argument math processor with the following type rules
 *    long    -> double
 *    double  -> double
 */
public abstract class VectorMathUnivariateDoubleProcessorFactory implements UnivariateVectorProcessorFactory
{
  @Override
  public <T> ExprVectorProcessor<T> asProcessor(Expr.VectorInputBindingInspector inspector, Expr arg)
  {
    final ExpressionType inputType = arg.getOutputType(inspector);

    ExprVectorProcessor<?> processor = null;
    if (inputType != null) {
      if (inputType.is(ExprType.LONG)) {
        processor = longProcessor(inspector, arg);
      } else if (inputType.is(ExprType.DOUBLE)) {
        processor = doubleProcessor(inspector, arg);
      }
    } else {
      processor = doubleProcessor(inspector, arg);
    }
    if (processor == null) {
      throw Exprs.cannotVectorize();
    }
    return (ExprVectorProcessor<T>) processor;
  }

  public abstract ExprVectorProcessor<double[]> longProcessor(Expr.VectorInputBindingInspector inspector, Expr arg);

  public abstract ExprVectorProcessor<double[]> doubleProcessor(Expr.VectorInputBindingInspector inspector, Expr arg);
}
