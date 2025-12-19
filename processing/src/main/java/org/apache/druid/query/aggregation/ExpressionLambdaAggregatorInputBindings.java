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

package org.apache.druid.query.aggregation;

import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;

import javax.annotation.Nullable;

/**
 * Special {@link Expr.ObjectBinding} for use with {@link ExpressionLambdaAggregatorFactory}.
 * This value binding holds a value for a special 'accumulator' variable, in addition to the 'normal' bindings to the
 * underlying selector inputs for other identifiers, which allows for easy forward feeding of the results of an
 * expression evaluation to use in the bindings of the next evaluation.
 */
public class ExpressionLambdaAggregatorInputBindings implements Expr.ObjectBinding
{
  private final Expr.ObjectBinding inputBindings;
  private final String accumlatorIdentifier;
  private ExprEval<?> accumulator;

  public ExpressionLambdaAggregatorInputBindings(
      Expr.ObjectBinding inputBindings,
      String accumulatorIdentifier,
      ExprEval<?> initialValue
  )
  {
    this.accumlatorIdentifier = accumulatorIdentifier;
    this.inputBindings = inputBindings;
    this.accumulator = initialValue;
  }

  @Nullable
  @Override
  public Object get(String name)
  {
    if (accumlatorIdentifier.equals(name)) {
      return accumulator.value();
    }
    return inputBindings.get(name);
  }

  @Nullable
  @Override
  public ExpressionType getType(String name)
  {
    if (accumlatorIdentifier.equals(name)) {
      return accumulator.type();
    }
    return inputBindings.getType(name);
  }

  public void accumulate(ExprEval<?> eval)
  {
    accumulator = eval;
  }

  public ExprEval<?> getAccumulator()
  {
    return accumulator;
  }

  public void setAccumulator(ExprEval<?> acc)
  {
    this.accumulator = acc;
  }
}
