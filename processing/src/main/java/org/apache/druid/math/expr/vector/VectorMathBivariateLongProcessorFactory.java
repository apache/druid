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
import org.apache.druid.segment.column.Types;

/**
 * Make a 2 argument, math processor with the following type rules
 *    long, long      -> long
 *    long, double    -> long
 *    double, long    -> long
 *    double, double  -> long
 */
public abstract class VectorMathBivariateLongProcessorFactory implements BivariateFunctionVectorProcessorFactory
{
  @Override
  public <T> ExprVectorProcessor<T> asProcessor(Expr.VectorInputBindingInspector inspector, Expr left, Expr right)
  {
    final ExpressionType leftType = left.getOutputType(inspector);
    final ExpressionType rightType = right.getOutputType(inspector);
    ExprVectorProcessor<?> processor = null;
    if (Types.is(leftType, ExprType.LONG)) {
      if (Types.isNullOr(rightType, ExprType.LONG)) {
        processor = longsProcessor(inspector, left, right);
      } else if (rightType.is(ExprType.DOUBLE)) {
        processor = longDoubleProcessor(inspector, left, right);
      } else {
        processor = doublesProcessor(inspector, left, right);
      }
    } else if (Types.is(leftType, ExprType.DOUBLE)) {
      if (Types.is(rightType, ExprType.LONG)) {
        processor = doubleLongProcessor(inspector, left, right);
      } else if (Types.isNullOr(rightType, ExprType.DOUBLE)) {
        processor = doublesProcessor(inspector, left, right);
      } else {
        processor = doublesProcessor(inspector, left, right);
      }
    } else if (leftType == null) {
      if (Types.isNullOr(rightType, ExprType.LONG)) {
        processor = longsProcessor(inspector, left, right);
      } else if (Types.is(rightType, ExprType.DOUBLE)) {
        processor = doublesProcessor(inspector, left, right);
      } else {
        processor = doublesProcessor(inspector, left, right);
      }
    } else {
      processor = doublesProcessor(inspector, left, right);
    }
    if (processor == null) {
      throw Exprs.cannotVectorize();
    }
    return (ExprVectorProcessor<T>) processor;
  }

  public abstract ExprVectorProcessor<long[]> longsProcessor(
      Expr.VectorInputBindingInspector inspector,
      Expr left,
      Expr right
  );

  public abstract ExprVectorProcessor<long[]> longDoubleProcessor(
      Expr.VectorInputBindingInspector inspector,
      Expr left,
      Expr right
  );

  public abstract ExprVectorProcessor<long[]> doubleLongProcessor(
      Expr.VectorInputBindingInspector inspector,
      Expr left,
      Expr right
  );

  public abstract ExprVectorProcessor<long[]> doublesProcessor(
      Expr.VectorInputBindingInspector inspector,
      Expr left,
      Expr right
  );
}
