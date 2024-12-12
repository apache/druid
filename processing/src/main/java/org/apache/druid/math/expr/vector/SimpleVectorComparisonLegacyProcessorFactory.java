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
import org.apache.druid.math.expr.ExpressionProcessing;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.vector.functional.DoubleBivariateDoubleLongFunction;
import org.apache.druid.math.expr.vector.functional.DoubleBivariateDoublesFunction;
import org.apache.druid.math.expr.vector.functional.DoubleBivariateLongDoubleFunction;
import org.apache.druid.math.expr.vector.functional.LongBivariateLongsFunction;
import org.apache.druid.math.expr.vector.functional.LongBivariateObjectsFunction;
import org.apache.druid.segment.column.Types;

public class SimpleVectorComparisonLegacyProcessorFactory extends SimpleVectorMathBivariateProcessorFactory
{
  private final LongBivariateObjectsFunction objectsFunction;

  protected SimpleVectorComparisonLegacyProcessorFactory(
      LongBivariateObjectsFunction objectsFunction,
      LongBivariateLongsFunction longsFunction,
      DoubleBivariateLongDoubleFunction longDoubleFunction,
      DoubleBivariateDoubleLongFunction doubleLongFunction,
      DoubleBivariateDoublesFunction doublesFunction
  )
  {
    super(longsFunction, longDoubleFunction, doubleLongFunction, doublesFunction);
    this.objectsFunction = objectsFunction;
  }

  @Override
  public <T> ExprVectorProcessor<T> asProcessor(Expr.VectorInputBindingInspector inspector, Expr left, Expr right)
  {
    assert !ExpressionProcessing.useStrictBooleans();
    final ExpressionType leftType = left.getOutputType(inspector);
    final ExpressionType rightType = right.getOutputType(inspector);
    ExprVectorProcessor<?> processor = null;
    if (Types.is(leftType, ExprType.STRING)) {
      if (Types.isNullOr(rightType, ExprType.STRING)) {
        processor = objectsProcessor(inspector, left, right, ExpressionType.STRING);
      } else {
        processor = doublesProcessor(inspector, left, right);
      }
    } else if (leftType == null) {
      if (Types.isNullOr(rightType, ExprType.STRING)) {
        processor = objectsProcessor(inspector, left, right, ExpressionType.STRING);
      }
    } else if (leftType.is(ExprType.DOUBLE) || Types.is(rightType, ExprType.DOUBLE)) {
      processor = doublesProcessor(inspector, left, right);
    }
    if (processor != null) {
      return (ExprVectorProcessor<T>) processor;
    }
    return super.asProcessor(inspector, left, right);
  }

  private ExprVectorProcessor<long[]> objectsProcessor(
      Expr.VectorInputBindingInspector inspector,
      Expr left,
      Expr right,
      ExpressionType inputType
  )
  {
    return new LongBivariateObjectsFunctionVectorProcessor(
        left.asVectorProcessor(inspector),
        right.asVectorProcessor(inspector),
        inputType,
        objectsFunction
    );
  }
}
