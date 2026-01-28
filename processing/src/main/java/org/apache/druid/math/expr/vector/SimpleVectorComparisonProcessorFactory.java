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

import org.apache.druid.math.expr.Evals;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.ExpressionTypeConversion;
import org.apache.druid.math.expr.vector.functional.ComparatorFunction;
import org.apache.druid.math.expr.vector.functional.LongBivariateDoubleLongFunction;
import org.apache.druid.math.expr.vector.functional.LongBivariateDoublesFunction;
import org.apache.druid.math.expr.vector.functional.LongBivariateLongDoubleFunction;
import org.apache.druid.math.expr.vector.functional.LongBivariateLongsFunction;
import org.apache.druid.math.expr.vector.functional.LongBivariateObjectsFunction;
import org.apache.druid.segment.column.NullableTypeStrategy;
import org.apache.druid.segment.column.Types;

import javax.annotation.Nullable;

public class SimpleVectorComparisonProcessorFactory extends SimpleVectorMathBivariateLongProcessorFactory
{
  private final ComparatorFunction comparatorFunction;

  public SimpleVectorComparisonProcessorFactory(
      ComparatorFunction comparatorFunction,
      LongBivariateLongsFunction longsFunction,
      LongBivariateLongDoubleFunction longDoubleFunction,
      LongBivariateDoubleLongFunction doubleLongFunction,
      LongBivariateDoublesFunction doublesFunction
  )
  {
    super(longsFunction, longDoubleFunction, doubleLongFunction, doublesFunction);
    this.comparatorFunction = comparatorFunction;
  }

  @Override
  public <T> ExprVectorProcessor<T> asProcessor(Expr.VectorInputBindingInspector inspector, Expr left, Expr right)
  {
    final ExpressionType leftType = left.getOutputType(inspector);
    final ExpressionType rightType = right.getOutputType(inspector);
    if (Types.isNumeric(leftType) || Types.isNumeric(rightType)) {
      return super.asProcessor(inspector, left, right);
    }
    ExprVectorProcessor<?> processor = null;
    final ExpressionType commonType = ExpressionTypeConversion.leastRestrictiveType(leftType, rightType);
    if (commonType != null) {
      processor = objectsProcessor(inspector, left, right, commonType);
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
        new ObjectsComparatorFunction(inputType.getNullableStrategy(), comparatorFunction)
    );
  }

  public static final class ObjectsComparatorFunction implements LongBivariateObjectsFunction
  {
    private final NullableTypeStrategy<Object> nullableTypeStrategy;
    private final ComparatorFunction comparatorFunction;

    public ObjectsComparatorFunction(
        NullableTypeStrategy<Object> nullableTypeStrategy,
        ComparatorFunction comparatorFunction
    )
    {
      this.nullableTypeStrategy = nullableTypeStrategy;
      this.comparatorFunction = comparatorFunction;
    }

    @Nullable
    @Override
    public Long process(@Nullable Object left, @Nullable Object right)
    {
      return Evals.asLong(comparatorFunction.compare(nullableTypeStrategy.compare(left, right)));
    }
  }
}
