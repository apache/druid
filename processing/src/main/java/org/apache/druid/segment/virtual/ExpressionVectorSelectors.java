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

package org.apache.druid.segment.virtual;

import com.google.common.base.Preconditions;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprType;
import org.apache.druid.math.expr.vector.ExprVectorProcessor;
import org.apache.druid.query.expression.ExprUtils;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.vector.ConstantVectorSelectors;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;

import java.util.List;

public class ExpressionVectorSelectors
{
  private ExpressionVectorSelectors()
  {
    // No instantiation.
  }

  public static SingleValueDimensionVectorSelector makeSingleValueDimensionVectorSelector(
      VectorColumnSelectorFactory factory,
      Expr expression
  )
  {
    final ExpressionPlan plan = ExpressionPlanner.plan(factory, expression);
    Preconditions.checkArgument(plan.is(ExpressionPlan.Trait.VECTORIZABLE));
    // only constant expressions are currently supported, nothing else should get here

    if (plan.isConstant()) {
      String constant = plan.getExpression().eval(ExprUtils.nilBindings()).asString();
      return ConstantVectorSelectors.singleValueDimensionVectorSelector(factory.getReadableVectorInspector(), constant);
    }
    throw new IllegalStateException("Only constant expressions currently support dimension selectors");
  }

  public static VectorValueSelector makeVectorValueSelector(
      VectorColumnSelectorFactory factory,
      Expr expression
  )
  {
    final ExpressionPlan plan = ExpressionPlanner.plan(factory, expression);
    Preconditions.checkArgument(plan.is(ExpressionPlan.Trait.VECTORIZABLE));

    if (plan.isConstant()) {
      return ConstantVectorSelectors.vectorValueSelector(
          factory.getReadableVectorInspector(),
          (Number) plan.getExpression().eval(ExprUtils.nilBindings()).value()
      );
    }
    final Expr.VectorInputBinding bindings = createVectorBindings(plan.getAnalysis(), factory);
    final ExprVectorProcessor<?> processor = plan.getExpression().buildVectorized(bindings);
    return new ExpressionVectorValueSelector(processor, bindings);
  }

  public static VectorObjectSelector makeVectorObjectSelector(
      VectorColumnSelectorFactory factory,
      Expr expression
  )
  {
    final ExpressionPlan plan = ExpressionPlanner.plan(factory, expression);
    Preconditions.checkArgument(plan.is(ExpressionPlan.Trait.VECTORIZABLE));

    if (plan.isConstant()) {
      return ConstantVectorSelectors.vectorObjectSelector(
          factory.getReadableVectorInspector(),
          plan.getExpression().eval(ExprUtils.nilBindings()).value()
      );
    }

    final Expr.VectorInputBinding bindings = createVectorBindings(plan.getAnalysis(), factory);
    final ExprVectorProcessor<?> processor = plan.getExpression().buildVectorized(bindings);
    return new ExpressionVectorObjectSelector(processor, bindings);
  }

  private static Expr.VectorInputBinding createVectorBindings(
      Expr.BindingAnalysis bindingAnalysis,
      VectorColumnSelectorFactory vectorColumnSelectorFactory
  )
  {
    ExpressionVectorInputBinding binding = new ExpressionVectorInputBinding(
        vectorColumnSelectorFactory.getReadableVectorInspector()
    );
    final List<String> columns = bindingAnalysis.getRequiredBindingsList();
    for (String columnName : columns) {
      final ColumnCapabilities columnCapabilities = vectorColumnSelectorFactory.getColumnCapabilities(columnName);
      final ValueType nativeType = columnCapabilities != null ? columnCapabilities.getType() : null;

      // null capabilities should be backed by a nil vector selector since it means the column effectively doesnt exist
      if (nativeType != null) {
        switch (nativeType) {
          case FLOAT:
          case DOUBLE:
            binding.addNumeric(columnName, ExprType.DOUBLE, vectorColumnSelectorFactory.makeValueSelector(columnName));
            break;
          case LONG:
            binding.addNumeric(columnName, ExprType.LONG, vectorColumnSelectorFactory.makeValueSelector(columnName));
            break;
          default:
            binding.addObjectSelector(
                columnName,
                ExprType.STRING,
                vectorColumnSelectorFactory.makeObjectSelector(columnName)
            );
        }
      }
    }
    return binding;
  }
}
