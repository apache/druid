/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.virtual;

import com.google.common.base.Strings;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprEval;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnValueSelector;
import io.druid.segment.DimensionSelector;

import javax.annotation.Nullable;

public class ExpressionSelectors
{
  private ExpressionSelectors()
  {
    // No instantiation.
  }

  public static ColumnValueSelector makeColumnValueSelector(
      ColumnSelectorFactory columnSelectorFactory,
      Expr expression
  )
  {
    final ExpressionObjectSelector baseSelector = ExpressionObjectSelector.from(columnSelectorFactory, expression);
    return new ColumnValueSelector()
    {
      @Override
      public double getDouble()
      {
        final ExprEval exprEval = baseSelector.getObject();
        return exprEval.isNull() ? 0.0 : exprEval.asDouble();
      }

      @Override
      public float getFloat()
      {
        final ExprEval exprEval = baseSelector.getObject();
        return exprEval.isNull() ? 0.0f : (float) exprEval.asDouble();
      }

      @Override
      public long getLong()
      {
        final ExprEval exprEval = baseSelector.getObject();
        return exprEval.isNull() ? 0L : exprEval.asLong();
      }

      @Nullable
      @Override
      public Object getObject()
      {
        final ExprEval exprEval = baseSelector.getObject();
        return exprEval.value();
      }

      @Override
      public Class classOfObject()
      {
        return Object.class;
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("baseSelector", baseSelector);
      }
    };
  }

  public static DimensionSelector makeDimensionSelector(
      final ColumnSelectorFactory columnSelectorFactory,
      final Expr expression,
      final ExtractionFn extractionFn
  )
  {
    final ExpressionObjectSelector baseSelector = ExpressionObjectSelector.from(columnSelectorFactory, expression);

    if (extractionFn == null) {
      class DefaultExpressionDimensionSelector extends BaseSingleValueDimensionSelector
      {
        @Override
        protected String getValue()
        {
          return Strings.emptyToNull(baseSelector.getObject().asString());
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          inspector.visit("baseSelector", baseSelector);
        }
      }
      return new DefaultExpressionDimensionSelector();
    } else {
      class ExtractionExpressionDimensionSelector extends BaseSingleValueDimensionSelector
      {
        @Override
        protected String getValue()
        {
          return extractionFn.apply(Strings.emptyToNull(baseSelector.getObject().asString()));
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          inspector.visit("baseSelector", baseSelector);
          inspector.visit("extractionFn", extractionFn);
        }
      }
      return new ExtractionExpressionDimensionSelector();
    }
  }
}
