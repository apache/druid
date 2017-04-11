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

import io.druid.math.expr.Expr;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;

public class ExpressionSelectors
{
  private ExpressionSelectors()
  {
    // No instantiation.
  }

  public static ExpressionObjectSelector makeObjectColumnSelector(
      final ColumnSelectorFactory columnSelectorFactory,
      final Expr expression
  )
  {
    return ExpressionObjectSelector.from(columnSelectorFactory, expression);
  }

  public static LongColumnSelector makeLongColumnSelector(
      final ColumnSelectorFactory columnSelectorFactory,
      final Expr expression,
      final long nullValue
  )
  {
    final ExpressionObjectSelector baseSelector = ExpressionObjectSelector.from(columnSelectorFactory, expression);
    class ExpressionLongColumnSelector implements LongColumnSelector
    {
      @Override
      public long get()
      {
        final Number number = baseSelector.get();
        return number != null ? number.longValue() : nullValue;
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("baseSelector", baseSelector);
      }
    }
    return new ExpressionLongColumnSelector();
  }

  public static FloatColumnSelector makeFloatColumnSelector(
      final ColumnSelectorFactory columnSelectorFactory,
      final Expr expression,
      final float nullValue
  )
  {
    final ExpressionObjectSelector baseSelector = ExpressionObjectSelector.from(columnSelectorFactory, expression);
    class ExpressionFloatColumnSelector implements FloatColumnSelector
    {
      @Override
      public float get()
      {
        final Number number = baseSelector.get();
        return number != null ? number.floatValue() : nullValue;
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("baseSelector", baseSelector);
      }
    }
    return new ExpressionFloatColumnSelector();
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
          final Number number = baseSelector.get();
          return number == null ? null : String.valueOf(number);
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
          return extractionFn.apply(baseSelector.get());
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          inspector.visit("baseSelector", baseSelector);
        }
      }
      return new ExtractionExpressionDimensionSelector();
    }
  }
}
