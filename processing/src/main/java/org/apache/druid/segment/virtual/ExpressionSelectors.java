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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.expression.ExprUtils;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.ConstantExprEvalSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.DimensionSelectorUtils;
import org.apache.druid.segment.NilColumnValueSelector;
import org.apache.druid.segment.NullDimensionSelector;
import org.apache.druid.segment.column.Column;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.IndexedInts;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

public class ExpressionSelectors
{
  private ExpressionSelectors()
  {
    // No instantiation.
  }

  /**
   * Makes a ColumnValueSelector whose getObject method returns an Object that is the value computed by
   * an {@link ExprEval}.
   *
   * @see ExpressionSelectors#makeExprEvalSelector(ColumnSelectorFactory, Expr)
   */
  public static ColumnValueSelector makeColumnValueSelector(
      ColumnSelectorFactory columnSelectorFactory,
      Expr expression
  )
  {
    final ColumnValueSelector<ExprEval> baseSelector = makeExprEvalSelector(columnSelectorFactory, expression);

    return new ColumnValueSelector()
    {
      @Override
      public double getDouble()
      {
        // No Assert for null handling as baseSelector already have it.
        return baseSelector.getDouble();
      }

      @Override
      public float getFloat()
      {
        // No Assert for null handling as baseSelector already have it.
        return baseSelector.getFloat();
      }

      @Override
      public long getLong()
      {
        // No Assert for null handling as baseSelector already have it.
        return baseSelector.getLong();
      }

      @Override
      public boolean isNull()
      {
        return baseSelector.isNull();
      }

      @Nullable
      @Override
      public Object getObject()
      {
        // No need for null check on getObject() since baseSelector impls will never return null.
        //noinspection ConstantConditions
        return baseSelector.getObject().value();
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

  /**
   * Makes a ColumnValueSelector whose getObject method returns an {@link ExprEval}.
   *
   * @see ExpressionSelectors#makeColumnValueSelector(ColumnSelectorFactory, Expr)
   */
  public static ColumnValueSelector<ExprEval> makeExprEvalSelector(
      ColumnSelectorFactory columnSelectorFactory,
      Expr expression
  )
  {
    final List<String> columns = Parser.findRequiredBindings(expression);

    if (columns.size() == 1) {
      final String column = Iterables.getOnlyElement(columns);
      final ColumnCapabilities capabilities = columnSelectorFactory.getColumnCapabilities(column);

      if (column.equals(Column.TIME_COLUMN_NAME)) {
        // Optimization for expressions that hit the __time column and nothing else.
        // May be worth applying this optimization to all long columns?
        return new SingleLongInputCachingExpressionColumnValueSelector(
            columnSelectorFactory.makeColumnValueSelector(Column.TIME_COLUMN_NAME),
            expression
        );
      } else if (capabilities != null
                 && capabilities.getType() == ValueType.STRING
                 && capabilities.isDictionaryEncoded()) {
        // Optimization for expressions that hit one string column and nothing else.
        return new SingleStringInputCachingExpressionColumnValueSelector(
            columnSelectorFactory.makeDimensionSelector(new DefaultDimensionSpec(column, column, ValueType.STRING)),
            expression
        );
      }
    }

    final Expr.ObjectBinding bindings = createBindings(expression, columnSelectorFactory);

    if (bindings.equals(ExprUtils.nilBindings())) {
      // Optimization for constant expressions.
      return new ConstantExprEvalSelector(expression.eval(bindings));
    }

    // No special optimization.
    return new ExpressionColumnValueSelector(expression, bindings);
  }

  public static DimensionSelector makeDimensionSelector(
      final ColumnSelectorFactory columnSelectorFactory,
      final Expr expression,
      final ExtractionFn extractionFn
  )
  {
    final List<String> columns = Parser.findRequiredBindings(expression);

    if (columns.size() == 1) {
      final String column = Iterables.getOnlyElement(columns);
      final ColumnCapabilities capabilities = columnSelectorFactory.getColumnCapabilities(column);

      if (capabilities != null
          && capabilities.getType() == ValueType.STRING
          && capabilities.isDictionaryEncoded()) {
        // Optimization for dimension selectors that wrap a single underlying string column.
        return new SingleStringInputDimensionSelector(
            columnSelectorFactory.makeDimensionSelector(new DefaultDimensionSpec(column, column, ValueType.STRING)),
            expression
        );
      }
    }

    final ColumnValueSelector<ExprEval> baseSelector = makeExprEvalSelector(columnSelectorFactory, expression);

    if (baseSelector instanceof ConstantExprEvalSelector) {
      // Optimization for dimension selectors on constants.
      return DimensionSelectorUtils.constantSelector(baseSelector.getObject().asString(), extractionFn);
    } else if (baseSelector instanceof NilColumnValueSelector) {
      // Optimization for null dimension selector.
      return NullDimensionSelector.instance();
    } else if (extractionFn == null) {
      class DefaultExpressionDimensionSelector extends BaseSingleValueDimensionSelector
      {
        @Override
        protected String getValue()
        {
          return NullHandling.emptyToNullIfNeeded(baseSelector.getObject().asString());
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
          return extractionFn.apply(NullHandling.emptyToNullIfNeeded(baseSelector.getObject().asString()));
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

  private static Expr.ObjectBinding createBindings(Expr expression, ColumnSelectorFactory columnSelectorFactory)
  {
    final Map<String, Supplier<Object>> suppliers = Maps.newHashMap();
    for (String columnName : Parser.findRequiredBindings(expression)) {
      final ColumnCapabilities columnCapabilities = columnSelectorFactory
          .getColumnCapabilities(columnName);
      final ValueType nativeType = columnCapabilities != null ? columnCapabilities.getType() : null;
      final Supplier<Object> supplier;

      if (nativeType == ValueType.FLOAT) {
        ColumnValueSelector selector = columnSelectorFactory
            .makeColumnValueSelector(columnName);
        supplier = makeNullableSupplier(selector, selector::getFloat);
      } else if (nativeType == ValueType.LONG) {
        ColumnValueSelector selector = columnSelectorFactory
            .makeColumnValueSelector(columnName);
        supplier = makeNullableSupplier(selector, selector::getLong);
      } else if (nativeType == ValueType.DOUBLE) {
        ColumnValueSelector selector = columnSelectorFactory
            .makeColumnValueSelector(columnName);
        supplier = makeNullableSupplier(selector, selector::getDouble);
      } else if (nativeType == ValueType.STRING) {
        supplier = supplierFromDimensionSelector(
            columnSelectorFactory
                .makeDimensionSelector(new DefaultDimensionSpec(columnName, columnName))
        );
      } else if (nativeType == null) {
        // Unknown ValueType. Try making an Object selector and see if that gives us anything useful.
        supplier = supplierFromObjectSelector(columnSelectorFactory.makeColumnValueSelector(columnName));
      } else {
        // Unhandleable ValueType (COMPLEX).
        supplier = null;
      }

      if (supplier != null) {
        suppliers.put(columnName, supplier);
      }
    }

    if (suppliers.isEmpty()) {
      return ExprUtils.nilBindings();
    } else if (suppliers.size() == 1) {
      // If there's only one supplier, we can skip the Map and just use that supplier when asked for something.
      final String column = Iterables.getOnlyElement(suppliers.keySet());
      final Supplier<Object> supplier = Iterables.getOnlyElement(suppliers.values());

      return identifierName -> {
        // There's only one binding, and it must be the single column, so it can safely be ignored in production.
        assert column.equals(identifierName);
        return supplier.get();
      };
    } else {
      return Parser.withSuppliers(suppliers);
    }
  }

  private static <T> Supplier<T> makeNullableSupplier(
      ColumnValueSelector selector,
      Supplier<T> supplier
  )
  {
    if (NullHandling.replaceWithDefault()) {
      return supplier;
    } else {
      return () -> {
        if (selector.isNull()) {
          return null;
        }
        return supplier.get();
      };
    }
  }

  @VisibleForTesting
  @Nonnull
  static Supplier<Object> supplierFromDimensionSelector(final DimensionSelector selector)
  {
    Preconditions.checkNotNull(selector, "selector");
    return () -> {
      final IndexedInts row = selector.getRow();

      if (row.size() == 1) {
        return selector.lookupName(row.get(0));
      } else {
        // Can't handle non-singly-valued rows in expressions.
        // Treat them as nulls until we think of something better to do.
        return null;
      }
    };
  }

  @Nullable
  static Supplier<Object> supplierFromObjectSelector(final BaseObjectColumnValueSelector<?> selector)
  {
    if (selector instanceof NilColumnValueSelector) {
      return null;
    }

    final Class<?> clazz = selector.classOfObject();
    if (Number.class.isAssignableFrom(clazz) || String.class.isAssignableFrom(clazz)) {
      // Number, String supported as-is.
      return selector::getObject;
    } else if (clazz.isAssignableFrom(Number.class) || clazz.isAssignableFrom(String.class)) {
      // Might be Numbers and Strings. Use a selector that double-checks.
      return () -> {
        final Object val = selector.getObject();
        if (val instanceof Number || val instanceof String) {
          return val;
        } else {
          return null;
        }
      };
    } else {
      // No numbers or strings.
      return null;
    }
  }
}
