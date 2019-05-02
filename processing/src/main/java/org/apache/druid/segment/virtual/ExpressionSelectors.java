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
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.expression.ExprUtils;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.BaseSingleValueDimensionSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.ConstantExprEvalSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.NilColumnValueSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.IndexedInts;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
    final Set<String> expectedArrays = Parser.findArrayFnBindings(expression);
    final Set<String> actualArrays = Parser.findArrayFnBindings(expression);
    final Set<String> unknownIfArrays = new HashSet<>();

    if (columns.size() == 1) {
      final String column = Iterables.getOnlyElement(columns);
      final ColumnCapabilities capabilities = columnSelectorFactory.getColumnCapabilities(column);

      if (capabilities != null && capabilities.getType() == ValueType.LONG) {
        // Optimization for expressions that hit one long column and nothing else.
        return new SingleLongInputCachingExpressionColumnValueSelector(
            columnSelectorFactory.makeColumnValueSelector(column),
            expression,
            !ColumnHolder.TIME_COLUMN_NAME.equals(column) // __time doesn't need an LRU cache since it is sorted.
        );
      } else if (capabilities != null
                 && capabilities.getType() == ValueType.STRING
                 && capabilities.isDictionaryEncoded()
                 && capabilities.isComplete()
                 && !capabilities.hasMultipleValues()
                 && !expectedArrays.contains(column)) {
        // Optimization for expressions that hit one string column and nothing else.
        return new SingleStringInputCachingExpressionColumnValueSelector(
            columnSelectorFactory.makeDimensionSelector(new DefaultDimensionSpec(column, column, ValueType.STRING)),
            expression
        );
      }
    }

    for (String column : columns) {
      final ColumnCapabilities capabilities = columnSelectorFactory.getColumnCapabilities(column);
      if (capabilities != null) {
        if (capabilities.hasMultipleValues()) {
          actualArrays.add(column);
        } else if (!capabilities.isComplete() && capabilities.getType().equals(ValueType.STRING) && (actualArrays.contains(column) || !expectedArrays.contains(column))) {
          unknownIfArrays.add(column);
        }
      } else {
        unknownIfArrays.add(column);
      }
    }

    final List<String> needsApplied = columns.stream().filter(c -> actualArrays.contains(c) && !expectedArrays.contains(c)).collect(Collectors.toList());
    final Expr finalExpr;
    if (needsApplied.size() > 0) {
      finalExpr = Parser.applyUnappliedIdentifiers(expression, needsApplied);
    } else {
      finalExpr = expression;
    }


    final Expr.ObjectBinding bindings = createBindings(expression, columnSelectorFactory, unknownIfArrays);

    if (bindings.equals(ExprUtils.nilBindings())) {
      // Optimization for constant expressions.
      return new ConstantExprEvalSelector(expression.eval(bindings));
    }

    if (unknownIfArrays.size() > 0) {
      return new OpportunisticMultiValueStringExpressionColumnValueSelector(finalExpr, bindings, unknownIfArrays);
    }
    // No special optimization.
    return new ExpressionColumnValueSelector(finalExpr, bindings);
  }

  public static DimensionSelector makeDimensionSelector(
      final ColumnSelectorFactory columnSelectorFactory,
      final Expr expression,
      final ExtractionFn extractionFn
  )
  {
    final List<String> columns = Parser.findRequiredBindings(expression);
    final Set<String> expectedArrays = Parser.findArrayFnBindings(expression);
    final Set<String> actualArrays = Parser.findArrayFnBindings(expression);
    final Set<String> unknownIfArrays = new HashSet<>();

    if (columns.size() == 1) {
      final String column = Iterables.getOnlyElement(columns);
      final ColumnCapabilities capabilities = columnSelectorFactory.getColumnCapabilities(column);

      if (capabilities != null
          && capabilities.getType() == ValueType.STRING
          && capabilities.isDictionaryEncoded()
          && capabilities.isComplete()
          && !capabilities.hasMultipleValues()
          && !expectedArrays.contains(column)
      ) {
        // Optimization for dimension selectors that wrap a single underlying string column.
        return new SingleStringInputDimensionSelector(
            columnSelectorFactory.makeDimensionSelector(new DefaultDimensionSpec(column, column, ValueType.STRING)),
            expression
        );
      }
    }

    for (String column : columns) {
      final ColumnCapabilities capabilities = columnSelectorFactory.getColumnCapabilities(column);
      if (capabilities != null) {
        if (capabilities.hasMultipleValues()) {
          actualArrays.add(column);
        } else if (!capabilities.isComplete() && capabilities.getType().equals(ValueType.STRING) && (actualArrays.contains(column) || !expectedArrays.contains(column))) {
          unknownIfArrays.add(column);
        }
      } else {
        unknownIfArrays.add(column);
      }
    }

    final ColumnValueSelector<ExprEval> baseSelector = makeExprEvalSelector(columnSelectorFactory, expression);
    final boolean multiVal = actualArrays.size() > 0 || expectedArrays.size() > 0 || unknownIfArrays.size() > 0;

    if (baseSelector instanceof ConstantExprEvalSelector) {
      // Optimization for dimension selectors on constants.
      return DimensionSelector.constant(baseSelector.getObject().asString(), extractionFn);
    } else if (baseSelector instanceof NilColumnValueSelector) {
      // Optimization for null dimension selector.
      return DimensionSelector.constant(null);
    } else if (extractionFn == null) {

      if (multiVal) {
        class MultiValueDimensionSelector extends BaseMultiValueExpressionDimensionSelector
        {
          private MultiValueDimensionSelector()
          {
            super(baseSelector);
          }

          @Override
          String getValue(ExprEval evaluated)
          {
            assert !evaluated.isArray();
            return NullHandling.emptyToNullIfNeeded(evaluated.asString());
          }

          @Override
          List<String> getArray(ExprEval evaluated)
          {
            assert evaluated.isArray();
            return Arrays.stream(evaluated.asStringArray())
                         .map(NullHandling::emptyToNullIfNeeded)
                         .collect(Collectors.toList());
          }

          @Override
          String getArrayValue(ExprEval evaluated, int i)
          {
            assert evaluated.isArray();
            String[] stringArray = evaluated.asStringArray();
            assert i < stringArray.length;
            return NullHandling.emptyToNullIfNeeded(stringArray[i]);
          }
        }
        return new MultiValueDimensionSelector();
      } else {
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
      }
    } else {
      if (multiVal) {
        class ExtractionMultiValueDimensionSelector extends BaseMultiValueExpressionDimensionSelector
        {
          ExtractionMultiValueDimensionSelector()
          {
            super(baseSelector);
          }

          @Override
          String getValue(ExprEval evaluated)
          {
            assert !evaluated.isArray();
            return extractionFn.apply(NullHandling.emptyToNullIfNeeded(evaluated.asString()));
          }

          @Override
          List<String> getArray(ExprEval evaluated)
          {
            assert evaluated.isArray();
            return Arrays.stream(evaluated.asStringArray())
                         .map(x -> extractionFn.apply(NullHandling.emptyToNullIfNeeded(x)))
                         .collect(Collectors.toList());
          }

          @Override
          String getArrayValue(ExprEval evaluated, int i)
          {
            assert evaluated.isArray();
            String[] stringArray = evaluated.asStringArray();
            assert i < stringArray.length;
            return extractionFn.apply(NullHandling.emptyToNullIfNeeded(stringArray[i]));
          }

          @Override
          public void inspectRuntimeShape(RuntimeShapeInspector inspector)
          {
            inspector.visit("baseSelector", baseSelector);
            inspector.visit("extractionFn", extractionFn);
          }
        }
        return new ExtractionMultiValueDimensionSelector();

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
  }

  private static Expr.ObjectBinding createBindings(Expr expression, ColumnSelectorFactory columnSelectorFactory)
  {
    final Map<String, Supplier<Object>> suppliers = new HashMap<>();
    final List<String> columns = Parser.findRequiredBindings(expression);
    for (String columnName : columns) {
      final ColumnCapabilities columnCapabilities = columnSelectorFactory
          .getColumnCapabilities(columnName);
      final ValueType nativeType = columnCapabilities != null ? columnCapabilities.getType() : null;
      final boolean multiVal = columnCapabilities != null && columnCapabilities.hasMultipleValues();
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
            columnSelectorFactory.makeDimensionSelector(new DefaultDimensionSpec(columnName, columnName)),
            multiVal
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
    } else if (suppliers.size() == 1 && columns.size() == 1) {
      // If there's only one column (and it has a supplier), we can skip the Map and just use that supplier when
      // asked for something.
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
  static Supplier<Object> supplierFromDimensionSelector(final DimensionSelector selector, boolean multiValue)
  {
    Preconditions.checkNotNull(selector, "selector");
    return () -> {
      final IndexedInts row = selector.getRow();

      if (row.size() == 1 && !multiValue) {
        return selector.lookupName(row.get(0));
      } else {
        // column selector factories hate you and use [] and [null] interchangeably for nullish data
        if (row.size() == 0) {
          return new String[]{null};
        }
        final String[] strings = new String[row.size()];
        for (int i = 0; i < row.size(); i++) {
          strings[i] = selector.lookupName(row.get(i));
        }
        return strings;
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
        } else if (val instanceof List) {
          // strings can be lists of strings!!
          // this can happen from an "unknown" capabilites multi-value string dimension row, and we fallback to the
          // object selector
          Object[] arrayVal = ((List) val).stream().map(Object::toString).toArray(String[]::new);
          if (arrayVal.length > 0) {
            return arrayVal;
          }
          return new String[]{null};
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
