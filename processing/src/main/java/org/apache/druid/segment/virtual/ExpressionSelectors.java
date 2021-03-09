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

import org.apache.druid.com.google.common.annotations.VisibleForTesting;
import org.apache.druid.com.google.common.base.Preconditions;
import org.apache.druid.com.google.common.base.Supplier;
import org.apache.druid.com.google.common.collect.Iterables;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.UOE;
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
import org.apache.druid.segment.NilColumnValueSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.IndexedInts;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
        ExprEval eval = baseSelector.getObject();
        return coerceEvalToSelectorObject(eval);
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
    return makeExprEvalSelector(columnSelectorFactory, ExpressionPlanner.plan(columnSelectorFactory, expression));
  }

  public static ColumnValueSelector<ExprEval> makeExprEvalSelector(
      ColumnSelectorFactory columnSelectorFactory,
      ExpressionPlan plan
  )
  {
    if (plan.is(ExpressionPlan.Trait.SINGLE_INPUT_SCALAR)) {
      final String column = plan.getSingleInputName();
      final ValueType inputType = plan.getSingleInputType();
      if (inputType == ValueType.LONG) {
        return new SingleLongInputCachingExpressionColumnValueSelector(
            columnSelectorFactory.makeColumnValueSelector(column),
            plan.getExpression(),
            !ColumnHolder.TIME_COLUMN_NAME.equals(column) // __time doesn't need an LRU cache since it is sorted.
        );
      } else if (inputType == ValueType.STRING) {
        return new SingleStringInputCachingExpressionColumnValueSelector(
            columnSelectorFactory.makeDimensionSelector(new DefaultDimensionSpec(column, column, ValueType.STRING)),
            plan.getExpression()
        );
      }
    }
    final Expr.ObjectBinding bindings = createBindings(plan.getAnalysis(), columnSelectorFactory);

    // Optimization for constant expressions
    if (bindings.equals(ExprUtils.nilBindings())) {
      return new ConstantExprEvalSelector(plan.getExpression().eval(bindings));
    }

    // if any unknown column input types, fall back to an expression selector that examines input bindings on a
    // per row basis
    if (plan.any(ExpressionPlan.Trait.UNKNOWN_INPUTS, ExpressionPlan.Trait.INCOMPLETE_INPUTS)) {
      return new RowBasedExpressionColumnValueSelector(plan, bindings);
    }

    // generic expression value selector for fully known input types
    return new ExpressionColumnValueSelector(plan.getAppliedExpression(), bindings);
  }

  /**
   * Makes a single or multi-value {@link DimensionSelector} wrapper around a {@link ColumnValueSelector} created by
   * {@link ExpressionSelectors#makeExprEvalSelector(ColumnSelectorFactory, Expr)} as appropriate
   */
  public static DimensionSelector makeDimensionSelector(
      final ColumnSelectorFactory columnSelectorFactory,
      final Expr expression,
      @Nullable final ExtractionFn extractionFn
  )
  {
    final ExpressionPlan plan = ExpressionPlanner.plan(columnSelectorFactory, expression);

    if (plan.is(ExpressionPlan.Trait.SINGLE_INPUT_MAPPABLE)) {
      final String column = plan.getSingleInputName();
      if (plan.getSingleInputType() == ValueType.STRING) {
        return new SingleStringInputDimensionSelector(
            columnSelectorFactory.makeDimensionSelector(DefaultDimensionSpec.of(column)),
            expression
        );
      }
    }

    final ColumnValueSelector<ExprEval> baseSelector = makeExprEvalSelector(columnSelectorFactory, expression);

    if (baseSelector instanceof ConstantExprEvalSelector) {
      // Optimization for dimension selectors on constants.
      return DimensionSelector.constant(baseSelector.getObject().asString(), extractionFn);
    } else if (baseSelector instanceof NilColumnValueSelector) {
      // Optimization for null dimension selector.
      return DimensionSelector.constant(null);
    } else {
      if (plan.any(
          ExpressionPlan.Trait.NON_SCALAR_OUTPUT,
          ExpressionPlan.Trait.NEEDS_APPLIED,
          ExpressionPlan.Trait.UNKNOWN_INPUTS,
          ExpressionPlan.Trait.INCOMPLETE_INPUTS
      )) {
        return ExpressionMultiValueDimensionSelector.fromValueSelector(baseSelector, extractionFn);
      } else {
        return ExpressionSingleValueDimensionSelector.fromValueSelector(baseSelector, extractionFn);
      }
    }
  }


  /**
   * Returns whether an expression can be applied to unique values of a particular column (like those in a dictionary)
   * rather than being applied to each row individually.
   *
   * This function should only be called if you have already determined that an expression is over a single column,
   * and that single column has a dictionary.
   *
   * @param bindingAnalysis       result of calling {@link Expr#analyzeInputs()} on an expression
   * @param hasMultipleValues result of calling {@link ColumnCapabilities#hasMultipleValues()}
   */
  public static boolean canMapOverDictionary(
      final Expr.BindingAnalysis bindingAnalysis,
      final ColumnCapabilities.Capable hasMultipleValues
  )
  {
    Preconditions.checkState(bindingAnalysis.getRequiredBindings().size() == 1, "requiredBindings.size == 1");
    return !hasMultipleValues.isUnknown() && !bindingAnalysis.hasInputArrays() && !bindingAnalysis.isOutputArray();
  }

  /**
   * Create {@link Expr.ObjectBinding} given a {@link ColumnSelectorFactory} and {@link Expr.BindingAnalysis} which
   * provides the set of identifiers which need a binding (list of required columns), and context of whether or not they
   * are used as array or scalar inputs
   */
  private static Expr.ObjectBinding createBindings(
      Expr.BindingAnalysis bindingAnalysis,
      ColumnSelectorFactory columnSelectorFactory
  )
  {
    final Map<String, Supplier<Object>> suppliers = new HashMap<>();
    final List<String> columns = bindingAnalysis.getRequiredBindingsList();
    for (String columnName : columns) {
      final ColumnCapabilities columnCapabilities = columnSelectorFactory.getColumnCapabilities(columnName);
      final ValueType nativeType = columnCapabilities != null ? columnCapabilities.getType() : null;
      final boolean multiVal = columnCapabilities != null && columnCapabilities.hasMultipleValues().isTrue();
      final Supplier<Object> supplier;

      if (nativeType == ValueType.FLOAT) {
        ColumnValueSelector<?> selector = columnSelectorFactory.makeColumnValueSelector(columnName);
        supplier = makeNullableNumericSupplier(selector, selector::getFloat);
      } else if (nativeType == ValueType.LONG) {
        ColumnValueSelector<?> selector = columnSelectorFactory.makeColumnValueSelector(columnName);
        supplier = makeNullableNumericSupplier(selector, selector::getLong);
      } else if (nativeType == ValueType.DOUBLE) {
        ColumnValueSelector<?> selector = columnSelectorFactory.makeColumnValueSelector(columnName);
        supplier = makeNullableNumericSupplier(selector, selector::getDouble);
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

  /**
   * Wraps a {@link ColumnValueSelector} and uses it to supply numeric values in a null-aware way.
   *
   * @see org.apache.druid.segment.BaseNullableColumnValueSelector#isNull() for why this only works in the numeric case
   */
  private static <T> Supplier<T> makeNullableNumericSupplier(
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

  /**
   * Create a supplier to feed {@link Expr.ObjectBinding} for a dimension selector, coercing values to always appear as
   * arrays if specified.
   */
  @VisibleForTesting
  static Supplier<Object> supplierFromDimensionSelector(final DimensionSelector selector, boolean coerceArray)
  {
    Preconditions.checkNotNull(selector, "selector");
    return () -> {
      final IndexedInts row = selector.getRow();

      if (row.size() == 1 && !coerceArray) {
        return selector.lookupName(row.get(0));
      } else {
        // column selector factories hate you and use [] and [null] interchangeably for nullish data
        if (row.size() == 0) {
          return new String[]{null};
        }
        final String[] strings = new String[row.size()];
        // noinspection SSBasedInspection
        for (int i = 0; i < row.size(); i++) {
          strings[i] = selector.lookupName(row.get(i));
        }
        return strings;
      }
    };
  }


  /**
   * Create a fallback supplier to feed {@link Expr.ObjectBinding} for a selector, used if column cannot be reliably
   * detected as a primitive type
   */
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
          return coerceListToArray((List) val);
        } else {
          return null;
        }
      };
    } else if (clazz.isAssignableFrom(List.class)) {
      return () -> {
        final Object val = selector.getObject();
        if (val != null) {
          return coerceListToArray((List) val);
        }
        return null;
      };
    } else {
      // No numbers or strings.
      return null;
    }
  }

  /**
   * Selectors are not consistent in treatment of null, [], and [null], so coerce [] to [null]
   */
  public static Object coerceListToArray(@Nullable List<?> val)
  {
    if (val != null && val.size() > 0) {
      Class coercedType = null;

      for (Object elem : val) {
        if (elem != null) {
          coercedType = convertType(coercedType, elem.getClass());
        }
      }

      if (coercedType == Long.class || coercedType == Integer.class) {
        return val.stream().map(x -> x != null ? ((Number) x).longValue() : null).toArray(Long[]::new);
      }
      if (coercedType == Float.class || coercedType == Double.class) {
        return val.stream().map(x -> x != null ? ((Number) x).doubleValue() : null).toArray(Double[]::new);
      }
      // default to string
      return val.stream().map(x -> x != null ? x.toString() : null).toArray(String[]::new);
    }
    return new String[]{null};
  }

  private static Class convertType(@Nullable Class existing, Class next)
  {
    if (Number.class.isAssignableFrom(next) || next == String.class) {
      if (existing == null) {
        return next;
      }
      // string wins everything
      if (existing == String.class) {
        return existing;
      }
      if (next == String.class) {
        return next;
      }
      // all numbers win over Integer
      if (existing == Integer.class) {
        return next;
      }
      if (existing == Float.class) {
        // doubles win over floats
        if (next == Double.class) {
          return next;
        }
        return existing;
      }
      if (existing == Long.class) {
        if (next == Integer.class) {
          // long beats int
          return existing;
        }
        // double and float win over longs
        return next;
      }
      // otherwise double
      return Double.class;
    }
    throw new UOE("Invalid array expression type: %s", next);
  }

  /**
   * Coerces {@link ExprEval} value back to selector friendly {@link List} if the evaluated expression result is an
   * array type
   */
  @Nullable
  public static Object coerceEvalToSelectorObject(ExprEval eval)
  {
    switch (eval.type()) {
      case STRING_ARRAY:
        return Arrays.stream(eval.asStringArray()).collect(Collectors.toList());
      case DOUBLE_ARRAY:
        return Arrays.stream(eval.asDoubleArray()).collect(Collectors.toList());
      case LONG_ARRAY:
        return Arrays.stream(eval.asLongArray()).collect(Collectors.toList());
      default:
        return eval.value();
    }
  }
}
