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
import org.apache.druid.java.util.common.NonnullPair;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.math.expr.Evals;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionProcessing;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.InputBindings;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.ConstantExprEvalSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.NilColumnValueSelector;
import org.apache.druid.segment.RowIdSupplier;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
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
        return eval.value();
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

  public static ColumnValueSelector makeStringColumnValueSelector(
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
        return coerceEvalToObjectOrList(eval);
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
    final RowIdSupplier rowIdSupplier = columnSelectorFactory.getRowIdSupplier();

    if (plan.is(ExpressionPlan.Trait.SINGLE_INPUT_SCALAR)) {
      final String column = plan.getSingleInputName();
      final ColumnType inputType = plan.getSingleInputType();
      if (inputType.is(ValueType.LONG)) {
        return new SingleLongInputCachingExpressionColumnValueSelector(
            columnSelectorFactory.makeColumnValueSelector(column),
            plan.getExpression(),
            !ColumnHolder.TIME_COLUMN_NAME.equals(column), // __time doesn't need an LRU cache since it is sorted.
            rowIdSupplier
        );
      } else if (inputType.is(ValueType.STRING)) {
        return new SingleStringInputCachingExpressionColumnValueSelector(
            columnSelectorFactory.makeDimensionSelector(new DefaultDimensionSpec(column, column, ColumnType.STRING)),
            plan.getExpression(),
            rowIdSupplier
        );
      }
    }
    final Expr.ObjectBinding bindings = createBindings(columnSelectorFactory, plan);

    // Optimization for constant expressions
    if (bindings.equals(InputBindings.nilBindings())) {
      return new ConstantExprEvalSelector(plan.getExpression().eval(bindings));
    }

    // if any unknown column input types, fall back to an expression selector that examines input bindings on a
    // per row basis
    if (plan.any(ExpressionPlan.Trait.UNKNOWN_INPUTS, ExpressionPlan.Trait.INCOMPLETE_INPUTS)) {
      return new RowBasedExpressionColumnValueSelector(plan, bindings, rowIdSupplier);
    }

    // generic expression value selector for fully known input types
    return new ExpressionColumnValueSelector(plan.getAppliedExpression(), bindings, rowIdSupplier);
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

    if (plan.any(ExpressionPlan.Trait.SINGLE_INPUT_SCALAR, ExpressionPlan.Trait.SINGLE_INPUT_MAPPABLE)) {
      final String column = plan.getSingleInputName();
      if (plan.getSingleInputType().is(ValueType.STRING)) {
        return new SingleStringInputDeferredEvaluationExpressionDimensionSelector(
            columnSelectorFactory.makeDimensionSelector(DefaultDimensionSpec.of(column)),
            expression
        );
      }
    }

    final ColumnValueSelector<ExprEval> baseSelector = makeExprEvalSelector(columnSelectorFactory, expression);

    if (baseSelector instanceof ConstantExprEvalSelector) {
      // Optimization for dimension selectors on constants.
      if (plan.is(ExpressionPlan.Trait.NON_SCALAR_OUTPUT)) {
        final Object[] value = baseSelector.getObject().asArray();
        final List<String> stringList;
        if (value != null) {
          stringList = Arrays.stream(value).map(Evals::asString).collect(Collectors.toList());
        } else {
          stringList = null;
        }
        return DimensionSelector.multiConstant(stringList, extractionFn);
      }
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
   * @param columnCapabilities    {@link ColumnCapabilities} for the input binding
   */
  public static boolean canMapOverDictionary(
      final Expr.BindingAnalysis bindingAnalysis,
      final ColumnCapabilities columnCapabilities
  )
  {
    Preconditions.checkState(bindingAnalysis.getRequiredBindings().size() == 1, "requiredBindings.size == 1");
    return columnCapabilities != null &&
           !columnCapabilities.hasMultipleValues().isUnknown() &&
           !bindingAnalysis.hasInputArrays() &&
           !bindingAnalysis.isOutputArray();
  }

  /**
   * Create {@link Expr.ObjectBinding} given a {@link ColumnSelectorFactory} and {@link ExpressionPlan} which
   * provides the set of identifiers which need a binding (list of required columns), and context of whether or not they
   * are used as array or scalar inputs
   */
  public static Expr.ObjectBinding createBindings(
      ColumnSelectorFactory columnSelectorFactory,
      ExpressionPlan plan
  )
  {
    final List<String> columns = plan.getAnalysis().getRequiredBindingsList();
    final Map<String, Pair<ExpressionType, Supplier<Object>>> suppliers = new HashMap<>();
    for (String columnName : columns) {
      final ColumnCapabilities capabilities = columnSelectorFactory.getColumnCapabilities(columnName);
      final boolean multiVal = capabilities != null && capabilities.hasMultipleValues().isTrue();
      final Supplier<Object> supplier;
      final ExpressionType expressionType = ExpressionType.fromColumnType(capabilities);

      final boolean useObjectSupplierForMultiValueStringArray =
          capabilities != null
          // if homogenizing null multi-value string arrays, or if a single valued function that must be applied across
          // multi-value rows, we can just use the dimension selector, which has the homogenization behavior built-in
          && ((!capabilities.is(ValueType.STRING))
              || (capabilities.is(ValueType.STRING)
                  && !ExpressionProcessing.isHomogenizeNullMultiValueStringArrays()
                  && !plan.is(ExpressionPlan.Trait.NEEDS_APPLIED)
              )
          )
          // expression has array output
          && plan.is(ExpressionPlan.Trait.NON_SCALAR_OUTPUT);

      final boolean homogenizeNullMultiValueStringArrays =
          plan.is(ExpressionPlan.Trait.NEEDS_APPLIED) || ExpressionProcessing.isHomogenizeNullMultiValueStringArrays();

      if (capabilities == null || capabilities.isArray() || useObjectSupplierForMultiValueStringArray) {
        // Unknown type, array type, or output array uses an Object selector and see if that gives anything useful
        supplier = supplierFromObjectSelector(
            columnSelectorFactory.makeColumnValueSelector(columnName),
            homogenizeNullMultiValueStringArrays
        );
      } else if (capabilities.is(ValueType.FLOAT)) {
        ColumnValueSelector<?> selector = columnSelectorFactory.makeColumnValueSelector(columnName);
        supplier = makeNullableNumericSupplier(selector, selector::getFloat);
      } else if (capabilities.is(ValueType.LONG)) {
        ColumnValueSelector<?> selector = columnSelectorFactory.makeColumnValueSelector(columnName);
        supplier = makeNullableNumericSupplier(selector, selector::getLong);
      } else if (capabilities.is(ValueType.DOUBLE)) {
        ColumnValueSelector<?> selector = columnSelectorFactory.makeColumnValueSelector(columnName);
        supplier = makeNullableNumericSupplier(selector, selector::getDouble);
      } else if (capabilities.is(ValueType.STRING)) {
        supplier = supplierFromDimensionSelector(
            columnSelectorFactory.makeDimensionSelector(new DefaultDimensionSpec(columnName, columnName)),
            multiVal,
            homogenizeNullMultiValueStringArrays
        );
      } else {
        // complex type just pass straight through
        ColumnValueSelector<?> selector = columnSelectorFactory.makeColumnValueSelector(columnName);
        if (!(selector instanceof NilColumnValueSelector)) {
          supplier = selector::getObject;
        } else {
          supplier = null;
        }
      }

      if (supplier != null) {
        suppliers.put(columnName, new Pair<>(expressionType, supplier));
      }
    }

    if (suppliers.isEmpty()) {
      return InputBindings.nilBindings();
    } else if (suppliers.size() == 1 && columns.size() == 1) {
      // If there's only one column (and it has a supplier), we can skip the Map and just use that supplier when
      // asked for something.
      final String column = Iterables.getOnlyElement(suppliers.keySet());
      final Pair<ExpressionType, Supplier<Object>> supplier = Iterables.getOnlyElement(suppliers.values());

      return new Expr.ObjectBinding()
      {
        @Nullable
        @Override
        public Object get(String name)
        {
          // There's only one binding, and it must be the single column, so it can safely be ignored in production.
          assert column.equals(name);
          return supplier.rhs.get();
        }

        @Nullable
        @Override
        public ExpressionType getType(String name)
        {
          return supplier.lhs;
        }
      };
    } else {
      return InputBindings.withTypedSuppliers(suppliers);
    }
  }

  /**
   * Wraps a {@link ColumnValueSelector} and uses it to supply numeric values in a null-aware way.
   *
   * @see org.apache.druid.segment.BaseNullableColumnValueSelector#isNull() for why this only works in the numeric case
   */
  @VisibleForTesting
  public static <T> Supplier<T> makeNullableNumericSupplier(
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
  static Supplier<Object> supplierFromDimensionSelector(final DimensionSelector selector, boolean coerceArray, boolean homogenize)
  {
    Preconditions.checkNotNull(selector, "selector");
    return () -> {
      final IndexedInts row = selector.getRow();

      if (row.size() == 1 && !coerceArray) {
        return selector.lookupName(row.get(0));
      } else {
        // column selector factories hate you and use [] and [null] interchangeably for nullish data
        if (row.size() == 0 || (row.size() == 1 && selector.getObject() == null)) {
          if (homogenize) {
            return new Object[]{null};
          } else {
            return null;
          }
        }
        final Object[] strings = new Object[row.size()];
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
  static Supplier<Object> supplierFromObjectSelector(
      final BaseObjectColumnValueSelector<?> selector,
      boolean homogenizeMultiValue
  )
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
        if (val instanceof List) {
          NonnullPair<ExpressionType, Object[]> coerced = ExprEval.coerceListToArray((List) val, homogenizeMultiValue);
          if (coerced == null) {
            return null;
          }
          return coerced.rhs;
        } else {
          return val;
        }
      };
    } else if (clazz.isAssignableFrom(List.class)) {
      return () -> {
        final Object val = selector.getObject();
        if (val != null) {
          NonnullPair<ExpressionType, Object[]> coerced = ExprEval.coerceListToArray((List) val, homogenizeMultiValue);
          if (coerced == null) {
            return null;
          }
          return coerced.rhs;
        }
        return null;
      };
    } else {
      // No numbers or strings, just pass it through
      return selector::getObject;
    }
  }

  /**
   * Coerces {@link ExprEval} value back to a {@link ColumnType#STRING} selector friendly value, converting into:
   *    - the expression value if the value is not an array
   *    - the single array element if the value is an array with 1 element
   *    - a list with all of the array elements if the value is an array with more than 1 element
   * This method is used by {@link #makeStringColumnValueSelector(ColumnSelectorFactory, Expr)}, which is used
   * exclusively for making {@link ColumnValueSelector} when an {@link ExpressionVirtualColumn} has STRING output type,
   * and by {@link org.apache.druid.segment.transform.ExpressionTransform} which should be reconsidered if we ever
   * want to add support for ingestion transforms producing {@link ValueType#ARRAY} typed outputs.
   */
  @Nullable
  public static Object coerceEvalToObjectOrList(ExprEval eval)
  {
    if (eval.type().isArray()) {
      final Object[] asArray = eval.asArray();
      if (asArray == null) {
        return null;
      }
      if (asArray.length == 1) {
        return asArray[0];
      }
      return Arrays.stream(asArray).collect(Collectors.toList());
    }
    return eval.value();
  }
}
