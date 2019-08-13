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
import org.apache.druid.java.util.common.Pair;
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
        ExprEval eval = baseSelector.getObject();
        if (eval.isArray()) {
          return Arrays.stream(eval.asStringArray())
                .map(NullHandling::emptyToNullIfNeeded)
                .collect(Collectors.toList());
        }
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
    final Expr.BindingDetails exprDetails = expression.analyzeInputs();
    Parser.validateExpr(expression, exprDetails);
    final List<String> columns = exprDetails.getRequiredBindingsList();

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
                 && exprDetails.getArrayBindings().isEmpty()) {
        // Optimization for expressions that hit one scalar string column and nothing else.
        return new SingleStringInputCachingExpressionColumnValueSelector(
            columnSelectorFactory.makeDimensionSelector(new DefaultDimensionSpec(column, column, ValueType.STRING)),
            expression
        );
      }
    }

    final Pair<Set<String>, Set<String>> arrayUsage =
        examineColumnSelectorFactoryArrays(columnSelectorFactory, exprDetails, columns);
    final Set<String> actualArrays = arrayUsage.lhs;
    final Set<String> unknownIfArrays = arrayUsage.rhs;

    final List<String> needsApplied =
        columns.stream()
               .filter(c -> actualArrays.contains(c) && !exprDetails.getArrayBindings().contains(c))
               .collect(Collectors.toList());
    final Expr finalExpr;
    if (needsApplied.size() > 0) {
      finalExpr = Parser.applyUnappliedBindings(expression, exprDetails, needsApplied);
    } else {
      finalExpr = expression;
    }

    final Expr.ObjectBinding bindings = createBindings(exprDetails, columnSelectorFactory);

    if (bindings.equals(ExprUtils.nilBindings())) {
      // Optimization for constant expressions.
      return new ConstantExprEvalSelector(expression.eval(bindings));
    }

    // if any unknown column input types, fall back to an expression selector that examines input bindings on a
    // per row basis
    if (unknownIfArrays.size() > 0) {
      return new RowBasedExpressionColumnValueSelector(
          finalExpr,
          exprDetails,
          bindings,
          unknownIfArrays
      );
    }

    // generic expression value selector for fully known input types
    return new ExpressionColumnValueSelector(finalExpr, bindings);
  }

  /**
   * Makes a single or multi-value {@link DimensionSelector} wrapper around a {@link ColumnValueSelector} created by
   * {@link ExpressionSelectors#makeExprEvalSelector(ColumnSelectorFactory, Expr)} as appropriate
   */
  public static DimensionSelector makeDimensionSelector(
      final ColumnSelectorFactory columnSelectorFactory,
      final Expr expression,
      final ExtractionFn extractionFn
  )
  {
    final Expr.BindingDetails exprDetails = expression.analyzeInputs();
    Parser.validateExpr(expression, exprDetails);
    final List<String> columns = exprDetails.getRequiredBindingsList();


    if (columns.size() == 1) {
      final String column = Iterables.getOnlyElement(columns);
      final ColumnCapabilities capabilities = columnSelectorFactory.getColumnCapabilities(column);

      // Optimization for dimension selectors that wrap a single underlying string column.
      // The string column can be multi-valued, but if so, it must be implicitly mappable (i.e. the expression is
      // not treating it as an array, not wanting to output an array, and the multi-value dimension appears
      // exactly once).
      if (capabilities != null
          && capabilities.getType() == ValueType.STRING
          && capabilities.isDictionaryEncoded()
          && capabilities.isComplete()
          && !exprDetails.hasInputArrays()
          && !exprDetails.isOutputArray()
          // the following condition specifically is to handle the case of when a multi-value column identifier
          // appears more than once in an expression,
          // e.g. 'x + x' is fine if 'x' is scalar, but if 'x' is multi-value it should be translated to
          // 'cartesian_map((x_1, x_2) -> x_1 + x_2, x, x)'
          && (!capabilities.hasMultipleValues() || exprDetails.getFreeVariables().size() == 1)
      ) {
        return new SingleStringInputDimensionSelector(
            columnSelectorFactory.makeDimensionSelector(new DefaultDimensionSpec(column, column, ValueType.STRING)),
            expression
        );
      }
    }

    final Pair<Set<String>, Set<String>> arrayUsage =
        examineColumnSelectorFactoryArrays(columnSelectorFactory, exprDetails, columns);
    final Set<String> actualArrays = arrayUsage.lhs;
    final Set<String> unknownIfArrays = arrayUsage.rhs;


    final ColumnValueSelector<ExprEval> baseSelector = makeExprEvalSelector(columnSelectorFactory, expression);
    final boolean multiVal = actualArrays.size() > 0 ||
                             exprDetails.getArrayBindings().size() > 0 ||
                             unknownIfArrays.size() > 0;

    if (baseSelector instanceof ConstantExprEvalSelector) {
      // Optimization for dimension selectors on constants.
      return DimensionSelector.constant(baseSelector.getObject().asString(), extractionFn);
    } else if (baseSelector instanceof NilColumnValueSelector) {
      // Optimization for null dimension selector.
      return DimensionSelector.constant(null);
    } else if (extractionFn == null) {

      if (multiVal) {
        return new MultiValueExpressionDimensionSelector(baseSelector);
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
        class ExtractionMultiValueDimensionSelector extends MultiValueExpressionDimensionSelector
        {
          private ExtractionMultiValueDimensionSelector()
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

  /**
   * Create {@link Expr.ObjectBinding} given a {@link ColumnSelectorFactory} and {@link Expr.BindingDetails} which
   * provides the set of identifiers which need a binding (list of required columns), and context of whether or not they
   * are used as array or scalar inputs
   */
  private static Expr.ObjectBinding createBindings(
      Expr.BindingDetails bindingDetails,
      ColumnSelectorFactory columnSelectorFactory
  )
  {
    final Map<String, Supplier<Object>> suppliers = new HashMap<>();
    final List<String> columns = bindingDetails.getRequiredBindingsList();
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
          return coerceListDimToStringArray((List) val);
        } else {
          return null;
        }
      };
    } else if (clazz.isAssignableFrom(List.class)) {
      return () -> {
        final Object val = selector.getObject();
        if (val != null) {
          return coerceListDimToStringArray((List) val);
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
  private static Object coerceListDimToStringArray(List val)
  {
    Object[] arrayVal = val.stream().map(x -> x != null ? x.toString() : x).toArray(String[]::new);
    if (arrayVal.length > 0) {
      return arrayVal;
    }
    return new String[]{null};
  }

  /**
   * Returns pair of columns which are definitely multi-valued, or 'actual' arrays, and those which we are unable to
   * discern from the {@link ColumnSelectorFactory#getColumnCapabilities(String)}, or 'unknown' arrays.
   */
  private static Pair<Set<String>, Set<String>> examineColumnSelectorFactoryArrays(
      ColumnSelectorFactory columnSelectorFactory,
      Expr.BindingDetails exprDetails,
      List<String> columns
  )
  {
    final Set<String> actualArrays = new HashSet<>();
    final Set<String> unknownIfArrays = new HashSet<>();
    for (String column : columns) {
      final ColumnCapabilities capabilities = columnSelectorFactory.getColumnCapabilities(column);
      if (capabilities != null) {
        if (capabilities.hasMultipleValues()) {
          actualArrays.add(column);
        } else if (
            !capabilities.isComplete() &&
            capabilities.getType().equals(ValueType.STRING) &&
            !exprDetails.getArrayBindings().contains(column)
        ) {
          unknownIfArrays.add(column);
        }
      } else {
        unknownIfArrays.add(column);
      }
    }

    return new Pair<>(actualArrays, unknownIfArrays);
  }
}
