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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Maps;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprEval;
import io.druid.math.expr.Parser;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.IndexedInts;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

public class ExpressionObjectSelector implements ObjectColumnSelector<ExprEval>
{
  private final Expr expression;
  private final Expr.ObjectBinding bindings;

  private ExpressionObjectSelector(Expr.ObjectBinding bindings, Expr expression)
  {
    this.bindings = Preconditions.checkNotNull(bindings, "bindings");
    this.expression = Preconditions.checkNotNull(expression, "expression");
  }

  static ExpressionObjectSelector from(ColumnSelectorFactory columnSelectorFactory, Expr expression)
  {
    return new ExpressionObjectSelector(createBindings(columnSelectorFactory, expression), expression);
  }

  private static Expr.ObjectBinding createBindings(ColumnSelectorFactory columnSelectorFactory, Expr expression)
  {
    final Map<String, Supplier<Object>> suppliers = Maps.newHashMap();
    for (String columnName : Parser.findRequiredBindings(expression)) {
      final ColumnCapabilities columnCapabilities = columnSelectorFactory.getColumnCapabilities(columnName);
      final ValueType nativeType = columnCapabilities != null ? columnCapabilities.getType() : null;
      final Supplier<Object> supplier;

      if (nativeType == ValueType.FLOAT) {
        supplier = columnSelectorFactory.makeFloatColumnSelector(columnName)::get;
      } else if (nativeType == ValueType.LONG) {
        supplier = columnSelectorFactory.makeLongColumnSelector(columnName)::get;
      } else if (nativeType == ValueType.DOUBLE) {
        supplier = columnSelectorFactory.makeDoubleColumnSelector(columnName)::get;
      } else if (nativeType == ValueType.STRING) {
        supplier = supplierFromDimensionSelector(
            columnSelectorFactory.makeDimensionSelector(new DefaultDimensionSpec(columnName, columnName))
        );
      } else if (nativeType == null) {
        // Unknown ValueType. Try making an Object selector and see if that gives us anything useful.
        supplier = supplierFromObjectSelector(columnSelectorFactory.makeObjectColumnSelector(columnName));
      } else {
        // Unhandleable ValueType (COMPLEX).
        supplier = null;
      }

      if (supplier != null) {
        suppliers.put(columnName, supplier);
      }
    }

    return Parser.withSuppliers(suppliers);
  }

  @VisibleForTesting
  @Nonnull
  static Supplier<Object> supplierFromDimensionSelector(final DimensionSelector selector)
  {
    Preconditions.checkNotNull(selector, "selector");
    return () -> {
      final IndexedInts row = selector.getRow();
      if (row.size() == 0) {
        // Treat empty multi-value rows as nulls.
        return null;
      } else if (row.size() == 1) {
        return selector.lookupName(row.get(0));
      } else {
        // Can't handle multi-value rows in expressions.
        // Treat them as nulls until we think of something better to do.
        return null;
      }
    };
  }

  @VisibleForTesting
  @Nullable
  static Supplier<Object> supplierFromObjectSelector(final ObjectColumnSelector selector)
  {
    if (selector == null) {
      return null;
    }

    final Class<?> clazz = selector.classOfObject();
    if (Number.class.isAssignableFrom(clazz) || String.class.isAssignableFrom(clazz)) {
      // Number, String supported as-is.
      return selector::get;
    } else if (clazz.isAssignableFrom(Number.class) || clazz.isAssignableFrom(String.class)) {
      // Might be Numbers and Strings. Use a selector that double-checks.
      return () -> {
        final Object val = selector.get();
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

  @Override
  public Class<ExprEval> classOfObject()
  {
    return ExprEval.class;
  }

  @Override
  public ExprEval get()
  {
    return expression.eval(bindings);
  }
}
