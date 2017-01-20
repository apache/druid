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
import com.google.common.primitives.Doubles;
import io.druid.common.guava.GuavaUtils;
import io.druid.math.expr.Expr;
import io.druid.math.expr.Parser;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ValueType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

public class ExpressionObjectSelector implements ObjectColumnSelector<Number>
{
  private final Expr expression;
  private final Expr.ObjectBinding bindings;

  private ExpressionObjectSelector(Expr.ObjectBinding bindings, Expr expression)
  {
    this.bindings = Preconditions.checkNotNull(bindings, "bindings");
    this.expression = Preconditions.checkNotNull(expression, "expression");
  }

  public static ExpressionObjectSelector from(ColumnSelectorFactory columnSelectorFactory, Expr expression)
  {
    return new ExpressionObjectSelector(createBindings(columnSelectorFactory, expression), expression);
  }

  private static Expr.ObjectBinding createBindings(ColumnSelectorFactory columnSelectorFactory, Expr expression)
  {
    final Map<String, Supplier<Number>> suppliers = Maps.newHashMap();
    for (String columnName : Parser.findRequiredBindings(expression)) {
      final ColumnCapabilities columnCapabilities = columnSelectorFactory.getColumnCapabilities(columnName);
      final ValueType nativeType = columnCapabilities != null ? columnCapabilities.getType() : null;
      final Supplier<Number> supplier;

      if (nativeType == ValueType.FLOAT) {
        supplier = supplierFromFloatSelector(columnSelectorFactory.makeFloatColumnSelector(columnName));
      } else if (nativeType == ValueType.LONG) {
        supplier = supplierFromLongSelector(columnSelectorFactory.makeLongColumnSelector(columnName));
      } else if (nativeType == null) {
        // Unknown ValueType. Try making an Object selector and see if that gives us anything useful.
        supplier = supplierFromObjectSelector(columnSelectorFactory.makeObjectColumnSelector(columnName));
      } else {
        // Unhandleable ValueType (possibly STRING or COMPLEX).
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
  static Supplier<Number> supplierFromFloatSelector(final FloatColumnSelector selector)
  {
    Preconditions.checkNotNull(selector, "selector");
    return new Supplier<Number>()
    {
      @Override
      public Number get()
      {
        return selector.get();
      }
    };
  }

  @VisibleForTesting
  @Nonnull
  static Supplier<Number> supplierFromLongSelector(final LongColumnSelector selector)
  {
    Preconditions.checkNotNull(selector, "selector");
    return new Supplier<Number>()
    {
      @Override
      public Number get()
      {
        return selector.get();
      }
    };
  }

  @VisibleForTesting
  @Nullable
  static Supplier<Number> supplierFromObjectSelector(final ObjectColumnSelector selector)
  {
    final Class<?> clazz = selector == null ? null : selector.classOfObject();
    if (selector != null && (clazz.isAssignableFrom(Number.class)
                             || clazz.isAssignableFrom(String.class)
                             || Number.class.isAssignableFrom(clazz))) {
      // There may be numbers here.
      return new Supplier<Number>()
      {
        @Override
        public Number get()
        {
          return tryParse(selector.get());
        }
      };
    } else {
      // We know there are no numbers here. Use a null supplier.
      return null;
    }
  }

  @Nullable
  private static Number tryParse(final Object value)
  {
    if (value == null) {
      return null;
    }

    if (value instanceof Number) {
      return (Number) value;
    }

    final String stringValue = String.valueOf(value);
    final Long longValue = GuavaUtils.tryParseLong(stringValue);
    if (longValue != null) {
      return longValue;
    }

    final Double doubleValue = Doubles.tryParse(stringValue);
    if (doubleValue != null) {
      return doubleValue;
    }

    return null;
  }

  @Override
  public Class<Number> classOfObject()
  {
    return Number.class;
  }

  @Override
  public Number get()
  {
    return expression.eval(bindings).numericValue();
  }
}
