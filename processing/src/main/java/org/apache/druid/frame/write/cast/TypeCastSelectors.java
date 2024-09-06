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

package org.apache.druid.frame.write.cast;

import org.apache.druid.error.DruidException;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.RowIdSupplier;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;

import javax.annotation.Nullable;

public class TypeCastSelectors
{
  /**
   * Create a {@link ColumnValueSelector} that does its own typecasting if necessary. If typecasting is not necessary,
   * returns a selector directly from the underlying {@link ColumnSelectorFactory}.
   *
   * @param columnSelectorFactory underlying factory
   * @param column                column name
   * @param desiredType           desired type of selector. Can be anything except {@link ColumnType#STRING}.
   *                              For strings, use {@link DimensionSelector} rather than {@link ColumnValueSelector}.
   */
  public static ColumnValueSelector<?> makeColumnValueSelector(
      final ColumnSelectorFactory columnSelectorFactory,
      final String column,
      final ColumnType desiredType
  )
  {
    final ColumnValueSelector<?> selector = columnSelectorFactory.makeColumnValueSelector(column);
    final ColumnCapabilities selectorCapabilities = columnSelectorFactory.getColumnCapabilities(column);

    return wrapColumnValueSelectorIfNeeded(
        selector,
        selectorCapabilities,
        columnSelectorFactory.getRowIdSupplier(),
        desiredType
    );
  }

  /**
   * Wraps a {@link ColumnValueSelector} with a type casting selector if necessary. If typecasting is not necessary,
   * returns the original selector.
   *
   * @param selector             selector
   * @param selectorCapabilities capabilities for the selector, from {@link ColumnSelectorFactory#getColumnCapabilities}
   * @param rowIdSupplier        row id supplier, from {@link ColumnSelectorFactory#getRowIdSupplier()}
   * @param desiredType          desired type for the returned selector
   */
  public static ColumnValueSelector<?> wrapColumnValueSelectorIfNeeded(
      final ColumnValueSelector<?> selector,
      @Nullable final ColumnCapabilities selectorCapabilities,
      @Nullable final RowIdSupplier rowIdSupplier,
      final ColumnType desiredType
  )
  {
    final ExpressionType desiredExpressionType = ExpressionType.fromColumnType(desiredType);

    if (desiredType.is(ValueType.STRING)) {
      throw DruidException.defensive("Type[%s] should be read using a DimensionSelector", desiredType);
    } else if (desiredType.isNumeric()
               && (selectorCapabilities == null || !selectorCapabilities.isNumeric())) {
      // When capabilities are unknown, or known to be non-numeric, fall back to getObject() and explicit typecasting.
      // This avoids using primitive numeric accessors (getLong / getDouble / getFloat / isNull) on a selector that
      // may not support them.
      return new ObjectToNumberColumnValueSelector(selector, desiredExpressionType, rowIdSupplier);
    } else if (desiredType.isArray()) {
      // Always wrap if desiredType is an array. Even if the underlying selector claims to offer the same type as
      // desiredType, it may fail to respect the BaseObjectColumnValueSelector contract. For example, it may return
      // List rather than Object[]. (RowBasedColumnSelectorFactory can do this if used incorrectly, i.e., if the
      // ColumnInspector declares type ARRAY<X> for a column, but the RowAdapter does not provide Object[].)
      return new ObjectToArrayColumnValueSelector(selector, desiredExpressionType, rowIdSupplier);
    } else {
      // OK to return the original selector.
      return selector;
    }
  }

  /**
   * Coerce an object to an object compatible with what {@link BaseObjectColumnValueSelector#getObject()} for a column
   * of the provided desiredType.
   *
   * @param obj         object
   * @param desiredType desired type
   */
  @Nullable
  public static Object bestEffortCoerce(
      @Nullable final Object obj,
      @Nullable final ExpressionType desiredType
  )
  {
    if (obj == null || desiredType == null) {
      return obj;
    }

    return ExprEval.ofType(desiredType, obj).value();
  }
}
