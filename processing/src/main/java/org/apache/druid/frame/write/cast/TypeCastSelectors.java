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

  public static ColumnValueSelector<?> wrapColumnValueSelectorIfNeeded(
      final ColumnValueSelector<?> selector,
      @Nullable final ColumnCapabilities selectorCapabilities,
      @Nullable final RowIdSupplier rowIdSupplier,
      final ColumnType desiredType
  )
  {
    if (desiredType.is(ValueType.STRING)) {
      throw DruidException.defensive("Type[%s] should be read using a DimensionSelector", desiredType);
    } else if (desiredType.isNumeric()
               && (selectorCapabilities == null || !selectorCapabilities.isNumeric())) {
      // When capabilities are unknown, or known to be non-numeric, fall back to getObject() and explicit typecasting.
      // This avoids using primitive numeric accessors (getLong / getDouble / getFloat / isNull) on a selector that
      // may not support them.
      final ColumnType selectorType = selectorCapabilities != null ? selectorCapabilities.toColumnType() : null;

      switch (desiredType.getType()) {
        case LONG:
          return new ObjectToLongColumnValueSelector(selector, selectorType, rowIdSupplier);

        case DOUBLE:
          return new ObjectToDoubleColumnValueSelector(selector, selectorType, rowIdSupplier);

        case FLOAT:
          return new ObjectToFloatColumnValueSelector(selector, selectorType, rowIdSupplier);

        default:
          throw DruidException.defensive("Unexpected numeric desiredType[%s]", desiredType);
      }
    } else if (desiredType.isArray()
               && (selectorCapabilities == null || !selectorCapabilities.toColumnType().equals(desiredType))) {
      // When reading arrays, wrap if the underlying type does not match the desired array type.
      final ColumnType columnType = selectorCapabilities != null ? selectorCapabilities.toColumnType() : null;
      return new ObjectEvalColumnValueSelector(selector, columnType, desiredType, rowIdSupplier);
    } else {
      // OK to return the original selector.
      return selector;
    }
  }
}
