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
import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.RowIdSupplier;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;

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

    if (desiredType.is(ValueType.STRING)) {
      throw DruidException.defensive("Unexpected type[%s]", column);
    } else if (desiredType.isNumeric() && AggregatorUtil.shouldUseGetObjectForNumbers(column, columnSelectorFactory)) {
      final RowIdSupplier rowIdSupplier = columnSelectorFactory.getRowIdSupplier();

      switch (desiredType.getType()) {
        case LONG:
          return new ObjectToLongColumnValueSelector(selector, rowIdSupplier);

        case DOUBLE:
          return new ObjectToDoubleColumnValueSelector(selector, rowIdSupplier);

        case FLOAT:
          return new ObjectToFloatColumnValueSelector(selector, rowIdSupplier);

        default:
          throw DruidException.defensive("No implementation for desiredType[%s]", desiredType);
      }
    } else {
      return selector;
    }
  }
}
