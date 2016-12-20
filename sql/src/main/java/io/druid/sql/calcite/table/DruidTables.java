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

package io.druid.sql.calcite.table;

import com.google.common.collect.Lists;
import io.druid.query.ordering.StringComparator;
import io.druid.query.ordering.StringComparators;
import io.druid.segment.column.ValueType;
import io.druid.sql.calcite.expression.RowExtraction;

import java.util.List;

public class DruidTables
{
  private DruidTables()
  {
    // No instantiation.
  }

  /**
   * Returns the "natural" rowOrder for a Druid table. This is the order that a scan without projection would return.
   *
   * @param druidTable druid table
   *
   * @return natural row order
   */
  public static List<String> rowOrder(
      final DruidTable druidTable
  )
  {
    final List<String> rowOrder = Lists.newArrayListWithCapacity(druidTable.getColumnCount());
    for (int i = 0; i < druidTable.getColumnCount(); i++) {
      rowOrder.add(druidTable.getColumnName(i));
    }
    return rowOrder;
  }

  /**
   * Return the "natural" {@link StringComparator} for an extraction from a Druid table. This will be a lexicographic
   * comparator for String types and a numeric comparator for Number types.
   *
   * @param druidTable    underlying Druid table
   * @param rowExtraction extraction from the table
   *
   * @return natural comparator
   */
  public static StringComparator naturalStringComparator(
      final DruidTable druidTable,
      final RowExtraction rowExtraction
  )
  {
    if (rowExtraction.getExtractionFn() != null
        || druidTable.getColumnType(druidTable.getColumnNumber(rowExtraction.getColumn())) == ValueType.STRING) {
      return StringComparators.LEXICOGRAPHIC;
    } else {
      return StringComparators.NUMERIC;
    }
  }
}
