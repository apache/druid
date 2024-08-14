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

package org.apache.druid.segment;

import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.Order;
import org.apache.druid.query.OrderBy;
import org.apache.druid.segment.column.ColumnHolder;

import java.util.Collections;
import java.util.List;

public class Cursors
{
  private static final List<OrderBy> TIME_ASCENDING_ORDER = Collections.singletonList(
      OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME)
  );

  private static final List<OrderBy> TIME_DESCENDING_ORDER = Collections.singletonList(
      OrderBy.descending(ColumnHolder.TIME_COLUMN_NAME)
  );

  /**
   * Check if the first {@link OrderBy} column of {@link CursorBuildSpec#getPreferredOrdering()} is
   * {@link Order#DESCENDING}, which allow {@link Cursor} on time ordered data to advance in descending order if
   * possible.
   */
  public static boolean preferDescendingTimeOrdering(CursorBuildSpec buildSpec)
  {
    final List<OrderBy> preferredOrdering = buildSpec.getPreferredOrdering();
    if (preferredOrdering != null && !preferredOrdering.isEmpty()) {
      final OrderBy orderBy = preferredOrdering.get(0);
      return ColumnHolder.TIME_COLUMN_NAME.equals(orderBy.getColumnName()) && Order.DESCENDING == orderBy.getOrder();
    }
    return false;
  }

  /**
   * Check if the first {@link OrderBy} of {@link CursorHolder#getOrdering()} is {@link ColumnHolder#TIME_COLUMN_NAME}
   * in the specified {@link Order}
   */
  public static boolean isTimeOrdered(CursorHolder holder, Order order)
  {
    final List<OrderBy> ordering = holder.getOrdering();
    if (ordering.isEmpty()) {
      return false;
    }
    final OrderBy orderBy = ordering.get(0);
    return ColumnHolder.TIME_COLUMN_NAME.equals(orderBy.getColumnName()) && orderBy.getOrder() == order;
  }

  /**
   * Require the first {@link OrderBy} of {@link CursorHolder#getOrdering()} is {@link ColumnHolder#TIME_COLUMN_NAME}.
   * Throws {@link DruidException} if the order does
   */
  public static void requireTimeOrdering(CursorHolder holder, Order order)
  {
    if (!isTimeOrdered(holder, order)) {
      final String failureReason = StringUtils.format(
          "Cursor must be ordered by [%s] with direction [%s] but was [%s] instead.",
          ColumnHolder.TIME_COLUMN_NAME,
          order,
          holder.getOrdering()
      );
      holder.close();
      throw DruidException.defensive(failureReason);
    }
  }

  /**
   * Get a {@link CursorHolder} {@link OrderBy} list that contains only a {@link ColumnHolder#TIME_COLUMN_NAME} as
   * {@link Order#ASCENDING}, classic Druid segment order.
   */
  public static List<OrderBy> ascendingTimeOrder()
  {
    return TIME_ASCENDING_ORDER;
  }

  /**
   * Get a {@link CursorHolder} {@link OrderBy} list that contains only a {@link ColumnHolder#TIME_COLUMN_NAME} as
   * {@link Order#DESCENDING}, classic Druid segment order in reverse.
   */
  public static List<OrderBy> descendingTimeOrder()
  {
    return TIME_DESCENDING_ORDER;
  }
}
