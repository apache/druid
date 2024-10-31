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

import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.Order;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.vector.VectorCursor;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.Collections;
import java.util.List;

public interface CursorHolder extends Closeable
{
  /**
   * Create a {@link Cursor} for use with non-vectorized query engines.
   */
  @Nullable
  Cursor asCursor();

  /**
   * Create a {@link VectorCursor} for use with vectorized query engines.
   */
  @Nullable
  default VectorCursor asVectorCursor()
  {
    throw new UOE(
        "Cannot vectorize. Check 'canVectorize' before calling 'makeVectorCursor' on %s.",
        this.getClass().getName()
    );
  }

  /**
   * Returns true if this {@link CursorHolder} supports calling {@link #asVectorCursor()}.
   */
  default boolean canVectorize()
  {
    return false;
  }

  /**
   * Returns true if the {@link Cursor} or {@link VectorCursor} contains pre-aggregated columns for all
   * {@link AggregatorFactory} specified in {@link CursorBuildSpec#getAggregators()}.
   * <p>
   * If this method returns true, {@link ColumnSelectorFactory} and
   * {@link org.apache.druid.segment.vector.VectorColumnSelectorFactory} created from {@link Cursor} and
   * {@link VectorCursor} respectively will provide selectors for {@link AggregatorFactory#getName()}, and engines
   * should rewrite the query using {@link AggregatorFactory#getCombiningFactory()}, since the values returned from
   * these selectors will be of type {@link AggregatorFactory#getIntermediateType()}, so the cursor becomes a "fold"
   * operation rather than a "build" operation.
   */
  default boolean isPreAggregated()
  {
    return false;
  }

  /**
   * Returns a set of replacement {@link AggregatorFactory} if and only if {@link #isPreAggregated()} is true. The
   * query engine should replace the query aggregators with these aggregators, which are combining aggregators derived
   * from the {@link CursorBuildSpec} passed into {@link CursorFactory#makeCursorHolder(CursorBuildSpec)}. If
   * {@link #isPreAggregated()} is not true, this method returns null
   */
  @Nullable
  default List<AggregatorFactory> getAggregatorsForPreAggregated()
  {
    return null;
  }

  /**
   * Returns cursor ordering, which may or may not match {@link CursorBuildSpec#getPreferredOrdering()}. If returns
   * an empty list then the cursor has no defined ordering.
   *
   * Cursors associated with this holder return rows in this ordering, using the natural comparator for the column type.
   * Includes {@link ColumnHolder#TIME_COLUMN_NAME} if appropriate.
   */
  default List<OrderBy> getOrdering()
  {
    return Collections.emptyList();
  }

  /**
   * If {@link #getOrdering()} starts with {@link ColumnHolder#TIME_COLUMN_NAME}, returns the time ordering; otherwise
   * returns {@link Order#NONE}.
   */
  default Order getTimeOrder()
  {
    final List<OrderBy> ordering = getOrdering();
    if (!ordering.isEmpty() && ColumnHolder.TIME_COLUMN_NAME.equals(ordering.get(0).getColumnName())) {
      return ordering.get(0).getOrder();
    } else {
      return Order.NONE;
    }
  }

  /**
   * Release any resources acquired by cursors.
   */
  @Override
  default void close()
  {
    // nothing to close
  }
}
