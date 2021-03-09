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

package org.apache.druid.sql.calcite.rel;

import org.apache.druid.com.google.common.base.Preconditions;
import org.apache.druid.com.google.common.collect.Iterables;
import org.apache.druid.com.google.common.primitives.Ints;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.LimitSpec;
import org.apache.druid.query.groupby.orderby.NoopLimitSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.sql.calcite.planner.OffsetLimit;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Represents Druid's concept of sorting and limiting, including post-sort projections. The sorting and limiting piece
 * may map onto multiple Druid concepts: LimitSpec (for groupBy), TopNMetricSpec and threshold (for topN),
 * "descending" (for timeseries), or ScanQuery.Order (for scan). The post-sort projections will map onto either
 * post-aggregations (for query types that aggregate) or virtual columns (for query types that don't).
 *
 * This corresponds to a Calcite Sort + optional Project.
 */
public class Sorting
{
  enum SortKind
  {
    UNORDERED,
    TIME_ASCENDING,
    TIME_DESCENDING,
    NON_TIME
  }

  private final List<OrderByColumnSpec> orderBys;

  @Nullable
  private final Projection projection;

  private final OffsetLimit offsetLimit;

  private Sorting(
      final List<OrderByColumnSpec> orderBys,
      final OffsetLimit offsetLimit,
      @Nullable final Projection projection
  )
  {
    this.orderBys = Preconditions.checkNotNull(orderBys, "orderBys");
    this.offsetLimit = offsetLimit;
    this.projection = projection;
  }

  public static Sorting create(
      final List<OrderByColumnSpec> orderBys,
      final OffsetLimit offsetLimit,
      @Nullable final Projection projection
  )
  {
    return new Sorting(orderBys, offsetLimit, projection);
  }

  public static Sorting none()
  {
    return new Sorting(Collections.emptyList(), OffsetLimit.none(), null);
  }

  public SortKind getSortKind(final String timeColumn)
  {
    if (orderBys.isEmpty()) {
      return SortKind.UNORDERED;
    } else {
      if (orderBys.size() == 1) {
        final OrderByColumnSpec orderBy = Iterables.getOnlyElement(orderBys);
        if (orderBy.getDimension().equals(timeColumn)) {
          return orderBy.getDirection() == OrderByColumnSpec.Direction.ASCENDING
                 ? SortKind.TIME_ASCENDING
                 : SortKind.TIME_DESCENDING;
        }
      }

      return SortKind.NON_TIME;
    }
  }

  public List<OrderByColumnSpec> getOrderBys()
  {
    return orderBys;
  }

  @Nullable
  public Projection getProjection()
  {
    return projection;
  }

  public OffsetLimit getOffsetLimit()
  {
    return offsetLimit;
  }

  /**
   * Returns a LimitSpec that encapsulates the orderBys, offset, and limit of this Sorting instance. Does not
   * encapsulate the projection at all; you must still call {@link #getProjection()} for that.
   */
  public LimitSpec limitSpec()
  {
    if (orderBys.isEmpty() && !offsetLimit.hasOffset() && !offsetLimit.hasLimit()) {
      return NoopLimitSpec.instance();
    } else {
      final Integer offsetAsInteger = offsetLimit.hasOffset() ? Ints.checkedCast(offsetLimit.getOffset()) : null;
      final Integer limitAsInteger = offsetLimit.hasLimit() ? Ints.checkedCast(offsetLimit.getLimit()) : null;

      if (limitAsInteger != null && limitAsInteger == 0) {
        // Zero limit would be rejected by DefaultLimitSpec.
        throw new ISE("Cannot create LimitSpec with zero limit");
      }

      return new DefaultLimitSpec(orderBys, offsetAsInteger, limitAsInteger);
    }
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Sorting sorting = (Sorting) o;
    return Objects.equals(orderBys, sorting.orderBys) &&
           Objects.equals(projection, sorting.projection) &&
           Objects.equals(offsetLimit, sorting.offsetLimit);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(orderBys, projection, offsetLimit);
  }

  @Override
  public String toString()
  {
    return "Sorting{" +
           "orderBys=" + orderBys +
           ", projection=" + projection +
           ", offsetLimit=" + offsetLimit +
           '}';
  }
}
