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

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;

import javax.annotation.Nullable;
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

  @Nullable
  private final Long limit;

  private Sorting(
      final List<OrderByColumnSpec> orderBys,
      @Nullable final Long limit,
      @Nullable final Projection projection
  )
  {
    this.orderBys = Preconditions.checkNotNull(orderBys, "orderBys");
    this.limit = limit;
    this.projection = projection;
  }

  public static Sorting create(
      final List<OrderByColumnSpec> orderBys,
      @Nullable final Long limit,
      @Nullable final Projection projection
  )
  {
    return new Sorting(orderBys, limit, projection);
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

  public boolean isLimited()
  {
    return limit != null;
  }

  @Nullable
  public Long getLimit()
  {
    return limit;
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
           Objects.equals(limit, sorting.limit);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(orderBys, projection, limit);
  }

  @Override
  public String toString()
  {
    return "Sorting{" +
           "orderBys=" + orderBys +
           ", projection=" + projection +
           ", limit=" + limit +
           '}';
  }
}
