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

package org.apache.druid.sql.calcite.filtration;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.column.ColumnHolder;

import java.util.ArrayList;
import java.util.List;

public class MoveTimeFiltersToIntervals implements Function<Filtration, Filtration>
{
  private static final MoveTimeFiltersToIntervals INSTANCE = new MoveTimeFiltersToIntervals();
  private static final BoundRefKey TIME_BOUND_REF_KEY = new BoundRefKey(
      ColumnHolder.TIME_COLUMN_NAME,
      null,
      StringComparators.NUMERIC
  );

  private MoveTimeFiltersToIntervals()
  {
  }

  public static MoveTimeFiltersToIntervals instance()
  {
    return INSTANCE;
  }

  @Override
  public Filtration apply(final Filtration filtration)
  {
    if (filtration.getDimFilter() == null) {
      return filtration;
    }

    // Convert existing filtration intervals to a RangeSet.
    final RangeSet<Long> rangeSet = RangeSets.fromIntervals(filtration.getIntervals());

    // Remove anything outside eternity.
    rangeSet.removeAll(RangeSets.fromIntervals(ImmutableList.of(Filtration.eternity())).complement());

    // Extract time bounds from the dimFilter.
    final Pair<DimFilter, RangeSet<Long>> pair = extractConvertibleTimeBounds(filtration.getDimFilter());

    if (pair.rhs != null) {
      rangeSet.removeAll(pair.rhs.complement());
    }

    return Filtration.create(pair.lhs, RangeSets.toIntervals(rangeSet));
  }

  /**
   * Extract bound filters on __time that can be converted to query-level "intervals".
   *
   * @return pair of new dimFilter + RangeSet of __time that should be ANDed together. Either can be null but not both.
   */
  private static Pair<DimFilter, RangeSet<Long>> extractConvertibleTimeBounds(final DimFilter filter)
  {
    if (filter instanceof AndDimFilter) {
      final List<DimFilter> children = ((AndDimFilter) filter).getFields();
      final List<DimFilter> newChildren = new ArrayList<>();
      final List<RangeSet<Long>> rangeSets = new ArrayList<>();

      for (DimFilter child : children) {
        final Pair<DimFilter, RangeSet<Long>> pair = extractConvertibleTimeBounds(child);
        if (pair.lhs != null) {
          newChildren.add(pair.lhs);
        }
        if (pair.rhs != null) {
          rangeSets.add(pair.rhs);
        }
      }

      final DimFilter newFilter;
      if (newChildren.size() == 0) {
        newFilter = null;
      } else if (newChildren.size() == 1) {
        newFilter = newChildren.get(0);
      } else {
        newFilter = new AndDimFilter(newChildren);
      }

      return Pair.of(
          newFilter,
          rangeSets.isEmpty() ? null : RangeSets.intersectRangeSets(rangeSets)
      );
    } else if (filter instanceof OrDimFilter) {
      final List<DimFilter> children = ((OrDimFilter) filter).getFields();
      final List<RangeSet<Long>> rangeSets = new ArrayList<>();

      boolean allCompletelyConverted = true;
      boolean allHadIntervals = true;
      for (DimFilter child : children) {
        final Pair<DimFilter, RangeSet<Long>> pair = extractConvertibleTimeBounds(child);
        if (pair.lhs != null) {
          allCompletelyConverted = false;
        }
        if (pair.rhs != null) {
          rangeSets.add(pair.rhs);
        } else {
          allHadIntervals = false;
        }
      }

      if (allCompletelyConverted) {
        return Pair.of(null, RangeSets.unionRangeSets(rangeSets));
      } else {
        return Pair.of(filter, allHadIntervals ? RangeSets.unionRangeSets(rangeSets) : null);
      }
    } else if (filter instanceof NotDimFilter) {
      final DimFilter child = ((NotDimFilter) filter).getField();
      final Pair<DimFilter, RangeSet<Long>> pair = extractConvertibleTimeBounds(child);
      if (pair.rhs != null && pair.lhs == null) {
        return Pair.of(null, pair.rhs.complement());
      } else {
        return Pair.of(filter, null);
      }
    } else if (filter instanceof BoundDimFilter) {
      final BoundDimFilter bound = (BoundDimFilter) filter;
      if (BoundRefKey.from(bound).equals(TIME_BOUND_REF_KEY)) {
        return Pair.of(null, RangeSets.of(toLongRange(Bounds.toRange(bound))));
      } else {
        return Pair.of(filter, null);
      }
    } else {
      return Pair.of(filter, null);
    }
  }

  private static Range<Long> toLongRange(final Range<BoundValue> range)
  {
    if (!range.hasUpperBound() && !range.hasLowerBound()) {
      return Range.all();
    } else if (range.hasUpperBound() && !range.hasLowerBound()) {
      return Range.upTo(Long.parseLong(range.upperEndpoint().getValue()), range.upperBoundType());
    } else if (!range.hasUpperBound() && range.hasLowerBound()) {
      return Range.downTo(Long.parseLong(range.lowerEndpoint().getValue()), range.lowerBoundType());
    } else {
      return Range.range(
          Long.parseLong(range.lowerEndpoint().getValue()), range.lowerBoundType(),
          Long.parseLong(range.upperEndpoint().getValue()), range.upperBoundType()
      );
    }
  }
}
