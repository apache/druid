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
import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.ordering.StringComparators;
import org.joda.time.Interval;

import java.util.List;

public class Bounds
{
  /**
   * Negates single-ended Bound filters.
   *
   * @param bound filter
   *
   * @return negated filter, or null if this bound is double-ended.
   */
  public static BoundDimFilter not(final BoundDimFilter bound)
  {
    if (bound.getUpper() != null && bound.getLower() != null) {
      return null;
    } else if (bound.getUpper() != null) {
      return new BoundDimFilter(
          bound.getDimension(),
          bound.getUpper(),
          null,
          !bound.isUpperStrict(),
          false,
          null,
          bound.getExtractionFn(),
          bound.getOrdering(),
          bound.getFilterTuning()
      );
    } else {
      // bound.getLower() != null
      return new BoundDimFilter(
          bound.getDimension(),
          null,
          bound.getLower(),
          false,
          !bound.isLowerStrict(),
          null,
          bound.getExtractionFn(),
          bound.getOrdering(),
          bound.getFilterTuning()
      );
    }
  }

  public static Range<BoundValue> toRange(final BoundDimFilter bound)
  {
    final BoundValue upper = bound.getUpper() != null ? new BoundValue(bound.getUpper(), bound.getOrdering()) : null;
    final BoundValue lower = bound.getLower() != null ? new BoundValue(bound.getLower(), bound.getOrdering()) : null;

    if (lower == null) {
      return bound.isUpperStrict() ? Range.lessThan(upper) : Range.atMost(upper);
    } else if (upper == null) {
      return bound.isLowerStrict() ? Range.greaterThan(lower) : Range.atLeast(lower);
    } else {
      BoundType lowerBoundType = bound.isLowerStrict() ? BoundType.OPEN : BoundType.CLOSED;
      BoundType upperBoundType = bound.isUpperStrict() ? BoundType.OPEN : BoundType.CLOSED;
      return Range.range(lower, lowerBoundType, upper, upperBoundType);
    }
  }

  public static List<Range<BoundValue>> toRanges(final List<BoundDimFilter> bounds)
  {
    return ImmutableList.copyOf(
        Lists.transform(
            bounds,
            new Function<BoundDimFilter, Range<BoundValue>>()
            {
              @Override
              public Range<BoundValue> apply(BoundDimFilter bound)
              {
                return toRange(bound);
              }
            }
        )
    );
  }

  public static BoundDimFilter toFilter(final BoundRefKey boundRefKey, final Range<BoundValue> range)
  {
    return new BoundDimFilter(
        boundRefKey.getDimension(),
        range.hasLowerBound() ? range.lowerEndpoint().getValue() : null,
        range.hasUpperBound() ? range.upperEndpoint().getValue() : null,
        range.hasLowerBound() && range.lowerBoundType() == BoundType.OPEN,
        range.hasUpperBound() && range.upperBoundType() == BoundType.OPEN,
        null,
        boundRefKey.getExtractionFn(),
        boundRefKey.getComparator(),
        null
    );
  }

  public static BoundDimFilter equalTo(final BoundRefKey boundRefKey, final String value)
  {
    return new BoundDimFilter(
        boundRefKey.getDimension(),
        value,
        value,
        false,
        false,
        null,
        boundRefKey.getExtractionFn(),
        boundRefKey.getComparator(),
        null
    );
  }

  public static BoundDimFilter greaterThan(final BoundRefKey boundRefKey, final String value)
  {
    return new BoundDimFilter(
        boundRefKey.getDimension(),
        value,
        null,
        true,
        false,
        null,
        boundRefKey.getExtractionFn(),
        boundRefKey.getComparator(),
        null
    );
  }

  public static BoundDimFilter greaterThanOrEqualTo(final BoundRefKey boundRefKey, final String value)
  {
    return new BoundDimFilter(
        boundRefKey.getDimension(),
        value,
        null,
        false,
        false,
        null,
        boundRefKey.getExtractionFn(),
        boundRefKey.getComparator(),
        null
    );
  }

  public static BoundDimFilter lessThan(final BoundRefKey boundRefKey, final String value)
  {
    return new BoundDimFilter(
        boundRefKey.getDimension(),
        null,
        value,
        false,
        true,
        null,
        boundRefKey.getExtractionFn(),
        boundRefKey.getComparator(),
        null
    );
  }

  public static BoundDimFilter lessThanOrEqualTo(final BoundRefKey boundRefKey, final String value)
  {
    return new BoundDimFilter(
        boundRefKey.getDimension(),
        null,
        value,
        false,
        false,
        null,
        boundRefKey.getExtractionFn(),
        boundRefKey.getComparator(),
        null
    );
  }

  public static BoundDimFilter interval(final BoundRefKey boundRefKey, final Interval interval)
  {
    if (!boundRefKey.getComparator().equals(StringComparators.NUMERIC)) {
      // Interval comparison only works with NUMERIC comparator.
      throw new ISE("Comparator must be NUMERIC but was[%s]", boundRefKey.getComparator());
    }

    return new BoundDimFilter(
        boundRefKey.getDimension(),
        String.valueOf(interval.getStartMillis()),
        String.valueOf(interval.getEndMillis()),
        false,
        true,
        null,
        boundRefKey.getExtractionFn(),
        boundRefKey.getComparator(),
        null
    );
  }
}
