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

import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.query.filter.RangeFilter;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;

public class Ranges
{
  /**
   * Negates single-ended Bound filters.
   *
   * @param range filter
   *
   * @return negated filter, or null if this range is double-ended.
   */
  @Nullable
  public static RangeFilter not(final RangeFilter range)
  {
    if (range.getUpper() != null && range.getLower() != null) {
      return null;
    } else if (range.getUpper() != null) {
      return new RangeFilter(
          range.getColumn(),
          range.getMatchValueType(),
          range.getUpper(),
          null,
          !range.isUpperOpen(),
          false,
          range.getFilterTuning()
      );
    } else {
      // range.getLower() != null
      return new RangeFilter(
          range.getColumn(),
          range.getMatchValueType(),
          null,
          range.getLower(),
          false,
          !range.isLowerOpen(),
          range.getFilterTuning()
      );
    }
  }

  public static Range<RangeValue> toRange(final RangeFilter range)
  {
    final RangeValue upper = range.getUpper() != null
                             ? new RangeValue(range.getUpper(), range.getMatchValueType())
                             : null;
    final RangeValue lower = range.getLower() != null
                             ? new RangeValue(range.getLower(), range.getMatchValueType())
                             : null;

    if (lower == null) {
      return range.isUpperOpen() ? Range.lessThan(upper) : Range.atMost(upper);
    } else if (upper == null) {
      return range.isLowerOpen() ? Range.greaterThan(lower) : Range.atLeast(lower);
    } else {
      BoundType lowerBoundType = range.isLowerOpen() ? BoundType.OPEN : BoundType.CLOSED;
      BoundType upperBoundType = range.isUpperOpen() ? BoundType.OPEN : BoundType.CLOSED;
      return Range.range(lower, lowerBoundType, upper, upperBoundType);
    }
  }

  public static Range<RangeValue> toRange(final RangeFilter range, final ColumnType newMatchValueType)
  {
    final ExpressionType exprType = ExpressionType.fromColumnType(newMatchValueType);
    final RangeValue upper = range.getUpper() != null
                             ? new RangeValue(ExprEval.ofType(exprType, range.getUpper())
                                                      .valueOrDefault(), newMatchValueType)
                             : null;
    final RangeValue lower = range.getLower() != null
                             ? new RangeValue(ExprEval.ofType(exprType, range.getLower())
                                                      .valueOrDefault(), newMatchValueType)
                             : null;

    if (lower == null) {
      return range.isUpperOpen() ? Range.lessThan(upper) : Range.atMost(upper);
    } else if (upper == null) {
      return range.isLowerOpen() ? Range.greaterThan(lower) : Range.atLeast(lower);
    } else {
      BoundType lowerBoundType = range.isLowerOpen() ? BoundType.OPEN : BoundType.CLOSED;
      BoundType upperBoundType = range.isUpperOpen() ? BoundType.OPEN : BoundType.CLOSED;
      return Range.range(lower, lowerBoundType, upper, upperBoundType);
    }
  }

  public static List<Range<RangeValue>> toRanges(final List<RangeFilter> ranges)
  {
    return ImmutableList.copyOf(Lists.transform(ranges, Ranges::toRange));
  }

  public static RangeFilter toFilter(final RangeRefKey rangeRefKey, final Range<RangeValue> range)
  {
    return new RangeFilter(
        rangeRefKey.getColumn(),
        rangeRefKey.getMatchValueType(),
        range.hasLowerBound() ? range.lowerEndpoint().getValue() : null,
        range.hasUpperBound() ? range.upperEndpoint().getValue() : null,
        range.hasLowerBound() && range.lowerBoundType() == BoundType.OPEN,
        range.hasUpperBound() && range.upperBoundType() == BoundType.OPEN,
        null
    );
  }

  public static RangeFilter equalTo(final RangeRefKey rangeRefKey, final Object value)
  {
    final Object castValue = castVal(rangeRefKey, value);
    return new RangeFilter(
        rangeRefKey.getColumn(),
        rangeRefKey.getMatchValueType(),
        castValue,
        castValue,
        false,
        false,
        null
    );
  }

  public static RangeFilter greaterThan(final RangeRefKey rangeRefKey, final Object value)
  {
    return new RangeFilter(
        rangeRefKey.getColumn(),
        rangeRefKey.getMatchValueType(),
        castVal(rangeRefKey, value),
        null,
        true,
        false,
        null
    );
  }

  public static RangeFilter greaterThanOrEqualTo(final RangeRefKey rangeRefKey, final Object value)
  {
    return new RangeFilter(
        rangeRefKey.getColumn(),
        rangeRefKey.getMatchValueType(),
        castVal(rangeRefKey, value),
        null,
        false,
        false,
        null
    );
  }

  public static RangeFilter lessThan(final RangeRefKey rangeRefKey, final Object value)
  {
    return new RangeFilter(
        rangeRefKey.getColumn(),
        rangeRefKey.getMatchValueType(),
        null,
        castVal(rangeRefKey, value),
        false,
        true,
        null
    );
  }

  public static RangeFilter lessThanOrEqualTo(final RangeRefKey rangeRefKey, final Object value)
  {
    return new RangeFilter(
        rangeRefKey.getColumn(),
        rangeRefKey.getMatchValueType(),
        null,
        castVal(rangeRefKey, value),
        false,
        false,
        null
    );
  }

  public static RangeFilter interval(final RangeRefKey rangeRefKey, final Interval interval)
  {
    if (!rangeRefKey.getMatchValueType().equals(ColumnType.LONG)) {
      // Interval comparison only works with LONG comparator.
      throw new ISE("Comparator must be LONG but was[%s]", rangeRefKey.getMatchValueType());
    }

    return new RangeFilter(
        rangeRefKey.getColumn(),
        rangeRefKey.getMatchValueType(),
        interval.getStartMillis(),
        interval.getEndMillis(),
        false,
        true,
        null
    );
  }

  /**
   * Casts a primitive value such that it matches the {@link RangeRefKey#getMatchValueType()} of a provided key.
   * Leaves nonprimitive values as-is.
   */
  private static Object castVal(final RangeRefKey rangeRefKey, final Object value)
  {
    if (value instanceof String || value instanceof Number || value == null) {
      final ColumnType columnType = rangeRefKey.getMatchValueType();
      if (columnType.is(ValueType.STRING) && (value instanceof String || value == null)) {
        // Short-circuit to save creation of ExprEval.
        return value;
      } else if (columnType.is(ValueType.DOUBLE) && value instanceof Double) {
        // Short-circuit to save creation of ExprEval.
        return value;
      } else if (columnType.is(ValueType.LONG) && value instanceof Long) {
        // Short-circuit to save creation of ExprEval.
        return value;
      } else {
        final ExpressionType expressionType = ExpressionType.fromColumnType(columnType);
        return ExprEval.ofType(expressionType, value).valueOrDefault();
      }
    } else {
      return value;
    }
  }
}
