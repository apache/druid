/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package com.metamx.druid.query.group.limit;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Longs;
import com.metamx.common.ISE;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.druid.aggregation.AggregatorFactory;
import com.metamx.druid.aggregation.post.PostAggregator;
import com.metamx.druid.input.Row;
import com.metamx.druid.query.dimension.DimensionSpec;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 */
public class DefaultLimitSpec implements LimitSpec
{
  private final List<OrderByColumnSpec> orderBy;
  private final int limit;

  @JsonCreator
  public DefaultLimitSpec(
      @JsonProperty("orderBy") List<OrderByColumnSpec> orderBy,
      @JsonProperty("limit") int limit
  )
  {
    this.orderBy = (orderBy == null) ? ImmutableList.<OrderByColumnSpec>of() : orderBy;
    this.limit = limit;

    Preconditions.checkState(limit > 0, "limit[%s] must be >0", limit);
  }

  public DefaultLimitSpec()
  {
    this.orderBy = Lists.newArrayList();
    this.limit = 0;
  }

  @JsonProperty
  public List<OrderByColumnSpec> getOrderBy()
  {
    return orderBy;
  }

  @JsonProperty
  public int getLimit()
  {
    return limit;
  }

  @Override
  public Function<Sequence<Row>, Sequence<Row>> build(
      List<DimensionSpec> dimensions, List<AggregatorFactory> aggs, List<PostAggregator> postAggs
  )
  {
    // Materialize the Comparator first for fast-fail error checking.
    final Comparator<Row> comparator = makeComparator(dimensions, aggs, postAggs);

    return new Function<Sequence<Row>, Sequence<Row>>()
    {
      @Override
      public Sequence<Row> apply(Sequence<Row> input)
      {
        return Sequences.limit(Sequences.sort(input, comparator), limit);
      }
    };
  }

  private Comparator<Row> makeComparator(
      List<DimensionSpec> dimensions, List<AggregatorFactory> aggs, List<PostAggregator> postAggs
  )
  {
    Ordering<Row> ordering = new Ordering<Row>()
    {
      @Override
      public int compare(Row left, Row right)
      {
        return Longs.compare(left.getTimestampFromEpoch(), right.getTimestampFromEpoch());
      }
    };

    Map<String, Ordering<Row>> possibleOrderings = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
    for (DimensionSpec spec : dimensions) {
      final String dimension = spec.getOutputName();
      possibleOrderings.put(dimension, dimensionOrdering(dimension));
    }

    for (final AggregatorFactory agg : aggs) {
      final String column = agg.getName();
      possibleOrderings.put(column, metricOrdering(column, agg.getComparator()));
    }

    for (PostAggregator postAgg : postAggs) {
      final String column = postAgg.getName();
      possibleOrderings.put(column, metricOrdering(column, postAgg.getComparator()));
    }

    for (OrderByColumnSpec columnSpec : orderBy) {
      Ordering<Row> nextOrdering = possibleOrderings.get(columnSpec.getDimension());

      if (nextOrdering == null) {
        throw new ISE("Unknown column in order clause[%s]", columnSpec);
      }

      switch (columnSpec.getDirection()) {
        case DESCENDING:
          nextOrdering = nextOrdering.reverse();
      }

      ordering = ordering.compound(nextOrdering);
    }

    return ordering;
  }

  private Ordering<Row> metricOrdering(final String column, final Comparator comparator)
  {
    return new Ordering<Row>()
    {
      @SuppressWarnings("unchecked")
      @Override
      public int compare(Row left, Row right)
      {
        return comparator.compare(left.getFloatMetric(column), right.getFloatMetric(column));
      }
    };
  }

  private Ordering<Row> dimensionOrdering(final String dimension)
  {
    return Ordering.natural()
                   .nullsFirst()
                   .onResultOf(
                       new Function<Row, String>()
                       {
                         @Override
                         public String apply(Row input)
                         {
                           // Multi-value dimensions have all been flattened at this point;
                           final List<String> dimList = input.getDimension(dimension);
                           return dimList.isEmpty() ? null : dimList.get(0);
                         }
                       }
                   );
  }

  @Override
  public String toString()
  {
    return "DefaultLimitSpec{" +
           "orderBy='" + orderBy + '\'' +
           ", limit=" + limit +
           '}';
  }
}
